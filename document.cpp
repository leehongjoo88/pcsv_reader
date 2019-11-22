#include "document.h"

#include <omp.h>

#include <numeric>

namespace csv {

namespace {

inline size_t Align64(size_t size) { return 64 * ((size + 63) / 64); }

}  // namespace

Document::Document(const std::vector<std::string>& field_names,
                   const std::vector<FieldType>& field_types)

    : field_names_(field_names),
      num_cols_(field_names.size()),
      actual_row_byte_size_(Align64(GetTotalFieldTypeSize(field_types))),
      current_memory_chunk_(nullptr),
      current_row_offset_in_chunk_(0),
      num_threads_(1) {
  column_infos_.reserve(field_types.size());
  int offset = 0;
  for (const auto field_type : field_types) {
    switch (field_type) {
    case FieldType::INT64:
      column_infos_.push_back(
          ColumnInfo{FieldType::INT64, offset, FieldTypeHelper<FieldType::INT64>::size});
      offset += FieldTypeHelper<FieldType::INT64>::size;
      break;
    case FieldType::DOUBLE:
      column_infos_.push_back(ColumnInfo{FieldType::DOUBLE, offset,
                                         FieldTypeHelper<FieldType::DOUBLE>::size});
      offset += FieldTypeHelper<FieldType::DOUBLE>::size;
      break;
    case FieldType::STRING:
      column_infos_.push_back(
          ColumnInfo{FieldType::STRING, offset, MemoryChunk::kStringCellSize});
      offset += MemoryChunk::kStringCellSize;
      break;
    default:
      break;
    }
  }
}

void Document::Write(size_t row, size_t column, const char* str, size_t str_length) {
  const int row_idx_in_chunk = row - current_row_offset_in_chunk_;
  const auto& column_info = column_infos_[column];
  const auto chunk_offset = row_idx_in_chunk * actual_row_byte_size_ + column_info.offset;
  switch (column_info.type) {
  case FieldType::INT64: {
    auto value = str_length == 0 ? 0u : Convert<int64_t>(str, str_length);
    current_memory_chunk_->Write(chunk_offset, value);
    break;
  }
  case FieldType::DOUBLE: {
    auto value = str_length == 0 ? 0u : Convert<double>(str, str_length);
    current_memory_chunk_->Write(chunk_offset, value);
    break;
  }
  case FieldType::STRING:
    current_memory_chunk_->Write(chunk_offset, str, str_length);
  default:
    break;
  }
}

template <typename T>
void Document::Get(const std::string& column, std::vector<T>& column_result) const {
  static_assert(std::is_same<T, int64_t>::value || std::is_same<T, double>::value ||
                    std::is_same<T, std::string>::value,
                "Given type must be one of int64_t, double, std::string");
  assert(column_result.size() == this->NumRows());
  int column_index = -1;
  for (int idx = 0; idx < static_cast<int>(field_names_.size()); idx++) {
    if (field_names_[idx] == column) {
      column_index = idx;
      break;
    }
  }

  if (column_index < 0) {
    throw std::invalid_argument(std::string("no column with name ") + column);
  }

  const auto& column_info = column_infos_[column_index];
  const auto column_offset = column_info.offset;
  auto row_offset = 0u;
  // use multi thread
  if (num_threads_ > 1) {
    for (const auto& document_memory_chunk : buffer_) {
      auto current_chunk = document_memory_chunk.chunk.get();
      omp_set_num_threads(num_threads_);
#pragma omp parallel for
      for (size_t row = 0u; row < document_memory_chunk.num_rows; ++row) {
        column_result[row + row_offset] =
            current_chunk->Read<T>(row * actual_row_byte_size_ + column_offset);
      }
      row_offset += document_memory_chunk.num_rows;
    }
  } else {
    for (const auto& document_memory_chunk : buffer_) {
      auto current_chunk = document_memory_chunk.chunk.get();
      for (size_t row = 0u; row < document_memory_chunk.num_rows; ++row) {
        column_result[row + row_offset] =
            current_chunk->Read<T>(row * actual_row_byte_size_ + column_offset);
      }
      row_offset += document_memory_chunk.num_rows;
    }
  }
}

void Document::AddChunk(size_t num_rows) {
  std::unique_ptr<MemoryChunk> new_memory_chunk(
      new MemoryChunk(num_rows * actual_row_byte_size_));
  const size_t last_chunk_size = buffer_.empty() ? 0u : buffer_.back().num_rows;
  buffer_.push_back(DocumentMemoryChunk{std::move(new_memory_chunk), num_rows});
  current_row_offset_in_chunk_ += last_chunk_size;
  current_memory_chunk_ = buffer_.back().chunk.get();
}

size_t Document::NumRows() const {
  return std::accumulate(std::begin(buffer_), std::end(buffer_), size_t{0u},
                         [](size_t num_rows, const DocumentMemoryChunk& chunk) {
                           return num_rows + chunk.num_rows;
                         });
}

std::vector<int64_t> Document::GetAsInt64(const std::string& column) const {
  std::vector<int64_t> column_result(this->NumRows());
  this->Get<int64_t>(column, column_result);
  return column_result;
}

void Document::GetAsInt64(const std::string& column, std::vector<int64_t>& result) const {
  if (result.size() != this->NumRows()) {
    throw std::invalid_argument(
        std::string("given output vector of size ") + std::to_string(result.size()) +
        "doesn't match with row count " + std::to_string(this->NumRows()));
  }
  this->Get<int64_t>(column, result);
}

std::vector<std::string> Document::GetAsString(const std::string& column) const {
  std::vector<std::string> column_result(this->NumRows());
  this->Get<std::string>(column, column_result);
  return column_result;
}

void Document::GetAsString(const std::string& column,
                           std::vector<std::string>& result) const {
  if (result.size() != this->NumRows()) {
    throw std::invalid_argument(
        std::string("given output vector of size ") + std::to_string(result.size()) +
        "doesn't match with row count " + std::to_string(this->NumRows()));
  }
  this->Get<std::string>(column, result);
}

std::vector<double> Document::GetAsDouble(const std::string& column) const {
  std::vector<double> column_result(this->NumRows());
  this->Get<double>(column, column_result);
  return column_result;
}

void Document::GetAsDouble(const std::string& column, std::vector<double>& result) const {
  if (result.size() != this->NumRows()) {
    throw std::invalid_argument(
        std::string("given output vector of size ") + std::to_string(result.size()) +
        "doesn't match with row count " + std::to_string(this->NumRows()));
  }
  this->Get<double>(column, result);
}

void Document::Dump(std::ostream& os) const {
  for (size_t i = 0; i < field_names_.size(); i++) {
    if (i != 0) {
      os << ',';
    }
    os << field_names_[i];
  }
  os << '\n';
  for (const auto& one_buffer : buffer_) {
    const auto current_chunk = one_buffer.chunk.get();
    for (size_t row_idx = 0; row_idx < one_buffer.num_rows; row_idx++) {
      for (size_t column_idx = 0; column_idx < column_infos_.size(); column_idx++) {
        if (column_idx != 0) {
          os << ',';
        }
        const auto& column_info = column_infos_[column_idx];
        const auto offset = row_idx * actual_row_byte_size_ + column_info.offset;
        switch (column_info.type) {
        case FieldType::INT64:
          os << current_chunk->ReadInt64(offset);
          break;
        case FieldType::DOUBLE:
          os << current_chunk->ReadDouble(offset);
          break;
        case FieldType::STRING:
          os << current_chunk->ReadString(offset);
          break;
        default:
          break;
        }
      }
      os << '\n';
    }
  }
}

}  // namespace csv
