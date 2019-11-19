#include "document.h"

#include <numeric>

namespace csv {

namespace {

inline size_t Align64(size_t size) { return 64 * ((size + 63) / 64); }

} // namespace

Document::Document(const std::vector<std::string> &field_names,
                   const std::vector<FieldType> &field_types)

    : field_names_(field_names), num_cols_(field_names.size()),
      actual_row_byte_size_(0u), current_memory_chunk_(nullptr),
      current_row_offset_in_chunk_(0) {
  column_infos_.reserve(field_types.size());
  int offset = 0;
  for (const auto field_type : field_types) {
    switch (field_type) {
    case FieldType::INT64:
      column_infos_.push_back(ColumnInfo{
          FieldType::INT64, offset, FieldTypeHelper<FieldType::INT64>::size});
      offset += FieldTypeHelper<FieldType::INT64>::size;
      break;
    case FieldType::DOUBLE:
      column_infos_.push_back(ColumnInfo{
          FieldType::DOUBLE, offset, FieldTypeHelper<FieldType::DOUBLE>::size});
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
  actual_row_byte_size_ = Align64(static_cast<size_t>(offset));
}


void Document::Write(size_t row, size_t column, const char *str,
                     size_t str_length) {
  const int row_idx_in_chunk = row - current_row_offset_in_chunk_;
  const auto &column_info = column_infos_[column];
  const auto chunk_offset =
      row_idx_in_chunk * actual_row_byte_size_ + column_info.offset;
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
                         [](size_t num_rows, const DocumentMemoryChunk &chunk) {
                           return num_rows + chunk.num_rows;
                         });
}

std::vector<int64_t> Document::GetAsInt64(const std::string& column) const {
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

  const auto &column_info = column_infos_[column_index];
  std::vector<int64_t> column_result(this->NumRows());
  const auto column_offset = column_info.offset;
  auto row_offset = 0u;
  for (const auto &document_memory_chunk : buffer_) {
    auto current_chunk = document_memory_chunk.chunk.get();
    for (size_t row = 0u; row < document_memory_chunk.num_rows; ++row) {
      column_result[row + row_offset] = current_chunk->ReadInt64(
          row * actual_row_byte_size_ + column_offset);
    }
    row_offset += document_memory_chunk.num_rows;
  }

  return column_result;
}

std::vector<std::string> Document::GetAsString(const std::string& column) const {
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

  const auto &column_info = column_infos_[column_index];
  std::vector<std::string> column_result(this->NumRows());
  const auto column_offset = column_info.offset;
  auto row_offset = 0u;
  for (const auto &document_memory_chunk : buffer_) {
    auto current_chunk = document_memory_chunk.chunk.get();
    for (size_t row = 0u; row < document_memory_chunk.num_rows; ++row) {
      column_result[row + row_offset] = current_chunk->ReadString(
          row * actual_row_byte_size_ + column_offset);
    }
    row_offset += document_memory_chunk.num_rows;
  }

  return column_result;
}

std::vector<double> Document::GetAsDouble(const std::string& column) const {
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

  const auto &column_info = column_infos_[column_index];
  std::vector<double> column_result(this->NumRows());
  const auto column_offset = column_info.offset;
  auto row_offset = 0u;
  for (const auto &document_memory_chunk : buffer_) {
    auto current_chunk = document_memory_chunk.chunk.get();
    for (size_t row = 0u; row < document_memory_chunk.num_rows; ++row) {
      column_result[row + row_offset] = current_chunk->ReadDouble(
          row * actual_row_byte_size_ + column_offset);
    }
    row_offset += document_memory_chunk.num_rows;
  }

  return column_result;
}

} // namespace csv
