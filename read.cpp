#include "read.h"

#include <omp.h>

#include <cassert>
#include <cmath>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace csv {

namespace {

constexpr size_t kMaxChunkSize = 256 * 1024 * 1024;  // 256MB

size_t EstimateLineSize(std::istream& file_in) {
  file_in.seekg(0, std::ios::beg);
  std::string line;

  // header
  std::getline(file_in, line);

  size_t total_line_size = 0u;
  size_t num_lines = 0u;
  // look up ten lines
  for (; num_lines < 10u; num_lines++) {
    if (!std::getline(file_in, line)) {
      file_in.clear();
      break;
    }
    total_line_size += line.size();
  }
  if (num_lines == 0u) {
    return 0u;
  }
  return static_cast<size_t>(
      std::lround(static_cast<double>(total_line_size) / static_cast<double>(num_lines)));
}

bool FillLines(std::istream& file_in, std::vector<std::string>& line_buffer,
               size_t& num_read_lines) {
  size_t read_bytes = 0u;
  size_t line_buffer_size = line_buffer.size();
  std::string line;
  num_read_lines = 0u;

  auto line_no = 0u;
  while (read_bytes < kMaxChunkSize) {
    if (!std::getline(file_in, line)) {
      return true;
    }

    if (line.empty()) {
      continue;
    }

    if (line_no >= line_buffer_size) {
      line_buffer.resize(line_buffer.size() * 2);
      line_buffer_size = line_buffer.size();
    }

    line_buffer[line_no++] = line;
    num_read_lines++;
    read_bytes += line.size();
  }

  return false;
}
}  // namespace

std::vector<std::string> ColumnNames(std::istream& file_in, const std::string& path) {
  file_in.seekg(0, std::ios::beg);
  std::string line;
  if (!std::getline(file_in, line)) {
    throw std::runtime_error(std::string("Failed to parse field names from ") + path);
  }

  const char quotechar = '"';
  const char separator = ',';

  std::vector<std::string> column_names;
  int cell_start = 0;
  int cell_end = 0;
  bool quoted = false;
  for (size_t i = 0; i < line.size(); ++i) {
    const char current_char = line[i];
    if (current_char == quotechar) {
      if (cell_start == cell_end || line[cell_end - 1] == quotechar) {
        quoted = !quoted;
      }
    } else if (current_char == separator && !quoted) {
      column_names.push_back(line.substr(cell_start, cell_end - cell_start));
      cell_start = cell_end + 1;
    }
    cell_end++;
  }

  if (cell_start != cell_end) {
    column_names.push_back(line.substr(cell_start, cell_end - cell_start));
  }

  return column_names;
}

Document ReadCSV(const std::string& path, const std::vector<FieldType>& field_types) {
  std::ifstream file_in(path, std::ios::ate);
  file_in.exceptions(std::ifstream::badbit);
  const auto file_size = static_cast<size_t>(file_in.tellg());
  const auto column_size = field_types.size();
  const auto column_names = ColumnNames(file_in, path);
  assert(column_size == column_names.size());
  const auto estimated_line_size = EstimateLineSize(file_in);

  // +1 to prevent 0 estimation && times to prevent for wrong estimation
  const auto estimated_buffer_lines =
      2 * (std::lround(static_cast<double>(kMaxChunkSize) /
                       static_cast<double>(estimated_line_size)) +
           1);
  const auto estimated_file_lines =
      2 * (std::lround(static_cast<double>(file_size) /
                       static_cast<double>(estimated_line_size)) +
           1);

  std::vector<std::string> lines(std::min(estimated_file_lines, estimated_buffer_lines));
  Document doc(column_names, field_types);

  const char quotechar = '"';
  const char separator = ',';
  const auto num_threads = 16u;

  bool process_done = false;
  size_t row_offset = 0u;

  file_in.seekg(0, std::ios::beg);
  std::string dummy;
  std::getline(file_in, dummy);
  do {
    size_t num_read_lines = 0u;
    process_done = FillLines(file_in, lines, num_read_lines);
    doc.AddChunk(num_read_lines);
    omp_set_num_threads(num_threads);
#pragma omp parallel for schedule(dynamic)
    for (size_t row_no = row_offset; row_no < row_offset + num_read_lines; ++row_no) {
      int cell_start = 0;
      int cell_end = 0;
      bool quoted = false;
      size_t column = 0u;
      const auto& line = lines[row_no - row_offset];
      const auto lineptr = line.c_str();
      if (line.empty()) {
        continue;
      }

      for (size_t i = 0; i < line.size(); ++i, ++cell_end) {
        const char current_char = line[i];
        if (current_char == quotechar) {
          if (cell_start == cell_end || line[cell_end - 1] == quotechar) {
            quoted = !quoted;
          }
        } else if (current_char == separator && !quoted) {
          if (field_types[column] == FieldType::STRING &&
              static_cast<unsigned>(cell_end - cell_start) >=
                  FieldTypeHelper<FieldType::STRING>::size) {
            throw std::runtime_error(std::string("at row ") + std::to_string(row_no) +
                                     ", col " + std::to_string(column) +
                                     ": string length should be shorter than 64");
          }
          doc.Write(row_no, column++, lineptr + cell_start, cell_end - cell_start);
          cell_start = cell_end + 1;
        }
      }

      if (cell_start != cell_end && !quoted) {
        doc.Write(row_no, column++, lineptr + cell_start, cell_end - cell_start);
      }

      if (column != column_size) {
        throw std::runtime_error("column size doesn't match for row " +
                                 std::to_string(row_no));
      }
    }

    row_offset += num_read_lines;
  } while (!process_done);

  return doc;
}

}  // namespace csv
