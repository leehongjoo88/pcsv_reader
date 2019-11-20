#include "read.h"

#include <cmath>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace csv {

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
    const char c = line[i];
    if (c == quotechar) {
      if (cell_start == cell_end || line[cell_end - 1] == quotechar) {
        quoted = !quoted;
      }
    } else if (c == separator && !quoted) {
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

size_t EstimateNumberOfLines(std::istream& file_in) {
  file_in.seekg(0, std::ios::beg);
  std::string line;

  // header
  std::getline(file_in, line);

  const auto header_size = line.size();

  if (!std::getline(file_in, line)) {
    return 0u;
  }

  file_in.seekg(0, std::ios::end);
  const auto file_size = static_cast<size_t>(file_in.tellg());

  // estimate with file size - +1 is new line
  const auto estimated_lines =
      static_cast<size_t>(std::lround(static_cast<double>(file_size - header_size) /
                                      static_cast<double>(line.size() + 1)));

  file_in.seekg(0, std::ios::beg);
  return estimated_lines;
}

// Document ReadCSV(const std::string& path, const std::vector<FieldType>& field_types) {
//   std::ifstream file_in(path);
//   file_in.exceptions(std::ifstream::badbit);
// }

}  // namespace csv
