#ifndef __READ_H__
#define __READ_H__

#include <istream>
#include <string>
#include <utility>
#include <vector>

#include "base.h"
#include "document.h"

namespace csv {

struct ReadOptions {
  char quotechar;
  char separator;
  int num_threads;

  ReadOptions() : quotechar('"'), separator(','), num_threads(16) {}
  ReadOptions(char quotechar, char separator, int num_threads)
      : quotechar(quotechar), separator(separator), num_threads(num_threads) {}
};

std::vector<std::string> ColumnNames(std::istream& file_in, const std::string& path,
                                     ReadOptions options = ReadOptions());

Document ReadCSV(const std::string& path, const std::vector<FieldType>& field_types,
                 ReadOptions options = ReadOptions());
}  // namespace csv

#endif
