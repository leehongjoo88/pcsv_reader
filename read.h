#ifndef __READ_H__
#define __READ_H__

#include <istream>
#include <string>
#include <vector>

#include "base.h"
#include "document.h"

namespace csv {

std::vector<std::string> ColumnNames(std::istream& file_in, const std::string& path);
size_t EstimateNumberOfLines(std::istream& file_in);
// Document ReadCSV(const std::string& path, const std::vector<FieldType>& field_types);

}

#endif
