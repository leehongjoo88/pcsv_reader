#include "read.h"

#include <iostream>
#include <stdexcept>
#include <vector>

#include "stop_watch.h"

std::vector<std::string> TokenizeFieldTypeString(const std::string& field_type_string) {
  constexpr auto delim = ',';
  std::string::size_type pos = 0u;
  std::vector<std::string> tokens;
  do {
    auto found = field_type_string.find(delim, pos);
    if (found == std::string::npos) {
      tokens.push_back(field_type_string.substr(pos));
      break;
    }

    tokens.push_back(field_type_string.substr(pos, found - pos));
    pos = found + 1;
  } while(true);

  return tokens;
}

std::vector<csv::FieldType> ParseFieldType(const std::string& field_type_string) {
  std::vector<csv::FieldType> types;
  const auto field_names = TokenizeFieldTypeString(field_type_string);
  types.reserve(field_names.size());
  const std::string int_str("INT64");
  const std::string double_str("DOUBLE");
  const std::string string_str("STRING");
  for (const auto& field_name : field_names) {
    if (field_name == int_str) {
      types.push_back(csv::FieldType::INT64);
    } else if (field_name == double_str) {
      types.push_back(csv::FieldType::DOUBLE);
    } else if (field_name == string_str) {
      types.push_back(csv::FieldType::STRING);
    } else {
      throw std::runtime_error(std::string("input type string has wrong token ") + field_name);
    }
  }

  return types;
}

int main(int argc, char** argv) {
  if (argc != 3) {
    std::cout << "must csv pat and field types be given.";
    return -1;
  }

  const std::string file_name = argv[1];
  const std::string field_type_string = argv[2];
  const auto field_types = ParseFieldType(field_type_string);

  Stopwatch watch("read");
  watch.Start();
  auto document = csv::ReadCSV(file_name, field_types);
  watch.End();

  Stopwatch watch2("vector");
  watch.Start();
  auto v = document.GetAsString("emp_length");
  watch.End();
  std::cout << v.size() << std::endl;
  std::cout << v[0] << std::endl << v[1] << v[2] << std::endl;

  return 0;
}
