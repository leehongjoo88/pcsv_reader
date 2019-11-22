#include <iostream>
#include <stdexcept>
#include <vector>

#include "read.h"
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
  } while (true);

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
      throw std::runtime_error(std::string("input type string has wrong token ") +
                               field_name);
    }
  }

  return types;
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cout << "must csv pat and field types be given.";
    return -1;
  }

  const std::string file_name = argv[1];
  const std::string field_type_string =       "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,STRING,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,INT64,DOUBLE,INT64,INT64,INT64,"
      "INT64,INT64,INT64,INT64,INT64,INT64";

  const auto field_types = ParseFieldType(field_type_string);

  Stopwatch watch("read");
  watch.Start();
  auto document = csv::ReadCSV(file_name, field_types, csv::ReadOptions{'"', '|', 16});
  watch.End();

  watch.message = "read_columns";

  std::vector<int64_t> int_vector;
  std::vector<double> double_vector;
  std::vector<std::string> string_vector;

  watch.Start();
  document.SetNumThreads(16);
  auto field_names = document.FieldNames();
  auto field_name_itr = std::begin(field_names);
  auto field_type_itr = std::begin(field_types);
  const auto field_type_end = std::end(field_types);
  for (; field_type_itr != field_type_end; ++field_name_itr, ++field_type_itr) {
    switch (*field_type_itr) {
    case csv::FieldType::INT64:
      if (int_vector.empty()) {
        int_vector = document.GetAsInt64(*field_name_itr);
      } else {
        document.GetAsInt64(*field_name_itr, int_vector);
      }
      break;
    case csv::FieldType::DOUBLE:
      if (double_vector.empty()) {
        double_vector = document.GetAsDouble(*field_name_itr);
      } else {
        document.GetAsDouble(*field_name_itr, double_vector);
      }
      break;

    case csv::FieldType::STRING:
      if (string_vector.empty()) {
        string_vector = document.GetAsString(*field_name_itr);
      } else {
        document.GetAsString(*field_name_itr, string_vector);
      }
      break;
    default:
      break;
    }
  }
  watch.End();

  return 0;
}
