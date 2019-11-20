#ifndef __BASE_H__
#define __BASE_H__

#include <cstddef>
#include <cstdint>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

namespace csv {

enum class FieldType { INT64 = 0, DOUBLE, STRING, END };

template <FieldType>
struct FieldTypeHelper {
  using type = void;
};

template <>
struct FieldTypeHelper<FieldType::INT64> {
  using type = int64_t;
  static constexpr size_t size = sizeof(type);
};

template <>
struct FieldTypeHelper<FieldType::DOUBLE> {
  using type = double;
  static constexpr size_t size = sizeof(type);
};

template <>
struct FieldTypeHelper<FieldType::STRING> {
  using type = std::string;
  static constexpr size_t size = 64;
};

template <typename T>
T Convert(const std::string&);

template <typename T>
T Convert(const char* str, size_t len);

template <>
inline int64_t Convert<int64_t>(const std::string& str) {
  return static_cast<int64_t>(std::stoll(str));
}

template <>
inline double Convert<double>(const std::string& str) {
  return std::stod(str);
}

template <>
inline int64_t Convert<int64_t>(const char* str, size_t len) {
  auto value = static_cast<int64_t>(std::atoll(str));
  if (value == 0 && !(*str == '0' && len == 1)) {
    throw std::invalid_argument(str);
  }

  return value;
}

template <>
inline double Convert<double>(const char* str, size_t len) {
  auto value = std::atof(str);
  if (value == 0.0) {
    return std::stoll(std::string(str, len));
  }

  return value;
}

inline size_t GetTotalFieldTypeSize(const std::vector<FieldType>& field_types) {
  using IntHelper = FieldTypeHelper<FieldType::INT64>;
  using DoubleHelper = FieldTypeHelper<FieldType::DOUBLE>;
  using StringHelper = FieldTypeHelper<FieldType::STRING>;
  auto size_getter = [](size_t sum, FieldType field_type) -> size_t {
    switch (field_type) {
    case FieldType::INT64:
      return sum + IntHelper::size;
    case FieldType::DOUBLE:
      return sum + DoubleHelper::size;
    case FieldType::STRING:
      return sum + StringHelper::size;
    default:
      return sum;
    }
  };
  return std::accumulate(std::begin(field_types), std::end(field_types), 0u, size_getter);
}

}  // namespace csv

#endif
