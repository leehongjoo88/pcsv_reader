#ifndef __BASE_H__
#define __BASE_H__

#include <cstddef>
#include <cstdint>
#include <string>
#include <stdexcept>

namespace csv {

enum class FieldType {
  INT64 = 0,
  DOUBLE,
  STRING,
  END
};

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


}  // namespace csv

#endif
