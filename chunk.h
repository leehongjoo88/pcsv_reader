#ifndef __CHUNK_H__
#define __CHUNK_H__

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>

#include "base.h"

namespace csv {

class MemoryChunk {
public:
  static constexpr size_t kStringCellSize = FieldTypeHelper<FieldType::STRING>::size;
  static constexpr size_t kMaxStringLength = kStringCellSize - 1;

  explicit MemoryChunk(size_t size) : buffer_(new char[size]), size_(size) {
    std::memset(reinterpret_cast<void *>(buffer_), 0, size_ * sizeof(char));
  }

  ~MemoryChunk() {
    if (size_ > 0 && !buffer_)
      delete[] buffer_;
  }

  int64_t ReadInt64(int offset) const {
    return *reinterpret_cast<int64_t *>(buffer_ + offset);
  }
  double ReadDouble(int offset) const {
    return *reinterpret_cast<double *>(buffer_ + offset);
  }
  std::string ReadString(int offset) const {
    return std::string(ReadCharPtr(offset), ReadStrLength(offset));
  }
  char *ReadCharPtr(int offset) const { return buffer_ + offset; }
  size_t ReadStrLength(int offset) const {
    return MemoryChunk::kMaxStringLength -
           static_cast<size_t>(*(buffer_ + offset + MemoryChunk::kMaxStringLength));
  }

  template <typename T>
  T Read(int offset) const;

  void Write(int offset, int64_t value) {
    std::memcpy(buffer_ + offset, &value, sizeof(int64_t));
  }
  void Write(int offset, double value) {
    std::memcpy(buffer_ + offset, &value, sizeof(double));
  }
  void Write(int offset, const std::string &str) {
    this->Write(offset, str.c_str(), str.size());
  }
  void Write(int offset, const char *str, size_t str_length) {
    assert(str_length < MemoryChunk::kStringCellSize);
    if (str_length != 0u) {
      std::memcpy(buffer_ + offset, str, sizeof(char) * str_length);
    }
    *(buffer_ + offset + str_length) = '\0';
    *(buffer_ + offset + MemoryChunk::kMaxStringLength) =
        static_cast<char>(MemoryChunk::kMaxStringLength - str_length);
  }

private:
  char *buffer_;
  size_t size_;
};

template <>
inline int64_t MemoryChunk::Read<int64_t>(int offset) const {
  return ReadInt64(offset);
}

template <>
inline double MemoryChunk::Read<double>(int offset) const {
  return ReadDouble(offset);
}

template <>
inline std::string MemoryChunk::Read<std::string>(int offset) const {
  return ReadString(offset);
}

}  // namespace csv

#endif
