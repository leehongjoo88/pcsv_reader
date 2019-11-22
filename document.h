#ifndef __DOCUMENT_H__
#define __DOCUMENT_H__

#include <memory>
#include <ostream>
#include <vector>

#include "base.h"
#include "chunk.h"

namespace csv {

// Document holds parsed CSV content.
// Parsed content can ge retreived using GetAs* methods.
// To get contents from Document fast, set number of threads to bigger numbers
// using SetNumThreads() (default = 1).
class Document {
public:
  Document(const std::vector<std::string>& field_names,
           const std::vector<FieldType>& field_types);
  const std::vector<std::string>& FieldNames() const { return field_names_; }

  int NumThreads() const { return num_threads_; }
  void SetNumThreads(int num_threads) {
    num_threads_ = num_threads;
  }

  void Write(size_t row, size_t column, const char *str, size_t str_length);
  void AddChunk(size_t num_rows);
  size_t NumRows() const;
  std::vector<int64_t> GetAsInt64(const std::string& column) const;
  void GetAsInt64(const std::string& column, std::vector<int64_t>& result) const;
  std::vector<std::string> GetAsString(const std::string& column) const;
  void GetAsString(const std::string& column, std::vector<std::string>& result) const;
  std::vector<double> GetAsDouble(const std::string& column) const;
  void GetAsDouble(const std::string& column, std::vector<double>& result) const;

  void Dump(std::ostream& os) const;
private:
  struct ColumnInfo {
    FieldType type;
    int offset;
    int size;
  };
  struct DocumentMemoryChunk {
    std::unique_ptr<MemoryChunk> chunk;
    size_t num_rows;
  };

  // Get() assigns column's result to output.
  // This method only should be used internally in document.cpp
  // Expects: output.size() == NumRows()
  template <typename T>
  void Get(const std::string& column, std::vector<T>& output) const;
  
  std::vector<std::string> field_names_;
  size_t num_cols_;
  size_t actual_row_byte_size_;
  std::vector<ColumnInfo> column_infos_;
  std::vector<DocumentMemoryChunk> buffer_;
  MemoryChunk *current_memory_chunk_;
  int current_row_offset_in_chunk_;
  int num_threads_;
};

} // namespace csv

#endif
