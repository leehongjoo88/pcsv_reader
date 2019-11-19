#ifndef __DOCUMENT_H__
#define __DOCUMENT_H__

#include <memory>
#include <vector>

#include "base.h"
#include "chunk.h"

namespace csv {

class Document {
public:
  Document(const std::vector<std::string>& field_names,
           const std::vector<FieldType>& field_types);
  void Write(size_t row, size_t column, const char *str, size_t str_length);
  void AddChunk(size_t num_rows);
  size_t NumRows() const;
  std::vector<int64_t> GetAsInt64(const std::string& column) const;
  std::vector<std::string> GetAsString(const std::string& column) const;
  std::vector<double> GetAsDouble(const std::string& column) const;
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
  std::vector<std::string> field_names_;
  size_t num_cols_;
  size_t actual_row_byte_size_;
  std::vector<ColumnInfo> column_infos_;
  std::vector<DocumentMemoryChunk> buffer_;
  MemoryChunk *current_memory_chunk_;
  int current_row_offset_in_chunk_;
};

} // namespace csv

#endif
