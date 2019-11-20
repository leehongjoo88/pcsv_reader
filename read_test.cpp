#include "read.h"

#include <cstdio>
#include <fstream>
#include <sstream>
#include <gtest/gtest.h>

namespace {

struct TempFileHandle {
  std::string file_name;
  TempFileHandle(): file_name(std::tmpnam(nullptr)) {}
  ~TempFileHandle() { if (!file_name.empty()) std::remove(file_name.c_str()); }
};

TEST(TestReadCSV, ColumnNames) {
  std::stringstream ss;
  ss << "id,name,age,grade\n";
  auto column_names = csv::ColumnNames(ss, "");
  ASSERT_EQ(4u, column_names.size());
  EXPECT_STREQ("id", column_names[0].c_str());
  EXPECT_STREQ("name", column_names[1].c_str());
  EXPECT_STREQ("age", column_names[2].c_str());
  EXPECT_STREQ("grade", column_names[3].c_str());
}

TEST(TestReadCSV, ReadCSV) {
  const std::string file_content = "id,name,age,grade\n"
                                   "0,A,20,2.7\n"
                                   "1,B,19,4.1\n"
                                   "2,AB,9,4.12\n"
                                   "3,ABCD,24,3.1415\n";
  TempFileHandle file_handle;
  std::ofstream ofs(file_handle.file_name);
  ofs << file_content;
  ofs.close();

  auto document = csv::ReadCSV(file_handle.file_name,
                               {csv::FieldType::INT64, csv::FieldType::STRING,
                                csv::FieldType::INT64, csv::FieldType::DOUBLE});

  std::vector<int64_t> ids = document.GetAsInt64("id");
  std::vector<std::string> names = document.GetAsString("name");
  std::vector<int64_t> ages = document.GetAsInt64("age");
  std::vector<double> grades = document.GetAsDouble("grade");

  ASSERT_EQ(4u, ids.size());
  ASSERT_EQ(4u, names.size());
  ASSERT_EQ(4u, ages.size());
  ASSERT_EQ(4u, grades.size());
}

}
