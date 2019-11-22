#include "document.h"

#include <gtest/gtest.h>

namespace {

// columns: [id, name, age, grade]
// column types: [int, string, int, double]

TEST(TestDocument, TestOneChunk) {
  csv::Document doc(std::vector<std::string>{"id", "name", "age", "grade"},
                    std::vector<csv::FieldType>{
                        csv::FieldType::INT64, csv::FieldType::STRING,
                        csv::FieldType::INT64, csv::FieldType::DOUBLE});
  doc.AddChunk(3);
  // [0, "A", 20, 2.7]
  doc.Write(0, 0, "0", 1);
  doc.Write(0, 1, "A", 1);
  doc.Write(0, 2, "20", 2);
  doc.Write(0, 3, "2.7", 3);

  // [1, "B", 19, 4.1]
  doc.Write(1, 0, "1", 1);
  doc.Write(1, 1, "B", 1);
  doc.Write(1, 2, "19", 2);
  doc.Write(1, 3, "4.1", 3);

  // [2, "AB", 9, 4.12]
  doc.Write(2, 0, "2", 1);
  doc.Write(2, 1, "AB", 2);
  doc.Write(2, 2, "9", 1);
  doc.Write(2, 3, "4.12", 4);

  std::vector<int64_t> ids = doc.GetAsInt64("id");
  std::vector<std::string> names = doc.GetAsString("name");
  std::vector<double> grades = doc.GetAsDouble("grade");
  ASSERT_EQ(3u, doc.NumRows());
  ASSERT_EQ(3u, ids.size());
  ASSERT_EQ(3u, names.size());
  ASSERT_EQ(3u, grades.size());

  EXPECT_EQ(0, ids[0]);
  EXPECT_EQ(1, ids[1]);
  EXPECT_EQ(2, ids[2]);

  EXPECT_STREQ("A", names[0].c_str());
  EXPECT_STREQ("B", names[1].c_str());
  EXPECT_STREQ("AB", names[2].c_str());

  EXPECT_DOUBLE_EQ(2.7, grades[0]);
  EXPECT_DOUBLE_EQ(4.1, grades[1]);
  EXPECT_DOUBLE_EQ(4.12, grades[2]);
}

TEST(TestDocument, TestTwoChunk) {
  csv::Document doc(std::vector<std::string>{"id", "name", "age", "grade"},
                    std::vector<csv::FieldType>{
                        csv::FieldType::INT64, csv::FieldType::STRING,
                        csv::FieldType::INT64, csv::FieldType::DOUBLE});
  doc.AddChunk(2);
  // [0, "A", 20, 2.7]
  doc.Write(0, 0, "0", 1);
  doc.Write(0, 1, "A", 1);
  doc.Write(0, 2, "20", 2);
  doc.Write(0, 3, "2.7", 3);

  // [1, "B", 19, 4.1]
  doc.Write(1, 0, "1", 1);
  doc.Write(1, 1, "B", 1);
  doc.Write(1, 2, "19", 2);
  doc.Write(1, 3, "4.1", 3);

  doc.AddChunk(2);
  // [2, "AB", 9, 4.12]
  doc.Write(2, 0, "2", 1);
  doc.Write(2, 1, "AB", 2);
  doc.Write(2, 2, "9", 1);
  doc.Write(2, 3, "4.12", 4);

  // [3, "ABCD", 24, 3.1415]
  doc.Write(3, 0, "3", 1);
  doc.Write(3, 1, "ABCD", 4);
  doc.Write(3, 2, "24", 2);
  doc.Write(3, 3, "3.1415", 6);

  std::vector<int64_t> ids = doc.GetAsInt64("id");
  std::vector<std::string> names = doc.GetAsString("name");
  std::vector<double> grades = doc.GetAsDouble("grade");
  std::vector<int64_t> ages = doc.GetAsInt64("age");
  ASSERT_EQ(4u, doc.NumRows());
  ASSERT_EQ(4u, ids.size());
  ASSERT_EQ(4u, names.size());
  ASSERT_EQ(4u, grades.size());
  ASSERT_EQ(4u, ages.size());

  EXPECT_EQ(0, ids[0]);
  EXPECT_EQ(1, ids[1]);
  EXPECT_EQ(2, ids[2]);
  EXPECT_EQ(3, ids[3]);

  EXPECT_STREQ("A", names[0].c_str());
  EXPECT_STREQ("B", names[1].c_str());
  EXPECT_STREQ("AB", names[2].c_str());
  EXPECT_STREQ("ABCD", names[3].c_str());

  EXPECT_DOUBLE_EQ(2.7, grades[0]);
  EXPECT_DOUBLE_EQ(4.1, grades[1]);
  EXPECT_DOUBLE_EQ(4.12, grades[2]);
  EXPECT_DOUBLE_EQ(3.1415, grades[3]);

  EXPECT_EQ(20, ages[0]);
  EXPECT_EQ(19, ages[1]);
  EXPECT_EQ(9, ages[2]);
  EXPECT_EQ(24, ages[3]);
}

TEST(TestDocument, TestDump) {
  csv::Document doc(std::vector<std::string>{"id", "name", "age", "grade"},
                    std::vector<csv::FieldType>{
                        csv::FieldType::INT64, csv::FieldType::STRING,
                        csv::FieldType::INT64, csv::FieldType::DOUBLE});
  doc.AddChunk(4);
  // [0, "A", 20, 2.7]
  doc.Write(0, 0, "0", 1);
  doc.Write(0, 1, "A", 1);
  doc.Write(0, 2, "20", 2);
  doc.Write(0, 3, "2.7", 3);

  // [1, "B", 19, 4.1]
  doc.Write(1, 0, "1", 1);
  doc.Write(1, 1, "B", 1);
  doc.Write(1, 2, "19", 2);
  doc.Write(1, 3, "4.1", 3);

  // [2, "AB", 9, 4.12]
  doc.Write(2, 0, "2", 1);
  doc.Write(2, 1, "AB", 2);
  doc.Write(2, 2, "9", 1);
  doc.Write(2, 3, "4.12", 4);

  // [3, "ABCD", 24, 3.1415]
  doc.Write(3, 0, "3", 1);
  doc.Write(3, 1, "ABCD", 4);
  doc.Write(3, 2, "24", 2);
  doc.Write(3, 3, "3.1415", 6);

  std::ostringstream os;
  doc.Dump(os);
  const std::string dumped = os.str();
  EXPECT_STREQ(
      "id,name,age,grade\n"
      "0,A,20,2.7\n"
      "1,B,19,4.1\n"
      "2,AB,9,4.12\n"
      "3,ABCD,24,3.1415\n",
      dumped.c_str());
}

}  // anonymous namespace
