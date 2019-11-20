#include "read.h"

#include <sstream>
#include <gtest/gtest.h>

namespace {

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

TEST(TestReadCSV, EstimateNumberOfLines) {
  std::stringstream ss;
  ss << "id,name,age,grade\n"
        "0,A,20,2.7\n"
        "1,B,19,4.1\n"
        "2,C,21,3.5\n"
        "3,D,18,4.0\n";

  csv::ColumnNames(ss, "");
  ASSERT_EQ(4u, csv::EstimateNumberOfLines(ss));
}

}
