#include "chunk.h"

#include <gtest/gtest.h>

namespace {

using MemoryChunk = csv::MemoryChunk;

TEST(TestCSVMemoryChunk, ReadWriteInt64) {
  MemoryChunk chunk(1000);

  int offset = 0;
  chunk.Write(sizeof(int64_t) * offset++, int64_t{0});
  chunk.Write(sizeof(int64_t) * offset++, int64_t{1});
  chunk.Write(sizeof(int64_t) * offset++, int64_t{2});
  chunk.Write(sizeof(int64_t) * offset++, int64_t{3});
  chunk.Write(50, int64_t{-1});

  EXPECT_EQ(0, chunk.ReadInt64(0));
  EXPECT_EQ(1, chunk.ReadInt64(sizeof(int64_t) * 1));
  EXPECT_EQ(2, chunk.ReadInt64(sizeof(int64_t) * 2));
  EXPECT_EQ(3, chunk.ReadInt64(sizeof(int64_t) * 3));
  EXPECT_EQ(-1, chunk.ReadInt64(50));
}

TEST(TestCSVMemoryChunk, ReadWriteDouble) {
  MemoryChunk chunk(1000);

  int offset = 0;
  chunk.Write(sizeof(double) * offset++, 0.0);
  chunk.Write(sizeof(double) * offset++, 1.1);
  chunk.Write(sizeof(double) * offset++, 2.2);
  chunk.Write(sizeof(double) * offset++, 3.3);
  chunk.Write(50, -1.1);

  constexpr double diff = 0.0000000001;

  EXPECT_NEAR(0.0, chunk.ReadDouble(0), diff);
  EXPECT_NEAR(1.1, chunk.ReadDouble(sizeof(double) * 1), diff);
  EXPECT_NEAR(2.2, chunk.ReadDouble(sizeof(double) * 2), diff);
  EXPECT_NEAR(3.3, chunk.ReadDouble(sizeof(double) * 3), diff);
  EXPECT_NEAR(-1.1, chunk.ReadDouble(50), diff);
}

TEST(TestCSVMemoryChunk, ReadWriteString) {
  MemoryChunk chunk(1000);

  constexpr size_t string_buffer_size = MemoryChunk::kStringCellSize;
  int offset = 0;

  chunk.Write(string_buffer_size * offset++, "yesterday", 9u);
  chunk.Write(string_buffer_size * offset++, "all my troubles", 15u);
  chunk.Write(string_buffer_size * offset++, "seemed so far away", 18u);
  chunk.Write(string_buffer_size * offset++,
              "Now it looks as though they're here to stay", 43u);
  chunk.Write(500,
              "exact 63 length string to match max string size of MemoryChunk.",
              63u);

  ASSERT_EQ(9u, chunk.ReadStrLength(string_buffer_size * 0));
  ASSERT_EQ(15u, chunk.ReadStrLength(string_buffer_size * 1));
  ASSERT_EQ(18u, chunk.ReadStrLength(string_buffer_size * 2));
  ASSERT_EQ(43u, chunk.ReadStrLength(string_buffer_size * 3));
  ASSERT_EQ(63u, chunk.ReadStrLength(500));

  EXPECT_EQ(std::string("yesterday"), chunk.ReadString(string_buffer_size * 0));
  EXPECT_EQ(std::string("all my troubles"),
            chunk.ReadString(string_buffer_size * 1));
  EXPECT_EQ(std::string("seemed so far away"),
            chunk.ReadString(string_buffer_size * 2));
  EXPECT_EQ(std::string("Now it looks as though they're here to stay"),
            chunk.ReadString(string_buffer_size * 3));
  EXPECT_EQ(
      std::string(
          "exact 63 length string to match max string size of MemoryChunk."),
      chunk.ReadString(500));
}

} // namespace
