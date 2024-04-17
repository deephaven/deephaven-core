/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <array>
#include <type_traits>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/array_column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/utility/cython_support.h"
#include "deephaven/third_party/catch.hpp"

using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::column::StringArrayColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::utility::CythonSupport;
using deephaven::dhcore::chunk::StringChunk;

namespace deephaven::client::tests {
TEST_CASE("CreateStringColumnsSource", "[cython]") {
  // hello, NULL, Deephaven, abc
  // We do these as constexpr so our test won't even compile unless we get the basics right
  constexpr const char *kText = "helloDeephavenabc";
  constexpr size_t kTextSize = std::char_traits<char>::length(kText);
  constexpr std::array<uint32_t, 5> kOffsets = {0, 5, 5, 14, 17};
  constexpr std::array<uint8_t, 1> kValidity = {0b1101};

  static_assert(kTextSize == *(kOffsets.end() - 1));
  constexpr auto kNumElements = kOffsets.size() - 1;
  static_assert((kNumElements + 7) / 8 == kValidity.size());

  auto result = CythonSupport::CreateStringColumnSource(kText, kText + kTextSize,
      kOffsets.data(), kOffsets.data() + kOffsets.size(),
      kValidity.data(), kValidity.data() + kValidity.size(),
      kNumElements);

  auto rs = RowSequence::CreateSequential(0, kNumElements);
  auto data = dhcore::chunk::StringChunk::Create(kNumElements);
  auto null_flags = dhcore::chunk::BooleanChunk::Create(kNumElements);
  result->FillChunk(*rs, &data, &null_flags);

  std::vector<std::string> expected_data = {"hello", "", "Deephaven", "abc"};
  std::vector<bool> expected_nulls = {false, true, false, false};

  std::vector<std::string> actual_data(data.begin(), data.end());
  std::vector<bool> actual_nulls(null_flags.begin(), null_flags.end());

  CHECK(expected_data == actual_data);
  CHECK(expected_nulls == actual_nulls);
}
}  // namespace deephaven::client::tests
