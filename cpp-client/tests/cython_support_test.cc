/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include <type_traits>
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/column/array_column_source.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/utility/cython_support.h"
#include "tests/third_party/catch.hpp"

using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::column::StringArrayColumnSource;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::utility::CythonSupport;
using deephaven::dhcore::chunk::StringChunk;

namespace deephaven::client::tests {
TEST_CASE("CreateStringColumnsSource", "[cython]") {
  // hello, NULL, Deephaven, abc
  // We do these as constexpr so our test won't even compile unless we get the basics right
  constexpr const char *text = "helloDeephavenabc";
  constexpr size_t textSize = std::char_traits<char>::length(text);
  constexpr std::array<uint32_t, 5> offsets = {0, 5, 5, 14, 17};
  constexpr std::array<uint8_t, 1> validity = {0b1101};

  static_assert(textSize == *(offsets.end() - 1));
  constexpr auto numElements = offsets.size() - 1;
  static_assert((numElements + 7) / 8 == validity.size());

  auto result = CythonSupport::createStringColumnSource(text, text + textSize,
      offsets.begin(), offsets.end(), validity.begin(), validity.end(), numElements);

  auto rs = RowSequence::createSequential(0, numElements);
  auto data = dhcore::chunk::StringChunk::create(numElements);
  auto nullFlags = dhcore::chunk::BooleanChunk::create(numElements);
  result->fillChunk(*rs, &data, &nullFlags);

  std::vector<std::string> expectedData = {"hello", "", "Deephaven", "abc"};
  std::vector<bool> expectedNulls = {false, true, false, false};

  std::vector<std::string> actualData(data.begin(), data.end());
  std::vector<bool> actualNulls(nullFlags.begin(), nullFlags.end());

  CHECK(expectedData == actualData);
  CHECK(expectedNulls == actualNulls);
}
}  // namespace deephaven::client::tests
