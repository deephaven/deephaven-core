/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <optional>

#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/utility/utility.h"

using deephaven::client::DeephavenConstants;
using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::client::SortPair;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("New Table", "[newtable]") {
  auto tm = TableMakerForTests::create();

  // std::vector<std::optional<bool>> boolData = { {}, false, true, false, false, true };
  std::vector<std::optional<int8_t>> byteData = { {}, 0, 1, -1, DeephavenConstants::MIN_BYTE, DeephavenConstants::MAX_BYTE };
  std::vector<std::optional<int16_t>> shortData = { {}, 0, 1, -1, DeephavenConstants::MIN_SHORT, DeephavenConstants::MAX_SHORT };
  std::vector<std::optional<int32_t>> intData = { {}, 0, 1, -1, DeephavenConstants::MIN_INT, DeephavenConstants::MAX_INT };
  std::vector<std::optional<int64_t>> longData = { {}, 0L, 1L, -1L, DeephavenConstants::MIN_LONG, DeephavenConstants::MAX_LONG };
  std::vector<std::optional<float>> floatData = { {}, 0.0f, 1.0f, -1.0f, -3.4e+38f, std::numeric_limits<float>::max() };
  std::vector<std::optional<double>> doubleData = { {}, 0.0, 1.0, -1.0, -1.79e+308, std::numeric_limits<double>::max() };
  std::vector<std::optional<std::string>> stringData = { {}, "", "A string", "Also a string", "AAAAAA", "ZZZZZZ" };

  TableMaker maker;
  // maker.addColumn("BoolValue", boolData);
  maker.addColumn("ByteValue", byteData);
  maker.addColumn("ShortValue", shortData);
  maker.addColumn("IntValue", intData);
  maker.addColumn("LongValue", longData);
  maker.addColumn("FloatValue", floatData);
  maker.addColumn("DoubleValue", doubleData);
  maker.addColumn("StringValue", stringData);
  auto temp = maker.makeTable(tm.client().getManager());
  std::cout << temp.stream(true) << '\n';
}
}  // namespace deephaven::client::tests
