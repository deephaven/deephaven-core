/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <optional>

#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::client::SortPair;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::DeephavenConstants;

namespace deephaven::client::tests {
TEST_CASE("New Table", "[newtable]") {
  auto tm = TableMakerForTests::Create();

  // std::vector<std::optional<bool>> boolData = { {}, false, true, false, false, true };
  std::vector<std::optional<int8_t>> byte_data = { {}, 0, 1, -1, DeephavenConstants::kMinByte, DeephavenConstants::kMaxByte };
  std::vector<std::optional<int16_t>> short_data = { {}, 0, 1, -1, DeephavenConstants::kMinShort, DeephavenConstants::kMaxShort };
  std::vector<std::optional<int32_t>> int_data = { {}, 0, 1, -1, DeephavenConstants::kMinInt, DeephavenConstants::kMaxInt };
  std::vector<std::optional<int64_t>> long_data = { {}, 0L, 1L, -1L, DeephavenConstants::kMinLong, DeephavenConstants::kMaxLong };
  std::vector<std::optional<float>> float_data = { {}, 0.0F, 1.0F, -1.0F, -3.4e+38F, std::numeric_limits<float>::max() };
  std::vector<std::optional<double>> double_data = { {}, 0.0, 1.0, -1.0, -1.79e+308, std::numeric_limits<double>::max() };
  std::vector<std::optional<std::string>> string_data = { {}, "", "A string", "Also a string", "AAAAAA", "ZZZZZZ" };

  TableMaker maker;
  // maker.addColumn("BoolValue", boolData);
  maker.AddColumn("ByteValue", byte_data);
  maker.AddColumn("ShortValue", short_data);
  maker.AddColumn("IntValue", int_data);
  maker.AddColumn("LongValue", long_data);
  maker.AddColumn("FloatValue", float_data);
  maker.AddColumn("DoubleValue", double_data);
  maker.AddColumn("StringValue", string_data);
  auto temp = maker.MakeTable(tm.Client().GetManager());
  std::cout << temp.Stream(true) << '\n';
}
}  // namespace deephaven::client::tests
