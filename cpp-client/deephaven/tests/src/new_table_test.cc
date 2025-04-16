/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <optional>

#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::utility::TableMaker;
using deephaven::dhcore::DeephavenConstants;

namespace deephaven::client::tests {
TEST_CASE("New Table", "[newtable]") {
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<std::optional<bool>>("BoolValue",
      { {}, false, true, false, false, true });
  maker.AddColumn<std::optional<int8_t>>("ByteValue",
      { {}, 0, 1, -1, DeephavenConstants::kMinByte, DeephavenConstants::kMaxByte });
  maker.AddColumn<std::optional<int16_t>>("ShortValue",
      { {}, 0, 1, -1, DeephavenConstants::kMinShort, DeephavenConstants::kMaxShort });
  maker.AddColumn<std::optional<int32_t>>("IntValue",
      { {}, 0, 1, -1, DeephavenConstants::kMinInt, DeephavenConstants::kMaxInt });
  maker.AddColumn<std::optional<int64_t>>("LongValue",
      { {}, 0L, 1L, -1L, DeephavenConstants::kMinLong, DeephavenConstants::kMaxLong });
  maker.AddColumn<std::optional<float>>("FloatValue",
      { {}, 0.0F, 1.0F, -1.0F, -3.4e+38F, std::numeric_limits<float>::max() });
  maker.AddColumn<std::optional<double>>("DoubleValue",
      { {}, 0.0, 1.0, -1.0, -1.79e+308, std::numeric_limits<double>::max() });
  maker.AddColumn<std::optional<std::string>>("StringValue",
      { {}, "", "A string", "Also a string", "AAAAAA", "ZZZZZZ" });

  auto temp = maker.MakeTable(tm.Client().GetManager());
  std::cout << temp.Stream(true) << '\n';
}
}  // namespace deephaven::client::tests
