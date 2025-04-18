/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <vector>

#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using Catch::Matchers::StartsWith;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::DeephavenConstants;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::LocalDate;
using deephaven::dhcore::LocalTime;

namespace deephaven::client::tests {

// Test schema validation
TEST_CASE("Schema validation", "[newtable]") {
  TableMaker maker;
  maker.AddColumn<bool>("Bools", { false, true, false, false, true });
  // Inconsistent number of rows
  maker.AddColumn<int32_t>("Ints", { 0, 1, 2, 3 });

  REQUIRE_THROWS_WITH(maker.MakeArrowTable(), StartsWith("Column sizes not consistent"));
}

// Make tables out of scalar types and see if they successfully round-trip.
TEST_CASE("Scalar Types", "[newtable]") {
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<std::optional<bool>>("Bools",
      { {}, false, true, false, false, true });
  maker.AddColumn<std::optional<char16_t>>("Chars",
      { {}, 0, 'a', u'ᐾ', DeephavenConstants::kMinChar, DeephavenConstants::kMaxChar });
  maker.AddColumn<std::optional<int8_t>>("Bytes",
      { {}, 0, 1, -1, DeephavenConstants::kMinByte, DeephavenConstants::kMaxByte });
  maker.AddColumn<std::optional<int16_t>>("Shorts",
      { {}, 0, 1, -1, DeephavenConstants::kMinShort, DeephavenConstants::kMaxShort });
  maker.AddColumn<std::optional<int32_t>>("Ints",
      { {}, 0, 1, -1, DeephavenConstants::kMinInt, DeephavenConstants::kMaxInt });
  maker.AddColumn<std::optional<int64_t>>("Longs",
      { {}, 0L, 1L, -1L, DeephavenConstants::kMinLong, DeephavenConstants::kMaxLong });
  maker.AddColumn<std::optional<float>>("Floats",
      { {}, 0.0F, 1.0F, -1.0F, -3.4e+38F, std::numeric_limits<float>::max() });
  maker.AddColumn<std::optional<double>>("Doubles",
      { {}, 0.0, 1.0, -1.0, -1.79e+308, std::numeric_limits<double>::max() });
  maker.AddColumn<std::optional<std::string>>("Strings",
      { {}, "", "A string", "Also a string", "AAAAAA", "ZZZZZZ" });
  maker.AddColumn<std::optional<DateTime>>("DateTimes",
      { {}, DateTime(), DateTime::FromNanos(-1), DateTime::FromNanos(1),
          DateTime::Parse("2020-03-01T12:34:56Z"), DateTime::Parse("1900-05-05T11:22:33Z") });
  maker.AddColumn<std::optional<LocalDate>>("LocalDates",
      { {}, LocalDate(), LocalDate::FromMillis(-86'400'000), LocalDate::FromMillis(86'400'000),
          LocalDate::Of(2020, 3, 1), LocalDate::Of(1900, 5, 5) });
  maker.AddColumn<std::optional<LocalTime>>("LocalTimes",
      { {}, LocalTime(), LocalTime::FromNanos(1), LocalTime::FromNanos(10'000'000'000),
          LocalTime::Of(12, 34, 56), LocalTime::Of(11, 22, 33) });

  auto dh_table = maker.MakeTable(tm.Client().GetManager());

  std::cout << dh_table.Stream(true) << '\n';

  TableComparerForTests::Compare(maker, dh_table);
}

// Make tables out of list types and see if they successfully round-trip.
TEST_CASE("List Types", "[newtable]") {
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<std::optional<std::vector<std::optional<bool>>>>("Bools", {
      {}, // a null list
      { { false, true } }, // a non-null list
      { { false, true, {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<char16_t>>>>("Chars", {
      {}, // a null list
      { { 'a', u'ᐾ' } }, // a non-null list
      { { 'a', u'ᐾ', {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<int8_t>>>>("Bytes", {
      {}, // a null list
      { { 0, 35 } }, // a non-null list
      { { 0, 35, {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<int16_t>>>>("Shorts", {
      {}, // a null list
      { { 0, 1111 } }, // a non-null list
      { { 0, 1111, {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<int32_t>>>>("Ints", {
      {}, // a null list
      { { 0, 91919 } }, // a non-null list
      { { 0, 91919, {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<int64_t>>>>("Longs", {
      {}, // a null list
      { { 0, 987654321 } }, // a non-null list
      { { 0, 987654321, {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<float>>>>("Floats", {
      {}, // a null list
      { { 0, 123.456 } }, // a non-null list
      { { 0, 123.456, {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<double>>>>("Doubles", {
      {}, // a null list
      { { 0, 123.456 } }, // a non-null list
      { { 0, 123.456, {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<std::string>>>>("Strings", {
      {}, // a null list
      { { "", "hello" } }, // a non-null list
      { { "", "hello", {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<DateTime>>>>("DateTimes", {
      {}, // a null list
      { { DateTime(), DateTime::Parse("2020-03-01T12:34:56Z") } }, // a non-null list
      { { DateTime(), DateTime::Parse("2020-03-01T12:34:56Z"), {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<LocalDate>>>>("LocalDates", {
      {}, // a null list
      { { LocalDate(), LocalDate::Of(2020, 3, 1) } }, // a non-null list
      { { LocalDate(), LocalDate::Of(2020, 3, 1), {} } } // a non-null list with a null entry
  });
  maker.AddColumn<std::optional<std::vector<std::optional<LocalTime>>>>("LocalTimes", {
      {}, // a null list
      { { LocalTime(), LocalTime::Of(11, 22, 33) } }, // a non-null list
      { { LocalTime(), LocalTime::Of(11, 22, 33), {} } } // a non-null list with a null entry
  });

  auto dh_table = maker.MakeTable(tm.Client().GetManager());

  std::cout << dh_table.Stream(true) << '\n';

  TableComparerForTests::Compare(maker, dh_table);
}
}  // namespace deephaven::client::tests
