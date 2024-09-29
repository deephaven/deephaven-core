/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/chunk/chunk.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::Int32Chunk;
using deephaven::dhcore::chunk::Int64Chunk;
using deephaven::dhcore::container::RowSequence;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::LocalDate;
using deephaven::dhcore::LocalTime;
using deephaven::dhcore::DeephavenConstants;
using deephaven::dhcore::utility::MakeReservedVector;

namespace deephaven::client::tests {
TEST_CASE("Fetch the entire table (small)", "[client_table]") {
  int64_t target = 10;
  auto tm = TableMakerForTests::Create();
  auto thm = tm.Client().GetManager();
  auto th = thm.EmptyTable(target)
      .Update({
        "Chars = ii == 5 ? null : (char)('a' + ii)",
        "Bytes = ii == 5 ? null : (byte)(ii)",
        "Shorts = ii == 5 ? null : (short)(ii)",
        "Ints = ii == 5 ? null : (int)(ii)",
        "Longs = ii == 5 ? null : (long)(ii)",
        "Floats = ii == 5 ? null : (float)(ii)",
        "Doubles = ii == 5 ? null : (double)(ii)",
        "Bools = ii == 5 ? null : ((ii % 2) == 0)",
        "Strings = ii == 5 ? null : `hello ` + i",
        "DateTimes = ii == 5 ? null : '2001-03-01T12:34:56Z' + ii",
        "LocalDates = ii == 5 ? null : '2001-03-01' + ((int)ii * 'P1D')",
        "LocalTimes = ii == 5 ? null : '12:34:46'.plus((int)ii * 'PT1S')"
      });
  std::cout << th.Stream(true) << '\n';

  auto ct = th.ToClientTable();
  auto schema = ct->Schema();

  auto chars = MakeReservedVector<std::optional<char16_t>>(target);
  auto int8s = MakeReservedVector<std::optional<int8_t>>(target);
  auto int16s = MakeReservedVector<std::optional<int16_t>>(target);
  auto int32s = MakeReservedVector<std::optional<int32_t>>(target);
  auto int64s = MakeReservedVector<std::optional<int64_t>>(target);
  auto floats = MakeReservedVector<std::optional<float>>(target);
  auto doubles = MakeReservedVector<std::optional<double>>(target);
  auto bools = MakeReservedVector<std::optional<bool>>(target);
  auto strings = MakeReservedVector<std::optional<std::string>>(target);
  auto date_times = MakeReservedVector<std::optional<DateTime>>(target);
  auto local_dates = MakeReservedVector<std::optional<LocalDate>>(target);
  auto local_times = MakeReservedVector<std::optional<LocalTime>>(target);

  auto date_time_start = DateTime::Parse("2001-03-01T12:34:56Z");

  for (int64_t i = 0; i != target; ++i) {
    chars.emplace_back(static_cast<char16_t>('a' + i));
    int8s.emplace_back(static_cast<int8_t>(i));
    int16s.emplace_back(static_cast<int16_t>(i));
    int32s.emplace_back(static_cast<int32_t>(i));
    int64s.emplace_back(static_cast<int64_t>(i));
    floats.emplace_back(static_cast<float>(i));
    doubles.emplace_back(static_cast<double>(i));
    bools.emplace_back((i % 2) == 0);
    strings.emplace_back(fmt::format("hello {}", i));
    date_times.emplace_back(DateTime::FromNanos(date_time_start.Nanos() + i));
    local_dates.emplace_back(LocalDate::Of(2001, 3, i + 1));
    local_times.emplace_back(LocalTime::Of(12, 34, 46 + i));
  }

  auto t2 = target / 2;
  // Set the middle element to the unset optional, which for the purposes of this test is
  // our representation of null.
  chars[t2] = {};
  int8s[t2] = {};
  int16s[t2] = {};
  int32s[t2] = {};
  int64s[t2] = {};
  floats[t2] = {};
  doubles[t2] = {};
  bools[t2] = {};
  strings[t2] = {};
  date_times[t2] = {};
  local_dates[t2] = {};
  local_times[t2] = {};

  CompareColumn(*ct, "Chars", chars);
  CompareColumn(*ct, "Bytes", int8s);
  CompareColumn(*ct, "Shorts", int16s);
  CompareColumn(*ct, "Ints", int32s);
  CompareColumn(*ct, "Longs", int64s);
  CompareColumn(*ct, "Floats", floats);
  CompareColumn(*ct, "Doubles", doubles);
  CompareColumn(*ct, "Bools", bools);
  CompareColumn(*ct, "Strings", strings);
  CompareColumn(*ct, "DateTimes", date_times);
  CompareColumn(*ct, "LocalDates", local_dates);
  CompareColumn(*ct, "LocalTimes", local_times);
}
}  // namespace deephaven::client::tests
