/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/client/utility/internal_types.h"
#include "deephaven/client/utility/table_maker.h"
#include "deephaven/dhcore/container/row_sequence.h"
#include "deephaven/dhcore/types.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::client::utility::internal::InternalDateTime;
using deephaven::client::utility::internal::InternalLocalTime;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::DeephavenConstants;
using deephaven::dhcore::LocalDate;
using deephaven::dhcore::LocalTime;
using deephaven::dhcore::chunk::BooleanChunk;
using deephaven::dhcore::chunk::DateTimeChunk;
using deephaven::dhcore::chunk::LocalTimeChunk;
using deephaven::dhcore::container::RowSequence;

namespace deephaven::client::tests {
TEST_CASE("Uploaded Arrow Timestamp units get normalized to nanos at FillChunk time", "[timeunit]") {
  auto tm = TableMakerForTests::Create();

  std::vector<std::optional<InternalDateTime<arrow::TimeUnit::SECOND>>> dt_sec;
  std::vector<std::optional<InternalDateTime<arrow::TimeUnit::MILLI>>> dt_milli;
  std::vector<std::optional<InternalDateTime<arrow::TimeUnit::MICRO>>> dt_micro;
  std::vector<std::optional<InternalDateTime<arrow::TimeUnit::NANO>>> dt_nano;

  // First row: one second (in various units)
  dt_sec.emplace_back(1);
  dt_milli.emplace_back(1'000);
  dt_micro.emplace_back(1'000'000);
  dt_nano.emplace_back(1'000'000'000);

  // Second row: null
  dt_sec.emplace_back();
  dt_milli.emplace_back();
  dt_micro.emplace_back();
  dt_nano.emplace_back();

  TableMaker maker;
  maker.AddColumn("dt_sec", dt_sec);
  maker.AddColumn("dt_milli", dt_milli);
  maker.AddColumn("dt_micro", dt_micro);
  maker.AddColumn("dt_nano", dt_nano);
  auto t = maker.MakeTable(tm.Client().GetManager());

  std::cout << t.Stream(true) << '\n';

  auto client_table = t.ToClientTable();

  auto rs = RowSequence::CreateSequential(0, 2);
  auto dest = DateTimeChunk::Create(2);
  auto nulls = BooleanChunk::Create(2);

  auto expected = DateTime::FromNanos(1'000'000'000);

  for (size_t i = 0; i != client_table->NumColumns(); ++i) {
    auto col = client_table->GetColumn(i);
    col->FillChunk(*rs, &dest, &nulls);

    CHECK(expected == dest.data()[0]);
    CHECK(false == nulls.data()[0]);

    CHECK(true == nulls.data()[1]);
  }
}

TEST_CASE("Uploaded Arrow Time64 units get normalized to nanos at FillChunk time", "[timeunit]") {
  auto tm = TableMakerForTests::Create();

  std::vector<std::optional<InternalLocalTime<arrow::TimeUnit::MICRO>>> lt_micro;
  std::vector<std::optional<InternalLocalTime<arrow::TimeUnit::NANO>>> lt_nano;

  // First row: one second (in various units)
  lt_micro.emplace_back(1'000'000);
  lt_nano.emplace_back(1'000'000'000);

  // Second row: null
  lt_micro.emplace_back();
  lt_nano.emplace_back();

  TableMaker maker;
  maker.AddColumn("lt_micro", lt_micro);
  maker.AddColumn("lt_nano", lt_nano);
  auto t = maker.MakeTable(tm.Client().GetManager());

  std::cout << t.Stream(true) << '\n';

  auto client_table = t.ToClientTable();

  auto rs = RowSequence::CreateSequential(0, 2);
  auto dest = LocalTimeChunk::Create(2);
  auto nulls = BooleanChunk::Create(2);

  auto expected = LocalTime::FromNanos(1'000'000'000);

  for (size_t i = 0; i != client_table->NumColumns(); ++i) {
    auto col = client_table->GetColumn(i);
    col->FillChunk(*rs, &dest, &nulls);

    CHECK(expected == dest.data()[0]);
    CHECK(false == nulls.data()[0]);

    CHECK(true == nulls.data()[1]);
  }
}
}  // namespace deephaven::client::tests
