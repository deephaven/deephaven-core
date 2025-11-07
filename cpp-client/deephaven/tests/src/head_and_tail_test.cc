/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"

using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Head and Tail", "[headtail]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  table = table.Where("ImportDate == `2017-11-01`");

  auto th = table.Head(2).Select("Ticker", "Volume");
  auto tt = table.Tail(2).Select("Ticker", "Volume");

//  std::cout << "==== Head(2) ====\n";
//  std::cout << th.Stream(true) << '\n';
//  std::cout << tt.Stream(true) << '\n';

  TableMaker expected_head;
  expected_head.AddColumn<std::string>("Ticker", {"XRX", "XRX"});
  expected_head.AddColumn<std::int64_t>("Volume", {345000, 87000});
  TableComparerForTests::Compare(expected_head, th);

  TableMaker expected_tail;
  expected_tail.AddColumn<std::string>("Ticker", {"ZNGA", "ZNGA"});
  expected_tail.AddColumn<std::int64_t>("Volume", {46123, 48300});
  TableComparerForTests::Compare(expected_tail, tt);
}
}  // namespace deephaven::client::tests
