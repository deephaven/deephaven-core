/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/third_party/fmt/format.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::DateTime;

namespace deephaven::client::tests {
TEST_CASE("Join", "[join]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  table = table.Where("ImportDate == `2017-11-01`");
  auto last_close = table.LastBy("Ticker");
  auto avg_view = table.View("Ticker", "Volume").AvgBy("Ticker");

  auto joined = last_close.NaturalJoin(avg_view,
      {"Ticker"},
      {"ADV = Volume"});

  auto filtered = joined.Select("Ticker", "Close", "ADV");

  TableMaker expected;
  expected.AddColumn<std::string>("Ticker", {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"});
  expected.AddColumn<double>("Close", {53.8, 88.5, 38.7, 453, 26.7, 544.9});
  expected.AddColumn<double>("ADV", {216000, 6060842, 138000, 138000000, 123000, 47211.50});
  TableComparerForTests::Compare(expected, filtered);
}

TEST_CASE("Aj", "[join]") {
  auto tm = TableMakerForTests::Create();

  TableHandle trades;
  {
    TableMaker table_maker;
    table_maker.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "AAPL", "IBM", "IBM"});
    table_maker.AddColumn<DateTime>("Timestamp", {
        DateTime::Parse("2021-04-05T09:10:00-0500"),
        DateTime::Parse("2021-04-05T09:31:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500")
    });
    table_maker.AddColumn<double>("Price", {2.5, 3.7, 3.0, 100.50, 110});
    table_maker.AddColumn<int32_t>("Size", {52, 14, 73, 11, 6});
    trades = table_maker.MakeTable(tm.Client().GetManager());
    // std::cout << trades.Stream(true) << '\n';
  }

  TableHandle quotes;
  {
    TableMaker table_maker;
    table_maker.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "IBM", "IBM", "IBM"});
    table_maker.AddColumn<DateTime>("Timestamp", {
        DateTime::Parse("2021-04-05T09:11:00-0500"),
        DateTime::Parse("2021-04-05T09:30:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500"),
        DateTime::Parse("2021-04-05T17:00:00-0500")
    });
    table_maker.AddColumn<double>("Bid", {2.5, 3.4, 97, 102, 108});
    table_maker.AddColumn<int32_t>("BidSize", {10, 20, 5, 13, 23});
    table_maker.AddColumn<double>("Ask", {2.5, 3.4, 105, 110, 111});
    table_maker.AddColumn<int32_t>("AskSize", {83, 33, 47, 15, 5});
    quotes = table_maker.MakeTable(tm.Client().GetManager());
    // std::cout << quotes.Stream(true) << '\n';
  }

  auto result = trades.Aj(quotes, {"Ticker", "Timestamp"});
  // std::cout << result.Stream(true) << '\n';

  TableMaker expected;
  expected.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "AAPL", "IBM", "IBM"});
  expected.AddColumn<DateTime>("Timestamp", {
      DateTime::Parse("2021-04-05T09:10:00-0500"),
      DateTime::Parse("2021-04-05T09:31:00-0500"),
      DateTime::Parse("2021-04-05T16:00:00-0500"),
      DateTime::Parse("2021-04-05T16:00:00-0500"),
      DateTime::Parse("2021-04-05T16:30:00-0500")});
  expected.AddColumn<double>("Price", {2.5, 3.7, 3.0, 100.50, 110});
  expected.AddColumn<int32_t>("Size", {52, 14, 73, 11, 6});
  expected.AddColumn<std::optional<double>>("Bid", {{}, 3.4, 3.4, 97, 102});
  expected.AddColumn<std::optional<int32_t>>("BidSize", {{}, 20, 20, 5, 13});
  expected.AddColumn<std::optional<double>>("Ask", {{}, 3.4, 3.4, 105, 110});
  expected.AddColumn<std::optional<int32_t>>("AskSize", {{}, 33, 33, 47, 15});

  TableComparerForTests::Compare(expected, result);
}

TEST_CASE("Raj", "[join]") {
  auto tm = TableMakerForTests::Create();
  TableHandle trades;
  {
    std::vector<std::string> ticker_data = {"AAPL", "AAPL", "AAPL", "IBM", "IBM"};
    std::vector<DateTime> instant_data = {
        DateTime::Parse("2021-04-05T09:10:00-0500"),
        DateTime::Parse("2021-04-05T09:31:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500")
    };
    std::vector<double> price_data = {2.5, 3.7, 3.0, 100.50, 110};
    std::vector<int32_t> size_data = {52, 14, 73, 11, 6};
    TableMaker table_maker;
    table_maker.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "AAPL", "IBM", "IBM"});
    table_maker.AddColumn<DateTime>("Timestamp", {
        DateTime::Parse("2021-04-05T09:10:00-0500"),
        DateTime::Parse("2021-04-05T09:31:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500")
    });
    table_maker.AddColumn<double>("Price", {2.5, 3.7, 3.0, 100.50, 110});
    table_maker.AddColumn<int32_t>("Size", {52, 14, 73, 11, 6});
    trades = table_maker.MakeTable(tm.Client().GetManager());
    // std::cout << trades.Stream(true) << '\n';
  }

  TableHandle quotes;
  {
    TableMaker table_maker;
    table_maker.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "IBM", "IBM", "IBM"});
    table_maker.AddColumn<DateTime>("Timestamp",  {
        DateTime::Parse("2021-04-05T09:11:00-0500"),
        DateTime::Parse("2021-04-05T09:30:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500"),
        DateTime::Parse("2021-04-05T17:00:00-0500")
    });
    table_maker.AddColumn<double>("Bid", {2.5, 3.4, 97, 102, 108});
    table_maker.AddColumn<int32_t>("BidSize", {10, 20, 5, 13, 23});
    table_maker.AddColumn<double>("Ask", {2.5, 3.4, 105, 110, 111});
    table_maker.AddColumn<int32_t>("AskSize", {83, 33, 47, 15, 5});
    quotes = table_maker.MakeTable(tm.Client().GetManager());
    // std::cout << quotes.Stream(true) << '\n';
  }

  auto result = trades.Raj(quotes, {"Ticker", "Timestamp"});
  // std::cout << result.Stream(true) << '\n';

  // Expected data
  {
    TableMaker expected;
    expected.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "AAPL", "IBM", "IBM"});
    expected.AddColumn<DateTime>("Timestamp", {
        DateTime::Parse("2021-04-05T09:10:00-0500"),
        DateTime::Parse("2021-04-05T09:31:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500")
    });
    expected.AddColumn<double>("Price", {2.5, 3.7, 3.0, 100.50, 110});
    expected.AddColumn<int32_t>("Size", {52, 14, 73, 11, 6});
    expected.AddColumn<std::optional<double>>("Bid", {2.5, {}, {}, 97, 102});
    expected.AddColumn<std::optional<int32_t>>("BidSize", {10, {}, {}, 5, 13});
    expected.AddColumn<std::optional<double>>("Ask", {2.5, {}, {}, 105, 110});
    expected.AddColumn<std::optional<int32_t>>("AskSize", {83, {}, {}, 47, 15});

    TableComparerForTests::Compare(expected, result);
  }
}
}  // namespace deephaven::client::tests
