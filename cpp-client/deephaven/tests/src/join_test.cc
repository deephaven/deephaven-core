/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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

  std::vector<std::string> ticker_data = {"XRX", "XYZZY", "IBM", "GME", "AAPL", "ZNGA"};
  std::vector<double> close_data = {53.8, 88.5, 38.7, 453, 26.7, 544.9};
  std::vector<double> adv_data = {216000, 6060842, 138000, 138000000, 123000, 47211.50};

  CompareTable(
      filtered,
      "Ticker", ticker_data,
      "Close", close_data,
      "ADV", adv_data
  );
}

TEST_CASE("Aj", "[join]") {
  auto tm = TableMakerForTests::Create();
  auto q = arrow::timestamp(arrow::TimeUnit::NANO, "UTC");

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
    table_maker.AddColumn("Ticker", ticker_data);
    table_maker.AddColumn("Timestamp", instant_data);
    table_maker.AddColumn("Price", price_data);
    table_maker.AddColumn("Size", size_data);
    trades = table_maker.MakeTable(tm.Client().GetManager());
    // std::cout << trades.Stream(true) << '\n';
  }

  TableHandle quotes;
  {
    std::vector<std::string> ticker_data = {"AAPL", "AAPL", "IBM", "IBM", "IBM"};
    std::vector<DateTime> timestamp_data = {
        DateTime::Parse("2021-04-05T09:11:00-0500"),
        DateTime::Parse("2021-04-05T09:30:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500"),
        DateTime::Parse("2021-04-05T17:00:00-0500")
    };
    std::vector<double> bid_data = {2.5, 3.4, 97, 102, 108};
    std::vector<int32_t> bid_size_data = {10, 20, 5, 13, 23};
    std::vector<double> ask_data = {2.5, 3.4, 105, 110, 111};
    std::vector<int32_t> ask_size_data = {83, 33, 47, 15, 5};
    TableMaker table_maker;
    table_maker.AddColumn("Ticker", ticker_data);
    table_maker.AddColumn("Timestamp", timestamp_data);
    table_maker.AddColumn("Bid", bid_data);
    table_maker.AddColumn("BidSize", bid_size_data);
    table_maker.AddColumn("Ask", ask_data);
    table_maker.AddColumn("AskSize", ask_size_data);
    quotes = table_maker.MakeTable(tm.Client().GetManager());
    // std::cout << quotes.Stream(true) << '\n';
  }

  auto result = trades.Aj(quotes, {"Ticker", "Timestamp"});
  // std::cout << result.Stream(true) << '\n';

  // Expected data
  {
    std::vector<std::string> ticker_data = {"AAPL", "AAPL", "AAPL", "IBM", "IBM"};
    std::vector<DateTime> timestamp_data = {
        DateTime::Parse("2021-04-05T09:10:00-0500"),
        DateTime::Parse("2021-04-05T09:31:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500")
    };
    for (const auto &ts : timestamp_data) {
      fmt::print(std::cout, "{} - {}\n", ts, ts.Nanos());

    }
    std::vector<double> price_data = {2.5, 3.7, 3.0, 100.50, 110};
    std::vector<int32_t> size_data = {52, 14, 73, 11, 6};
    std::vector<std::optional<double>> bid_data = {{}, 3.4, 3.4, 97, 102};
    std::vector<std::optional<int32_t>> bid_size_data = {{}, 20, 20, 5, 13};
    std::vector<std::optional<double>> ask_data = {{}, 3.4, 3.4, 105, 110};
    std::vector<std::optional<int32_t>> ask_size_data = {{}, 33, 33, 47, 15};
    TableMaker table_maker;
    table_maker.AddColumn("Ticker", ticker_data);
    table_maker.AddColumn("Timestamp", timestamp_data);
    table_maker.AddColumn("Price", price_data);
    table_maker.AddColumn("Size", size_data);
    table_maker.AddColumn("Bid", bid_data);
    table_maker.AddColumn("BidSize", bid_size_data);
    table_maker.AddColumn("Ask", ask_data);
    table_maker.AddColumn("AskSize", ask_size_data);

    CompareTable(
        result,
        "Ticker", ticker_data,
        "Timestamp", timestamp_data,
        "Price", price_data,
        "Size", size_data,
        "Bid", bid_data,
        "BidSize", bid_size_data,
        "Ask", ask_data,
        "AskSize", ask_size_data);
  }
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
    table_maker.AddColumn("Ticker", ticker_data);
    table_maker.AddColumn("Timestamp", instant_data);
    table_maker.AddColumn("Price", price_data);
    table_maker.AddColumn("Size", size_data);
    trades = table_maker.MakeTable(tm.Client().GetManager());
    // std::cout << trades.Stream(true) << '\n';
  }

  TableHandle quotes;
  {
    std::vector<std::string> ticker_data = {"AAPL", "AAPL", "IBM", "IBM", "IBM"};
    std::vector<DateTime> timestamp_data = {
        DateTime::Parse("2021-04-05T09:11:00-0500"),
        DateTime::Parse("2021-04-05T09:30:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500"),
        DateTime::Parse("2021-04-05T17:00:00-0500")
    };
    std::vector<double> bid_data = {2.5, 3.4, 97, 102, 108};
    std::vector<int32_t> bid_size_data = {10, 20, 5, 13, 23};
    std::vector<double> ask_data = {2.5, 3.4, 105, 110, 111};
    std::vector<int32_t> ask_size_data = {83, 33, 47, 15, 5};
    TableMaker table_maker;
    table_maker.AddColumn("Ticker", ticker_data);
    table_maker.AddColumn("Timestamp", timestamp_data);
    table_maker.AddColumn("Bid", bid_data);
    table_maker.AddColumn("BidSize", bid_size_data);
    table_maker.AddColumn("Ask", ask_data);
    table_maker.AddColumn("AskSize", ask_size_data);
    quotes = table_maker.MakeTable(tm.Client().GetManager());
    // std::cout << quotes.Stream(true) << '\n';
  }

  auto result = trades.Raj(quotes, {"Ticker", "Timestamp"});
  // std::cout << result.Stream(true) << '\n';

  // Expected data
  {
    std::vector<std::string> ticker_data = {"AAPL", "AAPL", "AAPL", "IBM", "IBM"};
    std::vector<DateTime> timestamp_data = {
        DateTime::Parse("2021-04-05T09:10:00-0500"),
        DateTime::Parse("2021-04-05T09:31:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:00:00-0500"),
        DateTime::Parse("2021-04-05T16:30:00-0500")
    };
    for (const auto &ts : timestamp_data) {
      fmt::print(std::cout, "{} - {}\n", ts, ts.Nanos());

    }
    std::vector<double> price_data = {2.5, 3.7, 3.0, 100.50, 110};
    std::vector<int32_t> size_data = {52, 14, 73, 11, 6};
    std::vector<std::optional<double>> bid_data = {2.5, {}, {}, 97, 102};
    std::vector<std::optional<int32_t>> bid_size_data = {10, {}, {}, 5, 13};
    std::vector<std::optional<double>> ask_data = {2.5, {}, {}, 105, 110};
    std::vector<std::optional<int32_t>> ask_size_data = {83, {}, {}, 47, 15};

    CompareTable(
        result,
        "Ticker", ticker_data,
        "Timestamp", timestamp_data,
        "Price", price_data,
        "Size", size_data,
        "Bid", bid_data,
        "BidSize", bid_size_data,
        "Ask", ask_data,
        "AskSize", ask_size_data);
  }
}
}  // namespace deephaven::client::tests
