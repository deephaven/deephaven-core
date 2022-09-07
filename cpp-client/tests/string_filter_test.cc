/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"

using deephaven::client::TableHandle;
namespace deephaven::client::tests {
namespace {
void testFilter(const char *description, const TableHandle &filteredTable,
    const std::vector<std::string> &tickerData,
    const std::vector<double> &closeData);
}  // namespace

TEST_CASE("String Filter", "[strfilter]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto close = table.getNumCol("Close");

  auto t2 = table.where(importDate == "2017-11-01").select(ticker, close);

  {
    std::vector<std::string> tickerData = {"AAPL", "AAPL", "AAPL", "ZNGA", "ZNGA"};
    std::vector<double> closeData = {23.5, 24.2, 26.7, 538.2, 544.9};
    testFilter("Contains A", t2.where(ticker.contains("A")),
      tickerData, closeData);
  }

  {
    std::vector<std::string> tickerData = {};
    std::vector<double> closeData = {};
    testFilter("Starts with BL", t2.where(ticker.startsWith("BL")),
      tickerData, closeData);
  }

  {
    std::vector<std::string> tickerData = {"XRX", "XRX"};
    std::vector<double> closeData = {88.2, 53.8};
    testFilter("Ends with X", t2.where(ticker.endsWith("X")),
        tickerData, closeData);
  }

  {
    std::vector<std::string> tickerData = {"IBM"};
    std::vector<double> closeData = {38.7};
    testFilter("Matches ^I.*M$", t2.where(ticker.matches("^I.*M$")),
        tickerData, closeData);
  }
}

namespace {
void testFilter(const char *description, const TableHandle &filteredTable,
    const std::vector<std::string> &tickerData,
    const std::vector<double> &closeData) {
  INFO(description);
  INFO(filteredTable.stream(true));
  compareTable(
      filteredTable,
      "Ticker", tickerData,
      "Close", closeData
  );
}
}  // namespace
}  // namespace deephaven::client::tests {
