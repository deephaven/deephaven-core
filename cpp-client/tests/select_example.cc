/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include <arrow/flight/client.h>
#include <arrow/flight/client_auth.h>
#include "tests/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/highlevel/client.h"
#include "deephaven/client/utility/utility.h"

#include <iostream>
#include <arrow/flight/client.h>
#include <arrow/flight/types.h>
#include <arrow/array.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compare.h>
#include <arrow/record_batch.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

using deephaven::client::highlevel::Client;
using deephaven::client::highlevel::NumCol;
using deephaven::client::highlevel::StrCol;
using deephaven::client::highlevel::TableHandle;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;
using deephaven::client::utility::TableMaker;

namespace deephaven {
namespace client {
namespace tests {

TEST_CASE("Create / update / fetch a table", "[select]") {
  auto tm = TableMakerForTests::create();

  std::vector<int32_t> intData = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<double> doubleData = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9};
  std::vector<std::string> stringData = {"zero", "one", "two", "three", "four", "five", "six", "seven",
      "eight", "nine"};
  TableMaker maker;
  maker.addColumn("IntValue", intData);
  maker.addColumn("DoubleValue", doubleData);
  maker.addColumn("StringValue", stringData);
  auto t = maker.makeTable(tm.client().getManager());
  auto t2 = t.update("Q2 = IntValue * 100");
  std::cout << t2.stream(true) << '\n';
  auto t3 = t2.update("Q3 = Q2 + 10");
  std::cout << t3.stream(true) << '\n';
  auto q2 = t3.getNumCol("Q2");
  auto t4 = t3.update((q2 + 100).as("Q4"));
  std::cout << t4.stream(true) << '\n';

  std::vector<int32_t> q2Data = {0, 100, 200, 300, 400, 500, 600, 700, 800, 900};
  std::vector<int32_t> q3Data = {10, 110, 210, 310, 410, 510, 610, 710, 810, 910};
  std::vector<int32_t> q4Data = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};

  compareTable(
      t4,
      "IntValue", intData,
      "DoubleValue", doubleData,
      "StringValue", stringData,
      "Q2", q2Data,
      "Q3", q3Data,
      "Q4", q4Data
      );
}

TEST_CASE("Simple where", "[select]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();
  auto updated = table.update("QQQ = i");
  // Symbolically
  auto importDate = updated.getStrCol("ImportDate");
  auto ticker = updated.getStrCol("Ticker");
  auto volume = updated.getNumCol("Volume");
  // if we allowed C++17 we could do something like
  // auto [importDate, ticker, volume] = table.getCol<StrCol, StrCol, NumCol>("ImportDate", "Ticker", "Volume");

  auto t2 = updated.where(importDate == "2017-11-01" && ticker == "IBM")
      .select(ticker, volume);
  std::cout << t2.stream(true) << '\n';

  std::vector<std::string> tickerData = {"IBM"};
  std::vector<int64_t> volData = {138000};

  compareTable(
      t2,
      "Ticker", tickerData,
      "Volume", volData
      );
}

TEST_CASE("Select a few columns", "[select]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto t1 = table.where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .select("Ticker", "Close", "Volume")
      .head(2);
  std::cout << t1.stream(true) << '\n';

  // Symbolically
  auto [importDate, ticker, close, volume] =
      table.getCols<StrCol, StrCol, NumCol, NumCol>("ImportDate", "Ticker", "Close", "Volume");
  auto t2 = table.where(importDate == "2017-11-01" && ticker == "AAPL")
      .select(ticker, close, volume)
      .head(2);
  std::cout << t2.stream(true) << '\n';

  std::vector<std::string> tickerData = {"AAPL", "AAPL"};
  std::vector<double> closeData = {23.5, 24.2};
  std::vector<int64_t> volData = {100000, 250000};

  compareTable(
      t2,
      "Ticker", tickerData,
      "Close", closeData,
      "Volume", volData
      );
}

TEST_CASE("Simple 'where' with syntax error", "[select]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  try {
    // String literal
    auto t1 = table.where(")))))");
    std::cout << t1.stream(true) << '\n';
  } catch (const std::exception &e) {
    // Expected
    streamf(std::cerr, "Caught *expected* exception %o\n", e.what());
    return;
  }
  throw std::runtime_error("Expected a failure, but didn't experience one");
}

TEST_CASE("LastBy + Select", "[select]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto t1 = table.where("ImportDate == `2017-11-01` && Ticker == `AAPL`").lastBy("Ticker")
      .select("Ticker", "Close", "Volume");
  std::cout << t1.stream(true) << '\n';

  // Symbolically
  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto close = table.getNumCol("Close");
  auto volume = table.getNumCol("Volume");
  auto t2 = table.where(importDate == "2017-11-01" && ticker == "AAPL").lastBy(ticker)
      .select(ticker, close, volume);
  std::cout << t2.stream(true) << '\n';

  std::vector<std::string> tickerData = {"AAPL"};
  std::vector<double> closeData = {26.7};
  std::vector<int64_t> volData = {19000};

  compareTable(
      t2,
      "Ticker", tickerData,
      "Close", closeData,
      "Volume", volData
      );
}

TEST_CASE("Formula Formula in the where clause", "[select]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto t1 = table.where("ImportDate == `2017-11-01` && Ticker == `AAPL` && Volume % 10 == Volume % 100")
      .select("Ticker", "Volume");
  std::cout << t1.stream(true) << '\n';

  // Symbolically
  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto volume = table.getNumCol("Volume");
  auto t2 = table.where(importDate == "2017-11-01" && ticker == "AAPL" && volume % 10 == volume % 100)
      .select(ticker, volume);
  std::cout << t2.stream(true) << '\n';

  std::vector<std::string> tickerData = {"AAPL", "AAPL", "AAPL"};
  std::vector<int64_t> volData = {100000, 250000, 19000};

  const TableHandle *tables[] = {&t1, &t2};
  for (const auto *t : tables) {
    compareTable(
        *t,
        "Ticker", tickerData,
        "Volume", volData
        );
  }
}

TEST_CASE("New columns", "[select]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  // A formula expression
  auto t1 = table.where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .select("MV1 = Volume * Close", "V_plus_12 = Volume + 12");
  std::cout << t1.stream(true) << '\n';

  // Symbolically
  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto close = table.getNumCol("Close");
  auto volume = table.getNumCol("Volume");

  auto t2 = table.where(importDate == "2017-11-01" && ticker == "AAPL")
      .select((volume * close).as("MV1"), (volume + 12).as("V_plus_12"));
  std::cout << t2.stream(true) << '\n';

  std::vector<double> mv1Data = {2350000, 6050000, 507300};
  std::vector<int64_t> mv2Data = {100012, 250012, 19012};

  const TableHandle *tables[] = {&t1, &t2};
  for (const auto *t : tables) {
    compareTable(
        *t,
        "MV1", mv1Data,
        "V_plus_12", mv2Data
        );
  }
}
}  // namespace tests
}  // namespace client
}  // namespace deephaven
