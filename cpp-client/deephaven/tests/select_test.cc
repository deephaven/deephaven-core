/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"
#include "deephaven/client/client.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::Client;
using deephaven::client::NumCol;
using deephaven::client::StrCol;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::DeephavenConstants;
using deephaven::dhcore::utility::Streamf;
using deephaven::dhcore::utility::Stringf;

namespace deephaven::client::tests {
TEST_CASE("Support all types", "[select]") {
  auto tm = TableMakerForTests::Create();

  std::vector<bool> bool_data;
  std::vector<char16_t> char_data;
  std::vector<int8_t> byte_data;
  std::vector<int16_t> short_data;
  std::vector<int32_t> int_data;
  std::vector<int64_t> long_data;
  std::vector<float> float_data;
  std::vector<double> doubleData;
  std::vector<std::string> stringData;

  const int startValue = -8;
  const int endValue = 8;
  for (auto i = startValue; i != endValue; ++i) {
    bool_data.push_back((i % 2) == 0);
    char_data.push_back(i * 10);
    byte_data.push_back(i * 11);
    short_data.push_back(i * 1000);
    int_data.push_back(i * 1'000'000);
    long_data.push_back(static_cast<long>(i) * 1'000'000'000);
    float_data.push_back(i * 123.456F);
    doubleData.push_back(i * 987654.321);
    stringData.push_back(Stringf("test %o", i));
  }

  TableMaker maker;
  maker.AddColumn("boolData", bool_data);
  maker.AddColumn("charData", char_data);
  maker.AddColumn("byteData", byte_data);
  maker.AddColumn("shortData", short_data);
  maker.AddColumn("intData", int_data);
  maker.AddColumn("longData", long_data);
  maker.AddColumn("floatData", float_data);
  maker.AddColumn("doubleData", doubleData);
  maker.AddColumn("stringData", stringData);

  auto t = maker.MakeTable(tm.Client().GetManager());

  std::cout << t.Stream(true) << '\n';

  CompareTable(
      t,
      "boolData", bool_data,
      "charData", char_data,
      "byteData", byte_data,
      "shortData", short_data,
      "intData", int_data,
      "longData", long_data,
      "floatData", float_data,
      "doubleData", doubleData,
      "stringData", stringData
  );
}

TEST_CASE("Create / Update / fetch a Table", "[select]") {
  auto tm = TableMakerForTests::Create();

  std::vector<int32_t> int_data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  std::vector<double> double_data = {0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9};
  std::vector<std::string> string_data = {"zero", "one", "two", "three", "four", "five", "six", "seven",
      "eight", "nine"};
  TableMaker maker;
  maker.AddColumn("IntValue", int_data);
  maker.AddColumn("DoubleValue", double_data);
  maker.AddColumn("StringValue", string_data);
  auto t = maker.MakeTable(tm.Client().GetManager());
  auto t2 = t.Update("Q2 = IntValue * 100");
  std::cout << t2.Stream(true) << '\n';
  auto t3 = t2.Update("Q3 = Q2 + 10");
  std::cout << t3.Stream(true) << '\n';
  auto q2 = t3.GetNumCol("Q2");
  auto t4 = t3.Update((q2 + 100).as("Q4"));
  std::cout << t4.Stream(true) << '\n';

  std::vector<int32_t> q2_data = {0, 100, 200, 300, 400, 500, 600, 700, 800, 900};
  std::vector<int32_t> q3_data = {10, 110, 210, 310, 410, 510, 610, 710, 810, 910};
  std::vector<int32_t> q4_data = {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000};

  CompareTable(
      t4,
      "IntValue", int_data,
      "DoubleValue", double_data,
      "StringValue", string_data,
      "Q2", q2_data,
      "Q3", q3_data,
      "Q4", q4_data
  );
}


TEST_CASE("Select a few columns", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .Select("Ticker", "Close", "Volume")
      .Head(2);
  std::cout << t1.Stream(true) << '\n';

  // Symbolically
  auto [importDate, ticker, close, volume] =
      table.GetCols<StrCol, StrCol, NumCol, NumCol>("ImportDate", "Ticker", "Close", "Volume");
  auto t2 = table.Where(importDate == "2017-11-01" && ticker == "AAPL")
      .Select(ticker, close, volume)
      .Head(2);
  std::cout << t2.Stream(true) << '\n';

  std::vector<std::string> ticker_data = {"AAPL", "AAPL"};
  std::vector<double> close_data = {23.5, 24.2};
  std::vector<int64_t> vol_data = {100000, 250000};

  CompareTable(
      t2,
      "Ticker", ticker_data,
      "Close", close_data,
      "Volume", vol_data
  );
}

TEST_CASE("LastBy + Select", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`").LastBy("Ticker")
      .Select("Ticker", "Close", "Volume");
  std::cout << t1.Stream(true) << '\n';

  // Symbolically
  auto importDate = table.GetStrCol("ImportDate");
  auto ticker = table.GetStrCol("Ticker");
  auto close = table.GetNumCol("Close");
  auto volume = table.GetNumCol("Volume");
  auto t2 = table.Where(importDate == "2017-11-01" && ticker == "AAPL").LastBy(ticker)
      .Select(ticker, close, volume);
  std::cout << t2.Stream(true) << '\n';

  std::vector<std::string> tickerData = {"AAPL"};
  std::vector<double> closeData = {26.7};
  std::vector<int64_t> volData = {19000};

  CompareTable(
      t2,
      "Ticker", tickerData,
      "Close", closeData,
      "Volume", volData
  );
}

TEST_CASE("New columns", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  // A formula expression
  auto t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .Select("MV1 = Volume * Close", "V_plus_12 = Volume + 12");
  std::cout << t1.Stream(true) << '\n';

  // Symbolically
  auto importDate = table.GetStrCol("ImportDate");
  auto ticker = table.GetStrCol("Ticker");
  auto close = table.GetNumCol("Close");
  auto volume = table.GetNumCol("Volume");

  auto t2 = table.Where(importDate == "2017-11-01" && ticker == "AAPL")
      .Select((volume * close).as("MV1"), (volume + 12).as("V_plus_12"));
  std::cout << t2.Stream(true) << '\n';

  std::vector<double> mv1Data = {2350000, 6050000, 507300};
  std::vector<int64_t> mv2Data = {100012, 250012, 19012};

  const TableHandle *tables[] = {&t1, &t2};
  for (const auto *t : tables) {
    CompareTable(
        *t,
        "MV1", mv1Data,
        "V_plus_12", mv2Data
    );
  }
}

TEST_CASE("Simple Where", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  auto updated = table.Update("QQQ = i");
  // Symbolically
  auto importDate = updated.GetStrCol("ImportDate");
  auto ticker = updated.GetStrCol("Ticker");
  auto volume = updated.GetNumCol("Volume");
  // if we allowed C++17 we could do something like
  // auto [importDate, ticker, volume] = table.getCol<StrCol, StrCol, NumCol>("ImportDate", "Ticker", "Volume");

  auto t2 = updated.Where(importDate == "2017-11-01" && ticker == "IBM")
      .Select(ticker, volume);
  std::cout << t2.Stream(true) << '\n';

  std::vector<std::string> tickerData = {"IBM"};
  std::vector<int64_t> volData = {138000};

  CompareTable(
      t2,
      "Ticker", tickerData,
      "Volume", volData
  );
}

TEST_CASE("Formula in the Where clause", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.Where(
          "ImportDate == `2017-11-01` && Ticker == `AAPL` && Volume % 10 == Volume % 100")
      .Select("Ticker", "Volume");
  std::cout << t1.Stream(true) << '\n';

  // Symbolically
  auto importDate = table.GetStrCol("ImportDate");
  auto ticker = table.GetStrCol("Ticker");
  auto volume = table.GetNumCol("Volume");
  auto t2 = table.Where(
          importDate == "2017-11-01" && ticker == "AAPL" && volume % 10 == volume % 100)
      .Select(ticker, volume);
  std::cout << t2.Stream(true) << '\n';

  std::vector<std::string> tickerData = {"AAPL", "AAPL", "AAPL"};
  std::vector<int64_t> volData = {100000, 250000, 19000};

  const TableHandle *tables[] = {&t1, &t2};
  for (const auto *t : tables) {
    CompareTable(
        *t,
        "Ticker", tickerData,
        "Volume", volData
    );
  }
}

TEST_CASE("Simple 'Where' with syntax error", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  try {
    // String literal
    auto t1 = table.Where(")))))");
    std::cout << t1.Stream(true) << '\n';
  } catch (const std::exception &e) {
    // Expected
    Streamf(std::cerr, "Caught *expected* exception %o\n", e.what());
    return;
  }
  throw std::runtime_error("Expected a failure, but didn't experience one");
}

TEST_CASE("WhereIn", "[select]") {
  auto tm = TableMakerForTests::Create();

  std::vector<std::string> letter_data = {"A", "C", "F", "B", "E", "D", "A"};
  std::vector<std::optional<int32_t>> number_data = { {}, 2, 1, {}, 4, 5, 3};
  std::vector<std::string> color_data = {"red", "blue", "orange", "purple", "yellow", "pink", "blue"};
  std::vector<std::optional<int32_t>> code_data = { 12, 13, 11, {}, 16, 14, {}};
  TableMaker source_maker;
  source_maker.AddColumn("Letter", letter_data);
  source_maker.AddColumn("Number", number_data);
  source_maker.AddColumn("Color", color_data);
  source_maker.AddColumn("Code", code_data);
  auto source = source_maker.MakeTable(tm.Client().GetManager());

  std::vector<std::string> filter_color_data = {"blue", "red", "purple", "white"};
  TableMaker filter_maker;
  filter_maker.AddColumn("Colors", filter_color_data);
  auto filter = filter_maker.MakeTable(tm.Client().GetManager());

  auto result = source.WhereIn(filter, {"Color = Colors"});

  std::vector<std::string> letter_expected = {"A", "C", "B", "A"};
  std::vector<std::optional<int32_t>> number_expected = { {}, 2, {}, 3};
  std::vector<std::string> color_expected = {"red", "blue", "purple", "blue"};
  std::vector<std::optional<int32_t>> code_expected = { 12, 13, {}, {}};

  CompareTable(result,
      "Letter", letter_expected,
      "Number", number_expected,
      "Color", color_expected,
      "Code", code_expected);
}

TEST_CASE("LazyUpdate", "[select]") {
  auto tm = TableMakerForTests::Create();

  std::vector<std::string> a_data = {"The", "At", "Is", "On"};
  std::vector<int32_t> b_data = {1, 2, 3, 4};
  std::vector<int32_t> c_data = {5, 2, 5, 5};
  TableMaker source_maker;
  source_maker.AddColumn("A", a_data);
  source_maker.AddColumn("B", b_data);
  source_maker.AddColumn("C", c_data);
  auto source = source_maker.MakeTable(tm.Client().GetManager());

  auto result = source.LazyUpdate({"Y = sqrt(C)"});

  std::vector<double> sqrt_data = {std::sqrt(5), std::sqrt(2), std::sqrt(5), std::sqrt(5)};

  CompareTable(result,
      "A", a_data,
      "B", b_data,
      "C", c_data,
      "Y", sqrt_data);
}

TEST_CASE("SelectDistinct", "[select]") {
  auto tm = TableMakerForTests::Create();

  std::vector<std::string> a_data = {"apple", "apple", "orange", "orange", "plum", "grape"};
  std::vector<int32_t> b_data = {1, 1, 2, 2, 3, 3};
  TableMaker source_maker;
  source_maker.AddColumn("A", a_data);
  source_maker.AddColumn("B", b_data);
  auto source = source_maker.MakeTable(tm.Client().GetManager());

  auto result = source.SelectDistinct({"A"});

  std::cout << result.Stream(true) << '\n';

  std::vector<std::string> expected_data = {"apple", "orange", "plum", "grape"};

  CompareTable(result,
      "A", expected_data);
}
}  // namespace deephaven::client::tests
