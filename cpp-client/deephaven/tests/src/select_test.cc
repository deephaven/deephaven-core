/*
 * Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/third_party/catch.hpp"
#include "deephaven/tests/test_util.h"
#include "deephaven/dhcore/types.h"
#include "deephaven/dhcore/utility/utility.h"
#include "deephaven/third_party/fmt/format.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::utility::TableMaker;
using deephaven::dhcore::DateTime;
using deephaven::dhcore::LocalDate;
using deephaven::dhcore::LocalTime;
using deephaven::dhcore::DeephavenConstants;

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
  std::vector<double> double_data;
  std::vector<std::string> string_data;
  std::vector<DateTime> date_time_data;
  std::vector<LocalDate> local_date_data;
  std::vector<LocalTime> local_time_data;

  const int start_value = -8;
  const int end_value = 8;
  for (auto i = start_value; i != end_value; ++i) {
    bool_data.push_back((i % 2) == 0);
    char_data.push_back(i * 10);
    byte_data.push_back(i * 11);
    short_data.push_back(i * 1000);
    int_data.push_back(i * 1'000'000);
    long_data.push_back(static_cast<int64_t>(i) * 1'000'000'000);
    float_data.push_back(i * 123.456F);
    double_data.push_back(i * 987654.321);
    string_data.push_back(fmt::format("test {}", i));
    date_time_data.push_back(DateTime::FromNanos(i));
    local_date_data.push_back(LocalDate::FromMillis(i * 86400 * 1000));
    local_time_data.push_back(LocalTime::FromNanos(1000 + i));  // nanos argument cannot be negative
  }

  TableMaker maker;
  maker.AddColumn("boolData", bool_data);
  maker.AddColumn("charData", char_data);
  maker.AddColumn("byteData", byte_data);
  maker.AddColumn("shortData", short_data);
  maker.AddColumn("intData", int_data);
  maker.AddColumn("longData", long_data);
  maker.AddColumn("floatData", float_data);
  maker.AddColumn("doubleData", double_data);
  maker.AddColumn("stringData", string_data);
  maker.AddColumn("dateTimeData", date_time_data);
  maker.AddColumn("localDateData", local_date_data);
  maker.AddColumn("localTimeData", local_time_data);

  auto t = maker.MakeTable(tm.Client().GetManager());

  std::cout << t.Stream(true) << '\n';

  TableComparerForTests::Compare(maker, t);
}

TEST_CASE("Create / Update / fetch a Table", "[select]") {
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<int32_t>("IntValue", {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
  maker.AddColumn<double>("DoubleValue", {0.0, 1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8, 9.9});
  maker.AddColumn<std::string>("StringValue", {"zero", "one", "two", "three", "four", "five", "six", "seven",
      "eight", "nine"});
  auto t = maker.MakeTable(tm.Client().GetManager());
  auto t2 = t.Update("Q2 = IntValue * 100");
  auto t3 = t2.Update("Q3 = Q2 + 10");
  auto t4 = t3.Update("Q4 = Q2 + 100");

  // Reuse the already-populated maker; keep the existing columns and add a few more.
  maker.AddColumn<int32_t>("Q2", {0, 100, 200, 300, 400, 500, 600, 700, 800, 900});
  maker.AddColumn<int32_t>("Q3", {10, 110, 210, 310, 410, 510, 610, 710, 810, 910});
  maker.AddColumn<int32_t>("Q4", {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000});
  TableComparerForTests::Compare(maker, t4);
}


TEST_CASE("Select a few columns", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .Select("Ticker", "Close", "Volume")
      .Head(2);

  TableMaker expected;
  expected.AddColumn<std::string>("Ticker", {"AAPL", "AAPL"});
  expected.AddColumn<double>("Close", {23.5, 24.2});
  expected.AddColumn<int64_t>("Volume", {100000, 250000});

  TableComparerForTests::Compare(expected, t1);
}

TEST_CASE("LastBy + Select", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .LastBy("Ticker")
      .Select("Ticker", "Close", "Volume");
  std::cout << t1.Stream(true) << '\n';

  TableMaker expected;
  expected.AddColumn<std::string>("Ticker", {"AAPL"});
  expected.AddColumn<double>("Close", {26.7});
  expected.AddColumn<int64_t>("Volume", {19000});
  TableComparerForTests::Compare(expected, t1);
}

TEST_CASE("New columns", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  // A formula expression
  auto t1 = table.Where("ImportDate == `2017-11-01` && Ticker == `AAPL`")
      .Select("MV1 = Volume * Close", "V_plus_12 = Volume + 12");

  TableMaker expected;
  expected.AddColumn<double>("MV1", {2350000, 6050000, 507300});
  expected.AddColumn<int64_t>("V_plus_12", {100012, 250012, 19012});
  TableComparerForTests::Compare(expected, t1);
}

TEST_CASE("Drop columns", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.DropColumns({"ImportDate", "Open", "Close"});
  CHECK(2 == t1.Schema()->NumCols());
}

TEST_CASE("Simple Where", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();
  auto updated = table.Update("QQQ = i");

  auto t1 = updated.Where("ImportDate == `2017-11-01` && Ticker == `IBM`")
      .Select("Ticker", "Volume");

  TableMaker expected;
  expected.AddColumn<std::string>("Ticker", {"IBM"});
  expected.AddColumn<int64_t>("Volume", {138000});
  TableComparerForTests::Compare(expected, t1);
}

TEST_CASE("Formula in the Where clause", "[select]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto t1 = table.Where(
          "ImportDate == `2017-11-01` && Ticker == `AAPL` && Volume % 10 == Volume % 100")
      .Select("Ticker", "Volume");
  std::cout << t1.Stream(true) << '\n';

  TableMaker expected;
  expected.AddColumn<std::string>("Ticker", {"AAPL", "AAPL", "AAPL"});
  expected.AddColumn<int64_t>("Volume", {100000, 250000, 19000});
  TableComparerForTests::Compare(expected, t1);
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
    fmt::print(std::cerr, "Caught *expected* exception {}\n", e.what());
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

  TableMaker expected;
  expected.AddColumn<std::string>("Letter", {"A", "C", "B", "A"});
  expected.AddColumn<std::optional<int32_t>>("Number", { {}, 2, {}, 3});
  expected.AddColumn<std::string>("Color", {"red", "blue", "purple", "blue"});
  expected.AddColumn<std::optional<int32_t>>("Code", { 12, 13, {}, {}});
  TableComparerForTests::Compare(expected, result);
}

TEST_CASE("LazyUpdate", "[select]") {
  auto tm = TableMakerForTests::Create();

  TableMaker maker;
  maker.AddColumn<std::string>("A", {"The", "At", "Is", "On"});
  maker.AddColumn<int32_t>("B", {1, 2, 3, 4});
  maker.AddColumn<int32_t>("C", {5, 2, 5, 5});
  auto source = maker.MakeTable(tm.Client().GetManager());

  auto result = source.LazyUpdate({"Y = sqrt(C)"});

  // Reuse maker and add one more column.
  maker.AddColumn<double>("Y", {std::sqrt(5), std::sqrt(2), std::sqrt(5), std::sqrt(5)});
  TableComparerForTests::Compare(maker, result);
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
  TableMaker expected;
  expected.AddColumn<std::string>("A", {"apple", "orange", "plum", "grape"});
  TableComparerForTests::Compare(expected, result);
}
}  // namespace deephaven::client::tests
