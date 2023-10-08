/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include "tests/third_party/catch.hpp"
#include "tests/test_util.h"

#include "deephaven/dhcore/utility/utility.h"

using deephaven::client::TableHandleManager;
using deephaven::client::TableHandle;
using deephaven::client::SortPair;
using deephaven::client::utility::TableMaker;

namespace deephaven::client::tests {
TEST_CASE("Sort demo Table", "[sort]") {
  auto tm = TableMakerForTests::Create();
  auto table = tm.Table();

  auto import_date = table.GetStrCol("ImportDate");
  auto ticker = table.GetStrCol("Ticker");
  auto open = table.GetNumCol("Open");
  auto volume = table.GetNumCol("Volume");

  // Limit by date and ticker
  auto filtered = table.Where(import_date == "2017-11-01")
      .Where(ticker >= "X")
      .Select(ticker, open, volume);
  std::cout << filtered.Stream(true) << '\n';

  auto table1 = filtered.Sort({SortPair::Descending("Ticker"), SortPair::Ascending("Volume")});
  std::cout << table1.Stream(true) << '\n';

  auto table2 = filtered.Sort({SortPair::Descending(ticker), SortPair::Ascending(volume)});
  std::cout << table2.Stream(true) << '\n';

  // with the sort direction convenience methods on the C# column var
  auto table3 = filtered.Sort({ticker.Descending(), volume.Ascending()});
  std::cout << table3.Stream(true) << '\n';

  std::vector<std::string> ticker_data = {"ZNGA", "ZNGA", "XYZZY", "XRX", "XRX"};
  std::vector<double> open_data = {541.2, 685.3, 92.3, 50.5, 83.1};
  std::vector<int64_t> vol_data = {46123, 48300, 6060842, 87000, 345000};

  const TableHandle *tables[] = {&table1, &table2, &table3};
  for (const auto *t : tables) {
    CompareTable(
        *t,
        "Ticker", ticker_data,
        "Open", open_data,
        "Volume", vol_data
    );
  }
}

TEST_CASE("Sort temp Table", "[sort]") {
  auto tm = TableMakerForTests::Create();

  std::vector<int32_t> int_data0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  std::vector<int32_t> int_data1{0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7};
  std::vector<int32_t> int_data2{0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
  std::vector<int32_t> int_data3{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1};

  TableMaker maker;
  maker.AddColumn("IntValue0", int_data0);
  maker.AddColumn("IntValue1", int_data1);
  maker.AddColumn("IntValue2", int_data2);
  maker.AddColumn("IntValue3", int_data3);

  std::string table_name("sortData");
  auto temp_table = maker.MakeTable(tm.Client().GetManager());

  auto iv0 = temp_table.GetNumCol("IntValue0");
  auto iv1 = temp_table.GetNumCol("IntValue1");
  auto iv2 = temp_table.GetNumCol("IntValue2");
  auto iv3 = temp_table.GetNumCol("IntValue3");
  auto sorted = temp_table.Sort({iv3.Descending(), iv2.Ascending()});
  std::cout << sorted.Stream(true) << '\n';

  std::vector<std::string> import_date_data = {"2017-11-01", "2017-11-01", "2017-11-01"};
  std::vector<std::string> ticker_data = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> open_data = {22.1, 26.8, 31.5};
  std::vector<double> close_data = {23.5, 24.2, 26.7};
  std::vector<int64_t> vol_data = {100000, 250000, 19000};

  std::vector<int32_t> sid0{8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7};
  std::vector<int32_t> sid1{4, 4, 5, 5, 6, 6, 7, 7, 0, 0, 1, 1, 2, 2, 3, 3};
  std::vector<int32_t> sid2{2, 2, 2, 2, 3, 3, 3, 3, 0, 0, 0, 0, 1, 1, 1, 1};
  std::vector<int32_t> sid3{1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0};

  CompareTable(
      sorted,
      "IntValue0", sid0,
      "IntValue1", sid1,
      "IntValue2", sid2,
      "IntValue3", sid3
  );
}
}  // namespace deephaven::client::tests
