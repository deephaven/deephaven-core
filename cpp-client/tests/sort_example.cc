/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include "tests/catch.hpp"
#include "tests/test_util.h"

#include "deephaven/client/utility/utility.h"

using deephaven::client::highlevel::TableHandleManager;
using deephaven::client::highlevel::TableHandle;
using deephaven::client::highlevel::SortPair;
using deephaven::client::utility::streamf;
using deephaven::client::utility::stringf;

namespace deephaven {
namespace client {
namespace tests {
TEST_CASE("Sort demo table", "[sort]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  auto importDate = table.getStrCol("ImportDate");
  auto ticker = table.getStrCol("Ticker");
  auto open = table.getNumCol("Open");
  auto volume = table.getNumCol("Volume");

  // Limit by date and ticker
  auto filtered = table.where(importDate == "2017-11-01")
      .where(ticker >= "X")
      .select(ticker, open, volume);
  std::cout << filtered.stream(true) << '\n';

  auto table1 = filtered.sort({SortPair::descending("Ticker"), SortPair::ascending("Volume")});
  std::cout << table1.stream(true) << '\n';

  auto table2 = filtered.sort({SortPair::descending(ticker), SortPair::ascending(volume)});
  std::cout << table2.stream(true) << '\n';

  // with the sort direction convenience methods on the C# column var
  auto table3 = filtered.sort({ticker.descending(), volume.ascending()});
  std::cout << table3.stream(true) << '\n';

  std::vector<std::string> tickerData = {"ZNGA", "ZNGA", "XYZZY", "XRX", "XRX"};
  std::vector<double> openData = {541.2, 685.3, 92.3, 50.5, 83.1};
  std::vector<int64_t> volData = {46123, 48300, 6060842, 87000, 345000};

  const TableHandle *tables[] = {&table1, &table2, &table3};
  for (const auto *t : tables) {
    compareTable(
        *t,
        "Ticker", tickerData,
        "Open", openData,
        "Volume", volData
        );
  }
}

TEST_CASE("Sort temp table", "[sort]") {
  auto tm = TableMakerForTests::create();
  auto table = tm.table();

  std::vector<int32_t> intData0{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  std::vector<int32_t> intData1{0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7};
  std::vector<int32_t> intData2{0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
  std::vector<int32_t> intData3{0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1};

  TableWizard w;
  w.addColumn("IntValue0", intData0);
  w.addColumn("IntValue1", intData1);
  w.addColumn("IntValue2", intData2);
  w.addColumn("IntValue3", intData3);

  std::string tableName("sortData");
  auto tempTable = w.makeTable(tm.client().getManager(), tableName);

  auto iv0 = tempTable.getNumCol("IntValue0");
  auto iv1 = tempTable.getNumCol("IntValue1");
  auto iv2 = tempTable.getNumCol("IntValue2");
  auto iv3 = tempTable.getNumCol("IntValue3");
  auto sorted = tempTable.sort({iv3.descending(), iv2.ascending()});
  std::cout << sorted.stream(true) << '\n';

  std::vector<std::string> importDateData = {"2017-11-01", "2017-11-01", "2017-11-01"};
  std::vector<std::string> tickerData = {"AAPL", "AAPL", "AAPL"};
  std::vector<double> openData = {22.1, 26.8, 31.5};
  std::vector<double> closeData = {23.5, 24.2, 26.7};
  std::vector<int64_t> volData = {100000, 250000, 19000};

  std::vector<int32_t> sid0{8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7};
  std::vector<int32_t> sid1{4, 4, 5, 5, 6, 6, 7, 7, 0, 0, 1, 1, 2, 2, 3, 3};
  std::vector<int32_t> sid2{2, 2, 2, 2, 3, 3, 3, 3, 0, 0, 0, 0, 1, 1, 1, 1};
  std::vector<int32_t> sid3{1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0};

  compareTable(
      sorted,
      "IntValue0", sid0,
      "IntValue1", sid1,
      "IntValue2", sid2,
      "IntValue3", sid3
      );
}
}  // namespace tests
}  // namespace client
}  // namespace deephaven
