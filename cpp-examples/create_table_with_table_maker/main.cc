/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/highlevel/client.h"
#include "deephaven/client/utility/table_maker.h"

using deephaven::client::highlevel::NumCol;
using deephaven::client::highlevel::Client;
using deephaven::client::highlevel::TableHandle;
using deephaven::client::highlevel::TableHandleManager;
using deephaven::client::utility::TableMaker;

// This example shows how to use the TableMaker wrapper to make a simple table.
void doit(const TableHandleManager &manager) {
  TableMaker tm;
  std::vector<std::string> symbols{"FB", "AAPL", "NFLX", "GOOG"};
  std::vector<double> prices{101.1, 102.2, 103.3, 104.4};
  tm.addColumn("Symbol", symbols);
  tm.addColumn("Price", prices);
  auto table = tm.makeTable(manager);

  std::cout << "table is:\n" << table.stream(true) << std::endl;
}

int main() {
  const char *server = "localhost:10000";
  try {
    auto client = Client::connect(server);
    auto manager = client.getManager();
    doit(manager);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}
