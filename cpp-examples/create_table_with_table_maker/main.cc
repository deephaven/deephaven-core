/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/client.h"
#include "deephaven/client/utility/table_maker.h"

using deephaven::client::NumCol;
using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;
using deephaven::client::utility::TableMaker;

namespace {
void doit(const TableHandleManager &manager);
}  // namespace

// This example shows how to use the TableMaker wrapper to make a simple table.
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

namespace {
void doit(const TableHandleManager &manager) {
  TableMaker tm;
  std::vector<std::string> symbols{"FB", "AAPL", "IBM"};
  std::vector<double> prices{111.111, 222.222, 333.333};
  tm.addColumn("Symbol", symbols);
  tm.addColumn("Price", prices);
  auto table = tm.makeTable(manager);

  std::cout << "table is:\n" << table.stream(true) << std::endl;
}
}  // namespace
