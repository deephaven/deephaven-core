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
void Doit(const TableHandleManager &manager);
}  // namespace

// This example shows how to use the TableMaker wrapper to make a simple table.
int main(int argc, char *argv[]) {
  const char *server = "localhost:10000";
  if (argc > 1) {
    if (argc != 2 || std::strcmp("-h", argv[1]) == 0) {
      std::cerr << "Usage: " << argv[0] << " [host:port]" << std::endl;
      std::exit(1);
    }
    server = argv[1];
  }

  try {
    auto client = Client::Connect(server);
    auto manager = client.GetManager();
    Doit(manager);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void Doit(const TableHandleManager &manager) {
  TableMaker tm;
  std::vector<std::string> symbols{"FB", "AAPL", "IBM"};
  std::vector<double> prices{111.111, 222.222, 333.333};
  tm.AddColumn("Symbol", symbols);
  tm.AddColumn("Price", prices);
  auto table = tm.MakeTable(manager);

  std::cout << "table is:\n" << table.Stream(true) << std::endl;
}
}  // namespace
