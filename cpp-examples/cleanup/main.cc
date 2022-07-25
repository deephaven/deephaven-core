/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/client.h"

using deephaven::client::NumCol;
using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;

// This example shows explicit QueryTable cleanup using destructors/RAII.
namespace {
void doit(const TableHandleManager &manager);
}

int main() {
  const char *server = "localhost:10000";

  try {
    auto client = Client::connect(server);
    auto manager = client.getManager();
    doit(manager);
  } catch (const std::runtime_error &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void doit(const TableHandleManager &manager) {
  auto table = manager.emptyTable(10).update("X = ii % 2", "Y = ii");
  auto [x, y] = table.getCols<NumCol, NumCol>("X", "Y");
  // This example will dispose each table individually.

  auto t1 = table.where(y < 5);
  std::cout << "This is t1:\n" << t1.stream(true) << '\n';

  {
    TableHandle t2Copy;
    {
      auto t2 = t1.countBy(x);
      std::cout << "This is t2:\n" << t2.stream(true) << '\n';

      t2Copy = t2;

      // The variable 't2' will be destructed here, but the server resource will stay alive
      // because 't2Copy' is still live.
    }
    std::cout << "t2Copy still alive:\n" << t2Copy.stream(true) << '\n';

    // t2Copy will be destructed here. As it is the last owner of the server resource,
    // the server resource will be released here.
  }

  // t1 and the TableHandleManger will be destructed here.
}
}  // namespace
