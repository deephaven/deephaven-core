/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include <iostream>
#include "deephaven/client/client.h"

using deephaven::client::Client;
using deephaven::client::TableHandle;
using deephaven::client::TableHandleManager;

// This example shows explicit QueryTable cleanup using destructors/RAII.
namespace {
void Doit(const TableHandleManager &manager);
}

int main() {
  const char *server = "localhost:10000";

  try {
    auto client = Client::Connect(server);
    auto manager = client.GetManager();
    Doit(manager);
  } catch (const std::runtime_error &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
}

namespace {
void Doit(const TableHandleManager &manager) {
  auto table = manager.EmptyTable(10).Update("X = ii % 2", "Y = ii");
  // This example will dispose each table individually.

  auto t1 = table.Where("y < 5");
  std::cout << "This is t1:\n" << t1.Stream(true) << '\n';

  {
    TableHandle t2_copy;
    {
      auto t2 = t1.CountBy("X");
      std::cout << "This is t2:\n" << t2.Stream(true) << '\n';

      t2_copy = t2;

      // The variable 't2' will be destructed here, but the server resource will stay alive
      // because 't2_copy' is still live.
    }
    std::cout << "t2_copy still alive:\n" << t2_copy.Stream(true) << '\n';

    // t2Copy will be destructed here. As it is the last owner of the server resource,
    // the server resource will be released here.
  }

  // t1 and the TableHandleManger will be destructed here.
}
}  // namespace
