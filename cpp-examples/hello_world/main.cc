/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <exception>
#include <iostream>
#include "deephaven/client/client.h"

using deephaven::client::Client;

int main() {
  const char *server = "localhost:10000";
  try {
    auto client = Client::connect(server);
    auto manager = client.getManager();
    auto table = manager.emptyTable(10);
    auto t2 = table.update("ABC = ii + 100");
    std::cout << t2.stream(true) << '\n';
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
  return 0;
}
