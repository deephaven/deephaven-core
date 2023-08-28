/*
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
#include <exception>
#include <iostream>
#include "deephaven/client/client.h"

using deephaven::client::Client;

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
    auto table = manager.EmptyTable(10);
    auto t2 = table.Update("ABC = ii + 100");
    std::cout << t2.Stream(true) << '\n';
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
  return 0;
}
