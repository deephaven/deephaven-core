#include <iostream>
#include "deephaven/client/highlevel/client.h"

using deephaven::client::highlevel::Client;

int main() {
  const char *server = "localhost:10000";
  auto client = Client::connect(server);
  auto manager = client.getManager();
  auto table = manager.emptyTable(10);
  auto t2 = table.update("ABC = ii + 100");
  std::cout << t2.stream(true) << '\n';

  return 0;
}
