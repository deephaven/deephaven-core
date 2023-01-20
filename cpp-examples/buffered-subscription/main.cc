/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
#include <chrono>
#include <exception>
#include <iostream>
#include "deephaven/client/client.h"

using deephaven::client::BufferedSubscriptionUpdate;
using deephaven::client::Client;

namespace {
void doit(const char *server);
void showNext(const BufferedSubscriptionUpdate &update);
}  // namespace

int main() {
  const char *server = "localhost:10000";

  try {
    doit(server);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
    return 0;
  }
}

namespace {
void doit(const char *server) {
  auto client = Client::connect(server);
  auto manager = client.getManager();
  auto now = std::chrono::system_clock::now();
  auto oneSecond = std::chrono::seconds(1);
  auto table = manager.timeTable(now, oneSecond);
  auto t2 = table.update("ABC = ii + 100");
  t2.bindToVariable("kosak");

  std::cout << "start flat\n" << t2.stream(true) << "\nend flat\n";
  auto sub = t2.subscribeBuffered(5);

  while (true) {
    auto next = sub.getNext();
    showNext(next);
    while (true) {
      auto n2o = sub.tryGetNext();
      if (!n2o.has_value()) {
        break;
      }
      std::cout << "CATCHING UP!!!!\n";
      showNext(*n2o);
    }
    std::this_thread::sleep_for(oneSecond * 15);
  }
}

void showNext(const BufferedSubscriptionUpdate &update) {
  if (update.overflow()) {
    std::cout << "Missed some frames!!!!!\n";
  }
  std::cout << update.update().current()->stream(true, false) << '\n';
}
}  // namespace
