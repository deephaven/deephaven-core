/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <exception>
#include <iostream>
#include <iomanip>
#include <cstdlib>
#include <thread>
#include "deephaven/client/client.h"

using deephaven::client::Client;
using deephaven::client::TableHandleManager;

void Usage(const char *const argv0, const int exit_status) {
  std::cerr << "Usage: " << argv0 << "[numthreads [host:port]]" << '\n';
  std::exit(exit_status);
}

void ThreadFun(Client *const client, const std::size_t ti) {
    auto manager = client->GetManager();
    using namespace std::chrono_literals;
    std::cout << "THREAD START " << ti << std::endl << std::flush;
    const std::string t1_name = std::string("import deephaven; t1_") + std::to_string(ti);
    while (true) {
        manager.RunScript(
                t1_name + "= deephaven.time_table(\"PT1S\")");
        std::this_thread::sleep_for(2000ms);
        auto t1 = manager.FetchTable(std::string("t1_") + std::to_string(ti));
    }
    std::cout << "THREAD END " << ti << std::endl << std::flush;
}

void Run(Client *const client, const std::size_t nthreads) {
  std::thread threads[nthreads];
  for (std::size_t i = 0; i < nthreads; ++i) {
    threads[i] = std::move(std::thread(&ThreadFun, client, i));
  }
  
  for (std::size_t i = 0; i < nthreads; ++i) {
    threads[i].join();
  }
}

int main(int argc, char *argv[]) {
  const char *server = "localhost:10000";
  std::size_t nthreads = 200;
  int c = 1;
  if (argc > 1 && std::strcmp("-h", argv[1]) == 0) {
    Usage(argv[0], 0);
  }
  if (argc > c) {
    ++c;
    try {
      nthreads = static_cast<std::size_t>(std::stoi(argv[c]));
      if (nthreads <= 0) {
        throw std::invalid_argument("<= 0");
      }
    } catch (const std::invalid_argument &e) {
      std::cerr << "argv[0]: can't convert number of threads argument #" << c << " '"
                << argv[c] << "' to a positive intenger ["
                << e.what() << "], aborting."
                << '\n';
      std::exit(1);
    }
  }
  if (argc > c) {
    server = argv[c];
  }
  ++c;
  if (argc > c) {
    Usage(argv[0], 1);
  }
  try {
    auto client = Client::Connect(server);
    Run(&client, nthreads);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
  return 0;
}
