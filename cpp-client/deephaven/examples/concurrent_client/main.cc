/*
 * Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
 */
#include <exception>
#include <iostream>
#include <iomanip>
#include <memory>
#include <cstdlib>
#include <thread>
#include "deephaven/client/client.h"
#include "deephaven/third_party/fmt/format.h"
#include "deephaven/third_party/fmt/ostream.h"

using deephaven::client::Client;
using deephaven::client::TableHandleManager;

void Usage(const char *const argv0, const int exit_status) {
  std::cerr << "Usage: " << argv0 << "[numthreads [iterations [host:port]]]" << '\n';
  std::exit(exit_status);
}

void ThreadFun(std::shared_ptr<Client> clientp, const std::size_t ti) {
    auto manager = clientp->GetManager();
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

void Run(std::shared_ptr<Client> clientp, const std::size_t nthreads) {
  std::vector<std::thread> threads;
  for (std::size_t i = 0; i < nthreads; ++i) {
    threads.emplace_back(&ThreadFun, clientp, i);
  }
  
  for (std::size_t i = 0; i < nthreads; ++i) {
    threads[i].join();
  }
}

std::size_t get_size_arg(
   const char *const description,
   const std::size_t defaultval,
   const int argc,
   const char *const argv[],
   int &c) {
  if (argc <= c) {
    return defaultval;
  }
  
  ++c;
  try {
    const std::size_t val = static_cast<std::size_t>(std::stoi(argv[c]));
    if (val <= 0) {
      throw std::invalid_argument("<= 0");
    }
    return val;
  } catch (const std::invalid_argument &e) {
    fmt::println(std::cerr, "{}: can't convert {} argument #{} '{}' to a positive intenger [{}], aborting.",
                 argv[0], description, c, argv[c], e.what());
    std::exit(1);
  }
}

int main(int argc, char *argv[]) {
  int c = 1;
  if (argc > 1 && std::strcmp("-h", argv[1]) == 0) {
    Usage(argv[0], 0);
  }
  std::size_t nthreads = get_size_arg("number of threads", 200, argc, argv, c);
  std::size_t iterations = get_size_arg("number of iterations", 600, argc, argv, c);
  const char *endpoint = "localhost:10000";
  if (argc > c) {
    endpoint = argv[c];
  }
  ++c;
  if (argc > c) {
    Usage(argv[0], 1);
  }
  try {
    auto client = Client::Connect(endpoint);
    Run(std::make_shared<Client>(std::move(client)), nthreads);
  } catch (const std::exception &e) {
    std::cerr << "Caught exception: " << e.what() << '\n';
  }
  return 0;
}
