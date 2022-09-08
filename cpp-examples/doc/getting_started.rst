Getting Started
===============

Installation
------------

1. Install the Deephaven library and the packages it depends on. Instructions
   for doing so are in the Deephaven repo in ``cpp-client/README.md``.
2. Confirm that you are able to build and run the "Hello, WOrld" example
   program provided in the Deephaven Examples repo in the directory
   ``hello_world``
3. The file ``hello_world/CMakeLists.txt`` shows how to use CMake to build
   your project with the Deephaven Client library. You can use this as a
   starting point for your own projects.

Hello, World
------------

Deephaven client programs typically follow this recipe:

1. Connect to the server with
   :cpp:func:`Client::connect <deephaven::client::Client::connect>`
   providing the server hostname and port.
2. Call
   :cpp:func:`Client.getManager <deephaven::client::Client::getManager>`
   to get a
   :cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>`.
3. Use that
   :cpp:class:`TableHandleManager <deephaven::client::TableHandleManager>`
   to create, access, or modify tables in the system.
4. Clean up resources automatically when the variables exit their scopes.

This is the "Hello, World" example from the file ``hello_world/main.cc``.

.. code:: c++

  #include <iostream>
  #include "deephaven/client/highlevel/client.h"

  using deephaven::client::Client;

  int main() {
    const char *server = "localhost:10000";
    auto client = Client::connect(server);
    auto manager = client.getManager();
    auto table = manager.emptyTable(10);
    auto t2 = table.update("ABC = ii + 100");
    std::cout << t2.stream(true) << '\n';

    return 0;
  }

This is the corresponding ``CMakeLists.txt``:

.. code:: cmake

  cmake_minimum_required(VERSION 3.16)
  project(hello_world)

  set(CMAKE_CXX_STANDARD 17)

  find_package(deephaven REQUIRED)

  find_package(absl REQUIRED)
  find_package(Arrow REQUIRED)
  find_package(ArrowFlight REQUIRED HINTS ${Arrow_DIR})
  find_package(Protobuf REQUIRED)
  find_package(gRPC REQUIRED)
  find_package(Threads REQUIRED)

  add_executable(hello_world main.cc)

  target_link_libraries(hello_world deephaven::client)

To build and run the example, type

.. code:: bash

  cd cpp_examples/hello_world
  mkdir build && cd build
  cmake ..
  make -j8
  ./hello_world
