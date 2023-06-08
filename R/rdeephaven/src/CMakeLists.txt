cmake_minimum_required(VERSION 3.16)
project(client_caller)

set(CMAKE_CXX_STANDARD 17)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED HINTS ${Arrow_DIR})
find_package(Immer REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)
find_package(deephaven REQUIRED)

add_executable(client_caller client_caller.cpp)

target_link_libraries(client_caller deephaven::client)