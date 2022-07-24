cmake_minimum_required(VERSION 3.16)
project(compare_approaches)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(../../cpp-client/deephaven deephaven_dir)
#find_package(deephaven REQUIRED)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED HINTS ${Arrow_DIR})
find_package(Boost REQUIRED)
find_package(Immer REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)
find_package(Immer REQUIRED)

add_executable(compare_approaches main.cc)

target_link_libraries(compare_approaches deephaven::client)
