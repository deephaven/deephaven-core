cmake_minimum_required(VERSION 3.16)
project(example_using_installed_dh)

set(CMAKE_CXX_STANDARD 17)

find_package(deephaven REQUIRED)

# I think we can move these find_package calls to the deephaven cmake config
find_package(absl REQUIRED)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED HINTS ${Arrow_DIR})
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)

add_executable(example_using_installed_dh main.cc)

target_link_libraries(example_using_installed_dh deephaven::client)
