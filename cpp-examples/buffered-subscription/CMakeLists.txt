cmake_minimum_required(VERSION 3.16)
project(buffered_subscription)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(../../cpp-client/deephaven deephaven_dir)

add_executable(buffered_subscription main.cc)

target_link_libraries(buffered_subscription deephaven::client)
