cmake_minimum_required(VERSION 3.14)
project(table_cleanup)

set(CMAKE_CXX_STANDARD 17)

add_executable(table_cleanup main.cc)

target_link_libraries(table_cleanup deephaven::client)
