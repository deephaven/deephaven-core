cmake_minimum_required(VERSION 3.16)
project(create_table_with_arrow_flight)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(../../cpp-client/deephaven deephaven_dir)
#find_package(deephaven REQUIRED)

add_executable(create_table_with_arrow_flight main.cc)

target_link_libraries(create_table_with_arrow_flight deephaven::client)
