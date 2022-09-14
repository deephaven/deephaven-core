cmake_minimum_required(VERSION 3.16)
project(read_csv)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(../../cpp-client/deephaven deephaven_dir)
#find_package(deephaven REQUIRED)

add_executable(read_csv main.cc)

target_link_libraries(read_csv deephaven::client)
