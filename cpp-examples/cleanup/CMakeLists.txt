cmake_minimum_required(VERSION 3.16)
project(cleanup)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(../../cpp-client/deephaven deephaven_dir)
#find_package(deephaven REQUIRED)

add_executable(cleanup main.cc)

target_link_libraries(cleanup deephaven::client)
