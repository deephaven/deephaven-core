cmake_minimum_required(VERSION 3.16)
project(demos)

set(CMAKE_CXX_STANDARD 17)

add_executable(chapter1 chapter1.cc)
add_executable(chapter2 chapter2.cc)
add_executable(chapter3 chapter3.cc)

target_link_libraries(chapter1 deephaven::client)
target_link_libraries(chapter2 deephaven::client)
target_link_libraries(chapter3 deephaven::client)
