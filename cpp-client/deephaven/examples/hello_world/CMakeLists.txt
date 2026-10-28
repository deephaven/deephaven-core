project(hello_world)

set(CMAKE_CXX_STANDARD 20)

add_executable(hello_world main.cc)

target_link_libraries(hello_world deephaven::client)
