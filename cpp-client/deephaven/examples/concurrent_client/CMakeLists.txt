project(hello_world)

set(CMAKE_CXX_STANDARD 20)

add_executable(concurrent_session main.cc)

target_link_libraries(concurrent_session deephaven::client)
