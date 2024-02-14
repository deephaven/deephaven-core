project(read_table_with_arrow_flight)

set(CMAKE_CXX_STANDARD 17)

add_executable(read_table_with_arrow_flight main.cc)

target_link_libraries(read_table_with_arrow_flight deephaven::client)
