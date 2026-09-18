project(create_table_with_arrow_flight)

set(CMAKE_CXX_STANDARD 20)

add_executable(create_table_with_arrow_flight main.cc)

target_link_libraries(create_table_with_arrow_flight deephaven::client)
