project(create_table_with_table_maker)

set(CMAKE_CXX_STANDARD 17)

add_executable(create_table_with_table_maker main.cc)

target_link_libraries(create_table_with_table_maker deephaven::client)
