project(read_csv)

set(CMAKE_CXX_STANDARD 20)

add_executable(read_csv main.cc)

target_link_libraries(read_csv deephaven::client)
