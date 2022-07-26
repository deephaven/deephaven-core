cmake_minimum_required(VERSION 3.16)
project(tests)

set(CMAKE_CXX_STANDARD 17)

add_executable(tests
    add_drop_test.cc
    aggregates_test.cc
    head_and_tail_test.cc
    filter_test.cc
    join_test.cc
    lastby_test.cc
    main.cc
    merge_tables_test.cc
    new_table_test.cc
    select_test.cc
    sort_test.cc
    string_filter_test.cc
    test_util.cc
    test_util.h
    ungroup_test.cc
    validation_test.cc
    view_test.cc

    third_party/catch.hpp
    )

target_compile_options(tests PRIVATE -Wall -Werror)
target_include_directories(tests PUBLIC "..")

add_subdirectory("../deephaven" "libclient")
target_link_libraries(tests deephaven::client)
