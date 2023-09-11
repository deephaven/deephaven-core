cmake_minimum_required(VERSION 3.16)
project(tests)

set(CMAKE_CXX_STANDARD 17)

add_executable(tests
    add_drop_test.cc
    aggregates_test.cc
    attributes_test.cc
    basic_test.cc
    buffer_column_source_test.cc
    cython_support_test.cc
    date_time_test.cc
    filter_test.cc
    head_and_tail_test.cc
    input_table_test.cc
    join_test.cc
    lastby_test.cc
    main.cc
    merge_tables_test.cc
    new_table_test.cc
    on_close_cb_test.cc
    script_test.cc
    select_test.cc
    sort_test.cc
    string_filter_test.cc
    test_util.cc
    test_util.h
    ticking_test.cc
    ungroup_test.cc
    update_by_test.cc
    utility_test.cc
    validation_test.cc
    view_test.cc

    third_party/catch.hpp
)

target_compile_options(tests PRIVATE -Wall -Werror)
target_include_directories(tests PUBLIC "..")

target_link_libraries(tests deephaven::client)
