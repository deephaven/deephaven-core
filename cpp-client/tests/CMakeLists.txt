cmake_minimum_required(VERSION 3.16)
project(tests)

set(CMAKE_CXX_STANDARD 17)

add_executable(tests
    add_drop_example.cc
    aggregates_example.cc
    head_and_tail_example.cc
    filter_example.cc
    join_example.cc
    lastby_example.cc
    main.cc
    merge_tables_example.cc
    new_table_example.cc
    select_example.cc
    sort_example.cc
    string_filter_example.cc
    table_cleanup_example.cc
    test_util.cc
    test_util.h
    ungroup_example.cc
    validation_example.cc
    view_example.cc
    )
target_compile_options(tests PRIVATE -Wall -Werror)
target_include_directories(tests PUBLIC "..")

add_subdirectory("../deephaven" "libclient")
target_link_libraries(tests deephaven::client)
