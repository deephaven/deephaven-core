cmake_minimum_required(VERSION 3.14)
project(dhclient_tests)

set(CMAKE_CXX_STANDARD 20)

add_executable(dhclient_tests
        src/add_drop_test.cc
        src/aggregates_test.cc
        src/attributes_test.cc
        src/barrage_processor_test.cc
        src/basic_test.cc
        src/buffer_column_source_test.cc
        src/cython_support_test.cc
        src/date_time_test.cc
        src/encoding_test.cc
        src/filter_test.cc
        src/group_test.cc
        src/growing_subscription_test.cc
        src/head_and_tail_test.cc
        src/input_table_test.cc
        src/join_test.cc
        src/lastby_test.cc
        src/main.cc
        src/merge_tables_test.cc
        src/new_table_test.cc
        src/on_close_cb_test.cc
        src/script_test.cc
        src/select_test.cc
        src/sort_test.cc
        src/string_filter_test.cc
        src/table_state_test.cc
        src/table_test.cc
        src/test_util.cc
        src/ticking_test.cc
        src/time_unit_test.cc
        src/types_test.cc
        src/ungroup_test.cc
        src/update_by_test.cc
        src/utility_test.cc
        src/validation_test.cc
        src/view_test.cc

        include/private/deephaven/tests/barrage_test_util.h
        include/private/deephaven/tests/test_util.h
        include/private/deephaven/third_party/catch.hpp
)

if (LINUX)
    target_compile_options(dhclient_tests PRIVATE -Wall -Werror -Wno-deprecated-declarations -Wno-tautological-compare)
endif()

if (WIN32)
    target_compile_options(dhclient_tests PRIVATE /W3 /MP)
endif()

target_include_directories(dhclient_tests PRIVATE include/private)
# The BarrageProcessor tests build the flatbuffer metadata the server would send, from the generated
# schema and the flatbuffers headers that dhcore vendors.
target_include_directories(dhclient_tests PRIVATE ../dhcore/flatbuf ../dhcore/third_party/flatbuffers/include)
# The table state tests exercise ImmerTableState directly. It is private to dhcore, which exports
# every symbol, so only its headers need to be reachable, along with the vendored roaring bitmap
# header that SpaceMapper's declaration pulls in.
target_include_directories(dhclient_tests PRIVATE ../dhcore/include/private ../dhcore/third_party/roaring/include)

target_link_libraries(dhclient_tests deephaven::client)
