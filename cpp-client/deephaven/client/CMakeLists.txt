cmake_minimum_required(VERSION 3.16)
project(client)

set(CMAKE_CXX_STANDARD 17)

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED HINTS ${Arrow_DIR})
find_package(Flatbuffers REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)

set(ALL_FILES
    src/lowlevel/server.cc

    include/private/deephaven/client/lowlevel/server.h

    src/highlevel/impl/aggregate_impl.cc
    src/highlevel/impl/boolean_expression_impl.cc
    src/highlevel/impl/client_impl.cc
    src/highlevel/impl/columns_impl.cc
    src/highlevel/impl/datetime_expression_impl.cc
    src/highlevel/impl/escape_utils.cc
    src/highlevel/impl/expression_impl.cc
    src/highlevel/impl/numeric_expression_impl.cc
    src/highlevel/impl/string_expression_impl.cc
    src/highlevel/impl/table_handle_impl.cc
    src/highlevel/impl/table_handle_manager_impl.cc

    include/private/deephaven/client/highlevel/impl/aggregate_impl.h
    include/private/deephaven/client/highlevel/impl/boolean_expression_impl.h
    include/private/deephaven/client/highlevel/impl/client_impl.h
    include/private/deephaven/client/highlevel/impl/columns_impl.h
    include/private/deephaven/client/highlevel/impl/datetime_expression_impl.h
    include/private/deephaven/client/highlevel/impl/escape_utils.h
    include/private/deephaven/client/highlevel/impl/expression_impl.h
    include/private/deephaven/client/highlevel/impl/numeric_expression_impl.h
    include/private/deephaven/client/highlevel/impl/string_expression_impl.h
    include/private/deephaven/client/highlevel/impl/table_handle_impl.h
    include/private/deephaven/client/highlevel/impl/table_handle_manager_impl.h

    src/highlevel/columns.cc
    src/highlevel/expressions.cc
    src/highlevel/client.cc
    src/highlevel/ticking.cc
    src/highlevel/types.cc
    src/highlevel/sad/sad_column_source.cc
    src/highlevel/sad/sad_row_sequence.cc
    src/highlevel/sad/sad_table.cc
    src/highlevel/sad/sad_ticking_table.cc
    src/highlevel/sad/sad_unwrapped_table.cc

    include/public/deephaven/client/highlevel/columns.h
    include/public/deephaven/client/highlevel/expressions.h
    include/public/deephaven/client/highlevel/client.h
    include/public/deephaven/client/highlevel/ticking.h
    include/public/deephaven/client/highlevel/types.h
    include/public/deephaven/client/highlevel/sad/sad_chunk.h
    include/public/deephaven/client/highlevel/sad/sad_column_source.h
    include/public/deephaven/client/highlevel/sad/sad_context.h
    include/public/deephaven/client/highlevel/sad/sad_row_sequence.h
    include/public/deephaven/client/highlevel/sad/sad_table.h
    include/public/deephaven/client/highlevel/sad/sad_ticking_table.h
    include/public/deephaven/client/highlevel/sad/sad_unwrapped_table.h

    src/utility/executor.cc
    include/private/deephaven/client/utility/executor.h

    src/utility/cbfuture.cc
    src/utility/table_maker.cc
    src/utility/utility.cc
    include/public/deephaven/client/utility/callbacks.h
    include/public/deephaven/client/utility/cbfuture.h
    include/public/deephaven/client/utility/table_maker.h
    include/public/deephaven/client/utility/utility.h

    proto/deephaven/proto/application.grpc.pb.cc
    proto/deephaven/proto/application.grpc.pb.h
    proto/deephaven/proto/application.pb.cc
    proto/deephaven/proto/application.pb.h
    proto/deephaven/proto/console.grpc.pb.cc
    proto/deephaven/proto/console.grpc.pb.h
    proto/deephaven/proto/console.pb.cc
    proto/deephaven/proto/console.pb.h
    proto/deephaven/proto/session.grpc.pb.cc
    proto/deephaven/proto/session.grpc.pb.h
    proto/deephaven/proto/session.pb.cc
    proto/deephaven/proto/session.pb.h
    proto/deephaven/proto/table.grpc.pb.cc
    proto/deephaven/proto/table.grpc.pb.h
    proto/deephaven/proto/table.pb.cc
    proto/deephaven/proto/table.pb.h
    proto/deephaven/proto/ticket.grpc.pb.cc
    proto/deephaven/proto/ticket.grpc.pb.h
    proto/deephaven/proto/ticket.pb.cc
    proto/deephaven/proto/ticket.pb.h

    flatbuf/deephaven/flatbuf/Barrage_generated.h
    include/public/deephaven/client/highlevel/sad/chunk_maker.h src/highlevel/sad/chunk_maker.cc src/highlevel/sad/sad_chunk.cc include/public/deephaven/client/highlevel/sad/chunk_filler.h src/highlevel/sad/chunk_filler.cc src/highlevel/sad/sad_context.cc)

add_library(client ${ALL_FILES})
# This is so deephaven::client works both when using the installed CMake config
# and when using this project as a CMake subdirectory of your own project.
add_library(deephaven::client ALIAS client)
target_compile_options(client PRIVATE -Wall -Werror)

target_include_directories(client PRIVATE include/private)
target_include_directories(client PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/public>)

# Protos and flatbuf are doing their own thing.
target_include_directories(client PRIVATE "./proto")
target_include_directories(client PRIVATE "./flatbuf")

target_link_libraries(client PUBLIC arrow_flight_static)
target_link_libraries(client PUBLIC arrow_static)
target_link_libraries(client PUBLIC protobuf::libprotobuf)
target_link_libraries(client PUBLIC flatbuffers::flatbuffers)
target_link_libraries(client PUBLIC gRPC::grpc++)
target_link_libraries(client PUBLIC Threads::Threads)
