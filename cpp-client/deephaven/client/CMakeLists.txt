cmake_minimum_required(VERSION 3.16)
project(client)

set(CMAKE_CXX_STANDARD 17)

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED HINTS ${Arrow_DIR})
find_package(Boost REQUIRED)
find_package(Immer REQUIRED)
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)
find_package(Threads REQUIRED)

set(ALL_FILES
    src/server/server.cc
    include/private/deephaven/client/server/server.h

    src/impl/aggregate_impl.cc
    src/impl/boolean_expression_impl.cc
    src/impl/client_impl.cc
    src/impl/columns_impl.cc
    src/impl/datetime_expression_impl.cc
    src/impl/escape_utils.cc
    src/impl/expression_impl.cc
    src/impl/numeric_expression_impl.cc
    src/impl/string_expression_impl.cc
    src/impl/table_handle_impl.cc
    src/impl/table_handle_manager_impl.cc

    include/private/deephaven/client/impl/aggregate_impl.h
    include/private/deephaven/client/impl/boolean_expression_impl.h
    include/private/deephaven/client/impl/client_impl.h
    include/private/deephaven/client/impl/columns_impl.h
    include/private/deephaven/client/impl/datetime_expression_impl.h
    include/private/deephaven/client/impl/escape_utils.h
    include/private/deephaven/client/impl/expression_impl.h
    include/private/deephaven/client/impl/numeric_expression_impl.h
    include/private/deephaven/client/impl/string_expression_impl.h
    include/private/deephaven/client/impl/table_handle_impl.h
    include/private/deephaven/client/impl/table_handle_manager_impl.h
    include/private/deephaven/client/impl/util.h

    include/private/deephaven/client/arrowutil/arrow_visitors.h
    include/private/deephaven/client/arrowutil/arrow_value_converter.h

    src/columns.cc
    src/expressions.cc
    src/client.cc
    src/ticking.cc
    src/types.cc
    src/chunk/chunk.cc
    src/chunk/chunk_filler.cc
    src/chunk/chunk_maker.cc
    src/column/array_column_source.cc
    src/column/column_source.cc
    src/container/row_sequence.cc
    src/table/table.cc

    include/public/deephaven/client/columns.h
    include/public/deephaven/client/expressions.h
    include/public/deephaven/client/client.h
    include/public/deephaven/client/ticking.h
    include/public/deephaven/client/types.h
    include/public/deephaven/client/chunk/chunk.h
    include/public/deephaven/client/chunk/chunk_filler.h
    include/public/deephaven/client/chunk/chunk_maker.h
    include/public/deephaven/client/column/array_column_source.h
    include/public/deephaven/client/column/column_source.h
    include/public/deephaven/client/container/row_sequence.h
    include/public/deephaven/client/table/table.h

    include/private/deephaven/client/subscription/batch_parser.h
    include/private/deephaven/client/subscription/classic_table_state.h
    include/private/deephaven/client/subscription/immer_table_state.h
    include/private/deephaven/client/subscription/index_decoder.h
    include/private/deephaven/client/subscription/shift_processor.h
    include/private/deephaven/client/subscription/space_mapper.h
    include/private/deephaven/client/subscription/subscribe_thread.h
    include/private/deephaven/client/subscription/subscription_handle.h
    include/private/deephaven/client/subscription/update_processor.h
    src/subscription/batch_parser.cc
    src/subscription/classic_table_state.cc
    src/subscription/immer_table_state.cc
    src/subscription/index_decoder.cc
    src/subscription/shift_processor.cc
    src/subscription/space_mapper.cc
    src/subscription/subscribe_thread.cc
    src/subscription/update_processor.cc

    src/immerutil/abstract_flex_vector.cc
    src/immerutil/immer_column_source.cc
    include/private/deephaven/client/immerutil/abstract_flex_vector.h
    include/private/deephaven/client/immerutil/immer_column_source.h

    src/utility/executor.cc
    src/utility/misc.cc
    include/private/deephaven/client/utility/executor.h
    include/private/deephaven/client/utility/misc.h

    src/utility/cbfuture.cc
    src/utility/misc.cc
    src/utility/table_maker.cc
    src/utility/utility.cc
    include/public/deephaven/client/utility/callbacks.h
    include/public/deephaven/client/utility/cbfuture.h
    include/private/deephaven/client/utility/misc.h
    include/public/deephaven/client/utility/table_maker.h
    include/public/deephaven/client/utility/utility.h

    flatbuf/deephaven/flatbuf/Barrage_generated.h

    proto/deephaven/proto/application.grpc.pb.cc
    proto/deephaven/proto/application.grpc.pb.h
    proto/deephaven/proto/application.pb.cc
    proto/deephaven/proto/application.pb.h
    proto/deephaven/proto/console.grpc.pb.cc
    proto/deephaven/proto/console.grpc.pb.h
    proto/deephaven/proto/console.pb.cc
    proto/deephaven/proto/console.pb.h
    proto/deephaven/proto/inputtable.grpc.pb.cc
    proto/deephaven/proto/inputtable.grpc.pb.h
    proto/deephaven/proto/inputtable.pb.cc
    proto/deephaven/proto/inputtable.pb.h
    proto/deephaven/proto/object.grpc.pb.cc
    proto/deephaven/proto/object.grpc.pb.h
    proto/deephaven/proto/object.pb.cc
    proto/deephaven/proto/object.pb.h
    proto/deephaven/proto/partitionedtable.grpc.pb.cc
    proto/deephaven/proto/partitionedtable.grpc.pb.h
    proto/deephaven/proto/partitionedtable.pb.cc
    proto/deephaven/proto/partitionedtable.pb.h
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

    third_party/flatbuffers/include/flatbuffers/allocator.h
    third_party/flatbuffers/include/flatbuffers/array.h
    third_party/flatbuffers/include/flatbuffers/base.h
    third_party/flatbuffers/include/flatbuffers/buffer.h
    third_party/flatbuffers/include/flatbuffers/buffer_ref.h
    third_party/flatbuffers/include/flatbuffers/default_allocator.h
    third_party/flatbuffers/include/flatbuffers/detached_buffer.h
    third_party/flatbuffers/include/flatbuffers/flatbuffer_builder.h
    third_party/flatbuffers/include/flatbuffers/flatbuffers.h
    third_party/flatbuffers/include/flatbuffers/stl_emulation.h
    third_party/flatbuffers/include/flatbuffers/string.h
    third_party/flatbuffers/include/flatbuffers/struct.h
    third_party/flatbuffers/include/flatbuffers/table.h
    third_party/flatbuffers/include/flatbuffers/util.h
    third_party/flatbuffers/include/flatbuffers/vector_downward.h
    third_party/flatbuffers/include/flatbuffers/vector.h
    third_party/flatbuffers/include/flatbuffers/verifier.h
    )

add_library(client ${ALL_FILES})
# This is so deephaven::client works both when using the installed CMake config
# and when using this project as a CMake subdirectory of your own project.
add_library(deephaven::client ALIAS client)
target_compile_options(client PRIVATE -Wall -Werror)

target_include_directories(client PRIVATE ${Boost_INCLUDE_DIR})
target_include_directories(client PRIVATE include/private)
target_include_directories(client PRIVATE third_party/flatbuffers/include)
target_include_directories(client PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/public>)

# Protos and flatbuf are doing their own thing.
target_include_directories(client PRIVATE "./proto")
target_include_directories(client PRIVATE "./flatbuf")

target_link_libraries(client PUBLIC arrow_flight_static)
target_link_libraries(client PUBLIC arrow_static)
target_link_libraries(client PUBLIC Boost::boost)
target_link_libraries(client PUBLIC immer)
target_link_libraries(client PUBLIC protobuf::libprotobuf)
target_link_libraries(client PUBLIC gRPC::grpc++)
target_link_libraries(client PUBLIC Threads::Threads)

