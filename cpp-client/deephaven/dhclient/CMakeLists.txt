cmake_minimum_required(VERSION 3.14)
project(dhclient)

set(CMAKE_CXX_STANDARD 17)

# for CMAKE_INSTALL_{dir}
include(GNUInstallDirs)

find_package(Arrow CONFIG REQUIRED)
find_package(ArrowFlight CONFIG REQUIRED HINTS ${Arrow_DIR})
find_package(Immer CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

set(PROTO_SRC_DIR
  "${CMAKE_CURRENT_SOURCE_DIR}/../../../proto/proto-backplane-grpc/src/main/proto")
set(PROTO_GEN_DIR
  "${CMAKE_BINARY_DIR}/${PROJECT_NAME}/proto")
set(PROTO_OUT_DIR "${PROTO_GEN_DIR}/deephaven/proto")

set(PROTO_FILES
    "${PROTO_SRC_DIR}/deephaven/proto/application.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/config.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/console.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/hierarchicaltable.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/inputtable.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/object.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/partitionedtable.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/session.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/storage.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/table.proto"
    "${PROTO_SRC_DIR}/deephaven/proto/ticket.proto"
)

foreach(PROTO_FILE ${PROTO_FILES})
  get_filename_component(BASENAME ${PROTO_FILE} NAME_WLE)
  list(APPEND PROTO_GEN_FILES
      "${PROTO_OUT_DIR}/${BASENAME}.grpc.pb.cc"
      "${PROTO_OUT_DIR}/${BASENAME}.grpc.pb.h"
      "${PROTO_OUT_DIR}/${BASENAME}.pb.cc"
      "${PROTO_OUT_DIR}/${BASENAME}.pb.h"
  )
endforeach()

add_custom_command(
  OUTPUT ${PROTO_GEN_FILES}
  COMMAND "${CMAKE_COMMAND}"
  ARGS -E make_directory "${PROTO_OUT_DIR}"
  COMMAND protobuf::protoc
  ARGS "--plugin=protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
       "--cpp_out=${PROTO_GEN_DIR}"
       "--grpc_out=${PROTO_GEN_DIR}"
       "-I${PROTO_SRC_DIR}"
       ${PROTO_FILES}
  DEPENDS ${PROTO_FILES}
  COMMENT "Generating protos"
)

set(ALL_FILES
    src/server/server.cc
    include/private/deephaven/client/server/server.h

    src/impl/aggregate_impl.cc
    src/impl/client_impl.cc
    src/impl/table_handle_impl.cc
    src/impl/table_handle_manager_impl.cc
    src/impl/update_by_operation_impl.cc
    include/private/deephaven/client/impl/aggregate_impl.h
    include/private/deephaven/client/impl/client_impl.h
    include/private/deephaven/client/impl/table_handle_impl.h
    include/private/deephaven/client/impl/table_handle_manager_impl.h
    include/private/deephaven/client/impl/update_by_operation_impl.h
    include/private/deephaven/client/impl/util.h

    src/arrowutil/arrow_client_table.cc
    include/private/deephaven/client/arrowutil/arrow_client_table.h
    include/private/deephaven/client/arrowutil/arrow_column_source.h
    include/private/deephaven/client/arrowutil/arrow_value_converter.h
    include/private/deephaven/client/arrowutil/arrow_visitors.h

    src/client_options.cc
    src/client.cc
    src/flight.cc
    src/update_by.cc
    include/public/deephaven/client/client.h
    include/public/deephaven/client/client_options.h
    include/public/deephaven/client/flight.h
    include/public/deephaven/client/update_by.h

    src/interop/client_interop.cc
    src/interop/client_options_interop.cc
    src/interop/update_by_interop.cc
    include/public/deephaven/client/interop/client_interop.h
    include/public/deephaven/client/interop/client_options_interop.h
    include/public/deephaven/client/interop/update_by_interop.h

    src/subscription/subscribe_thread.cc

    include/private/deephaven/client/subscription/subscribe_thread.h
    include/private/deephaven/client/subscription/subscription_handle.h

    src/utility/executor.cc
    include/private/deephaven/client/utility/executor.h

    src/utility/arrow_util.cc
    src/utility/table_maker.cc

    include/public/deephaven/client/utility/arrow_util.h
    include/public/deephaven/client/utility/misc_types.h
    include/public/deephaven/client/utility/table_maker.h
)

list(APPEND ALL_FILES ${PROTO_GEN_FILES})

add_library(dhclient SHARED ${ALL_FILES})

# This is so deephaven::client works both when using the installed CMake config
# and when using this project as a CMake subdirectory of your own project.
add_library(deephaven::client ALIAS ${PROJECT_NAME})

set_property(TARGET ${PROJECT_NAME} PROPERTY POSITION_INDEPENDENT_CODE ON)

if (LINUX)
    target_compile_options(${PROJECT_NAME} PRIVATE -Wall -Werror -Wno-deprecated-declarations)
endif()

if (WIN32)
    set_property(TARGET ${PROJECT_NAME} PROPERTY WINDOWS_EXPORT_ALL_SYMBOLS ON)
    # /Wall is a bit too chatty so we stick with /W3
    # /bigobj needed because ticking/immer_table_state.cc compiles to something too large apparently
    target_compile_options(${PROJECT_NAME} PRIVATE /W3 /bigobj)
endif()

target_include_directories(${PROJECT_NAME} PRIVATE include/private)
target_include_directories(${PROJECT_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/public>)
# Protos and flatbuf are doing their own thing.
target_include_directories(${PROJECT_NAME} PRIVATE "${PROTO_GEN_DIR}")
target_include_directories(${PROJECT_NAME} PRIVATE "./flatbuf")

target_link_libraries(${PROJECT_NAME} PUBLIC deephaven::dhcore)

target_link_libraries(${PROJECT_NAME} PUBLIC ArrowFlight::arrow_flight_shared)
target_link_libraries(${PROJECT_NAME} PUBLIC Arrow::arrow_shared)
target_link_libraries(${PROJECT_NAME} PRIVATE protobuf::libprotobuf)
target_link_libraries(${PROJECT_NAME} PRIVATE gRPC::grpc++)
target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads)
