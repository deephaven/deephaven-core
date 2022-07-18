cmake_minimum_required(VERSION 3.16)
project(alt_arrow)

set(CMAKE_CXX_STANDARD 17)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED HINTS ${Arrow_DIR})
find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

set(ALL_FILES
    main.cc

    deephaven/proto/application.grpc.pb.cc
    deephaven/proto/application.grpc.pb.h
    deephaven/proto/application.pb.cc
    deephaven/proto/application.pb.h
    deephaven/proto/console.grpc.pb.cc
    deephaven/proto/console.grpc.pb.h
    deephaven/proto/console.pb.cc
    deephaven/proto/console.pb.h
    deephaven/proto/inputtable.grpc.pb.cc
    deephaven/proto/inputtable.grpc.pb.h
    deephaven/proto/inputtable.pb.cc
    deephaven/proto/inputtable.pb.h
    deephaven/proto/object.grpc.pb.cc
    deephaven/proto/object.grpc.pb.h
    deephaven/proto/object.pb.cc
    deephaven/proto/object.pb.h
    deephaven/proto/partitionedtable.grpc.pb.cc
    deephaven/proto/partitionedtable.grpc.pb.h
    deephaven/proto/partitionedtable.pb.cc
    deephaven/proto/partitionedtable.pb.h
    deephaven/proto/session.grpc.pb.cc
    deephaven/proto/session.grpc.pb.h
    deephaven/proto/session.pb.cc
    deephaven/proto/session.pb.h
    deephaven/proto/table.grpc.pb.cc
    deephaven/proto/table.grpc.pb.h
    deephaven/proto/table.pb.cc
    deephaven/proto/table.pb.h
    deephaven/proto/ticket.grpc.pb.cc
    deephaven/proto/ticket.grpc.pb.h
    deephaven/proto/ticket.pb.cc
    deephaven/proto/ticket.pb.h
    )


add_executable(alt_arrow ${ALL_FILES})
target_include_directories(alt_arrow PUBLIC .)
target_link_libraries(alt_arrow PUBLIC arrow_flight_static)
target_link_libraries(alt_arrow PUBLIC arrow_static)
target_link_libraries(alt_arrow PUBLIC protobuf::libprotobuf)
target_link_libraries(alt_arrow PUBLIC gRPC::grpc++)
