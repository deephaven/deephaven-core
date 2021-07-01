require("io/deephaven/proto/barrage_pb");
require("io/deephaven/proto/session_pb");
require("io/deephaven/proto/table_pb");
require("io/deephaven/proto/console_pb");
require("arrow/flight/protocol/flight_pb");
var barrageService = require("io/deephaven/proto/barrage_pb_service");
var sessionService = require("io/deephaven/proto/session_pb_service");
var tableService = require("io/deephaven/proto/table_pb_service");
var consoleService = require("io/deephaven/proto/console_pb_service");
var flightService = require("arrow/flight/protocol/flight_pb_service");

var browserHeaders = require("browser-headers");

var grpcWeb = require("@improbable-eng/grpc-web");//usually .grpc
var jspb = require("google-protobuf");
var flatbuffers = require("flatbuffers").flatbuffers;
var barrage = require("@deephaven/barrage");

var io = { deephaven: {
    proto: {
            barrage_pb: proto.io.deephaven.proto.backplane.grpc,
            barrage_pb_service: barrageService,
            session_pb: proto.io.deephaven.proto.backplane.grpc,
            session_pb_service: sessionService,
            table_pb: proto.io.deephaven.proto.backplane.grpc,
            table_pb_service: tableService,
            console_pb: proto.io.deephaven.proto.backplane.script.grpc,
            console_pb_service: consoleService,
        },
        barrage: {
            "flatbuf": {
                "Barrage_generated": barrage,
                "Schema_generated": barrage,
                "Message_generated": barrage
            }
        }
}};
var arrow = {
    flight: {
        protocol: {
            flight_pb: proto.arrow.flight.protocol,
            flight_pb_service: flightService
        }
    }
};
var dhinternal = {
    browserHeaders,
    jspb,
    grpcWeb,//TODO need to expand this to the specific things we need
    flatbuffers,
    io,
    arrow
};
export {
    dhinternal
};
