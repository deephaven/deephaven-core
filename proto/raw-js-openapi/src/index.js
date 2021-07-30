require("Barrage_pb");
require("Session_pb");
require("Table_pb");
require("Console_pb");
require("flight_pb");
var barrageService = require("Barrage_pb_service");
var sessionService = require("Session_pb_service");
var tableService = require("Table_pb_service");
var consoleService = require("Console_pb_service");
var flightService = require("flight_pb_service");

var browserHeaders = require("browser-headers");

var grpcWeb = require("@improbable-eng/grpc-web");//usually .grpc
var jspb = require("google-protobuf");
var flatbuffers = require("flatbuffers").flatbuffers;
var barrage = require("@deephaven/barrage");

var io = { deephaven: {
    proto: {
            Barrage_pb: proto.io.deephaven.proto.backplane.grpc,
            Barrage_pb_service: barrageService,
            Session_pb: proto.io.deephaven.proto.backplane.grpc,
            Session_pb_service: sessionService,
            Table_pb: proto.io.deephaven.proto.backplane.grpc,
            Table_pb_service: tableService,
            Console_pb: proto.io.deephaven.proto.backplane.script.grpc,
            Console_pb_service: consoleService,
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
