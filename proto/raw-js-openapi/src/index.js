require("deephaven/proto/session_pb");
require("deephaven/proto/table_pb");
require("deephaven/proto/console_pb");
require("deephaven/proto/ticket_pb");
require("Flight_pb")
require("BrowserFlight_pb")
var sessionService = require("deephaven/proto/session_pb_service");
var tableService = require("deephaven/proto/table_pb_service");
var consoleService = require("deephaven/proto/console_pb_service");
var browserFlightService = require("BrowserFlight_pb_service");
var flightService = require("Flight_pb_service");

var browserHeaders = require("browser-headers");

var grpcWeb = require("@improbable-eng/grpc-web");//usually .grpc
var jspb = require("google-protobuf");
var flatbuffers = require("flatbuffers").flatbuffers;
var barrage = require("@deephaven/barrage");

var message = require('./arrow/flight/flatbuf/Message_generated');
var schema = require('./arrow/flight/flatbuf/Schema_generated');

var io = { deephaven: {
    proto: {
            session_pb: proto.io.deephaven.proto.backplane.grpc,
            session_pb_service: sessionService,
            table_pb: proto.io.deephaven.proto.backplane.grpc,
            table_pb_service: tableService,
            console_pb: proto.io.deephaven.proto.backplane.script.grpc,
            console_pb_service: consoleService,
            ticket_pb: proto.io.deephaven.proto.backplane.grpc,
        },
        barrage: {
            "flatbuf": {
                "Barrage_generated": barrage,
            }
        }
}};
var arrow = { flight: {
    flatbuf: {
        Message_generated: message,
        Schema_generated: schema,
    },
    protocol: {
            Flight_pb: proto.arrow.flight.protocol,
            Flight_pb_service: flightService,
            BrowserFlight_pb: proto.arrow.flight.protocol,
            BrowserFlight_pb_service: browserFlightService
    }
}};
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
