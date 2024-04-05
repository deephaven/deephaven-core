var session_pb = require("deephaven/proto/session_pb");
var table_pb = require("deephaven/proto/table_pb");
var console_pb = require("deephaven/proto/console_pb");
var ticket_pb = require("deephaven/proto/ticket_pb");
var application_pb = require("deephaven/proto/application_pb");
var inputtable_pb = require("deephaven/proto/inputtable_pb");
var object_pb = require("deephaven/proto/object_pb");
var partitionedtable_pb = require("deephaven/proto/partitionedtable_pb");
var storage_pb = require("deephaven/proto/storage_pb");
var config_pb = require("deephaven/proto/config_pb");
var hierarchicaltable_pb = require("deephaven/proto/hierarchicaltable_pb");
var Flight_pb = require("Flight_pb")
var BrowserFlight_pb = require("BrowserFlight_pb")
var sessionService = require("deephaven/proto/session_pb_service");
var tableService = require("deephaven/proto/table_pb_service");
var consoleService = require("deephaven/proto/console_pb_service");
var applicationService = require("deephaven/proto/application_pb_service");
var inputTableService = require("deephaven/proto/inputtable_pb_service");
var objectService = require("deephaven/proto/object_pb_service");
var partitionedTableService = require("deephaven/proto/partitionedtable_pb_service");
var storageService = require("deephaven/proto/storage_pb_service");
var configService = require("deephaven/proto/config_pb_service");
var hierarchicalTableService = require("deephaven/proto/hierarchicaltable_pb_service");
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
            session_pb: session_pb,
            session_pb_service: sessionService,
            table_pb: table_pb,
            table_pb_service: tableService,
            console_pb: console_pb,
            console_pb_service: consoleService,
            ticket_pb: ticket_pb,
            application_pb: application_pb,
            application_pb_service: applicationService,
            inputtable_pb: inputtable_pb,
            inputtable_pb_service: inputTableService,
            object_pb: object_pb,
            object_pb_service: objectService,
            partitionedtable_pb: partitionedtable_pb,
            partitionedtable_pb_service: partitionedTableService,
            storage_pb: storage_pb,
            storage_pb_service: storageService,
            config_pb: config_pb,
            config_pb_service: configService,
            hierarchicaltable_pb: hierarchicaltable_pb,
            hierarchicaltable_pb_service: hierarchicalTableService
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
            Flight_pb: Flight_pb,
            Flight_pb_service: flightService,
            BrowserFlight_pb: BrowserFlight_pb,
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
