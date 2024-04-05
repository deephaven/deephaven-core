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
var session_pb_service = require("deephaven/proto/session_pb_service");
var table_pb_service = require("deephaven/proto/table_pb_service");
var console_pb_service = require("deephaven/proto/console_pb_service");
var application_pb_service = require("deephaven/proto/application_pb_service");
var inputtable_pb_service = require("deephaven/proto/inputtable_pb_service");
var object_pb_service = require("deephaven/proto/object_pb_service");
var partitionedtable_pb_service = require("deephaven/proto/partitionedtable_pb_service");
var storage_pb_service = require("deephaven/proto/storage_pb_service");
var config_pb_service = require("deephaven/proto/config_pb_service");
var hierarchicaltable_pb_service = require("deephaven/proto/hierarchicaltable_pb_service");
var BrowserFlight_pb_service = require("BrowserFlight_pb_service");
var Flight_pb_service = require("Flight_pb_service");

var browserHeaders = require("browser-headers");

var grpcWeb = require("@improbable-eng/grpc-web");//usually .grpc
var jspb = require("google-protobuf");
var flatbuffers = require("flatbuffers").flatbuffers;
var barrage = require("@deephaven/barrage");

var message = require('./arrow/flight/flatbuf/Message_generated');
var schema = require('./arrow/flight/flatbuf/Schema_generated');

var io = { deephaven: {
    proto: {
            session_pb,
            session_pb_service,
            table_pb,
            table_pb_service,
            console_pb,
            console_pb_service,
            ticket_pb,
            application_pb,
            application_pb_service,
            inputtable_pb,
            inputtable_pb_service,
            object_pb,
            object_pb_service,
            partitionedtable_pb,
            partitionedtable_pb_service,
            storage_pb,
            storage_pb_service,
            config_pb,
            config_pb_service,
            hierarchicaltable_pb,
            hierarchicaltable_pb_service
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
        Flight_pb,
        Flight_pb_service,
        BrowserFlight_pb,
        BrowserFlight_pb_service
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
