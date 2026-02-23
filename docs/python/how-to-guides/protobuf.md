---
title: Protocol Buffers and Remote Procedure Calls
sidebar_label: Protobuf and RPC
---

[Protobuf](https://protobuf.dev/), short for Protocol Buffers, is a language-neutral, platform-neutral mechanism for serializing structured data. By defining how data is structured a single time, you can use Protobuf to generate source code in multiple languages, which can then be used to serialize and deserialize data consistently across different systems.

Protobuf is one of several options for users who wish to build their own Deephaven client API. It is particularly powerful when considering performance, as binary serialization is much faster than string-based serialization. This is important for Deephaven, which is specifically designed to handle large amounts of data in real time. In short, Protobuf offers a closer-to-the-hardware binary representation of data, supporting higher throughput and lower latency than text-based serialization formats like JSON or XML.

Protobuf allows you to define structured data types and services in language-neutral ways so that your code can interact with other languages and platforms. This is critical for client APIs because they need to be able to communicate with a Deephaven server regardless of the client language used. For example, the Python client API communicates with the Deephaven server despite being written largely in Java. Protobuf is the mechanism that standardizes the structures and messages sent between the two.

Deephaven defines several of its APIs using [gRPC](https://grpc.io/), Google's open-source Remote Procedure Call (RPC) framework. gRPC uses Protobuf as its interface description language, enabling it to let programs and/or objects written in one language communicate with those written in another language. Deephaven leverages this to facilitate communication between servers and clients in different languages. In particular, gRPC's support of bidirectional streaming is a key feature of Deephaven's architecture and design. For example, you can use the Java client API to connect to a Deephaven server and run queries while simultaneously using the Python client API to check the results of those queries. Protobuf can be used on its own, but since Deephaven uses it in conjunction with gRPC, this guide focuses on both.

This guide provides a brief overview of what Protobuf and RPC are, what they do, and gives two examples of Deephaven `.proto` (Protobuf) files. For more detailed and specific guidance on Protobuf and gRPC, see:

- [Protobuf programming guides](https://protobuf.dev/programming-guides/)
- [gRPC documentation](https://grpc.io/docs/)

## Protobuf in Deephaven

Deephaven uses [gRPC](https://grpc.io/) (Remote Procedure Call) to define several of its APIs. gRPC uses Protobuf as its interface description language, enabling it to let programs and/or objects written in one language communicate with those written in another language. This is particularly useful for Deephaven, which is designed to work with multiple languages and platforms. In particular, gRPC's support of bidirectional streaming is a key feature of Deephaven's architecture and design.

It's because of gRPC and Protobuf that you can use multiple client APIs to interact with a Deephaven server. For example, you can use the Java client API to connect to a Deephaven server and run queries while simultaneously using the Python client API to check the results of those queries.

## Protobuf concepts

Protobuf can be used on its own for things like [serializing and deserializing Kafka payloads](./data-import-export/kafka-stream.md#read-kafka-topic-in-protobuf-format). The concepts defined in this section are specific to Protobuf; gRPC is the mechanism that enables clients to call server functions and methods, while Protobuf describes the data passed between them.

### Messages

[Messages](https://protobuf.dev/reference/protobuf/edition-2023-spec/#message_definition) are structured data types that define schemas for serialized data. These messages contain fields with names and types in a strict, well-defined sequence.

The following example defines a message called `Person` with four fields: `name`, `id`, `email`, and `phones`.

```protobuf
syntax = "proto3";

message Person {
    string name = 1;            // Field 1: A string for the person's name
    int32 id = 2;               // Field 2: A 32-bit integer for the person's ID
    string email = 3;           // Field 3: A string for the person's email
    repeated string phones = 4; // Field 4: Zero or more strings for the person's phone numbers
}
```

### Enums

[Enums](https://protobuf.dev/reference/protobuf/edition-2023-spec/#enum_definition) are a way to define a set of named values. They are useful for defining a fixed set of options for a field in a message. Enums are defined using the `enum` keyword, followed by the name of the enum and its values.

The following example defines an Enum called `PhoneType` with three values: `MOBILE`, `HOME`, and `WORK`.

```protobuf
syntax = "proto3";

enum PhoneType {
    MOBILE = 0; // Value 0: Mobile phone
    HOME = 1;   // Value 1: Home phone
    WORK = 2;   // Value 2: Work phone
}
```

## RPC concepts

Remote Procedure Calls (RPC) allow programs to execute procedures on remote systems as if they were local. RPC allows data to be sent and received between a client and server, while Protobuf defines the structure, serializes, and deserializes it.

### Services

[Services](https://protobuf.dev/reference/protobuf/edition-2023-spec/#service_definition) are a way to define a set of RPC methods that can be called remotely. Services are defined using the `service` keyword, followed by the name of the service and its methods.

The following example defines a Service called `Greeter`, which implements two messages: `HelloRequest` and `HelloResponse` to send a hello message with a name and receive a greeting in response.

```protobuf
syntax = "proto3";

service Greeter {
    // A simple RPC method
    rpc SayHello (HelloRequest) returns (HelloResponse);
}

// Input message for the SayHello method
message HelloRequest {
    string name = 1;
}

// Output message for the SayHello method
message HelloResponse {
    string message = 1;
}
```

> [!NOTE]
> It's best practice to define separate messages for requests and responses, hence the `HelloRequest` and `HelloResponse` messages in the example above. The use of separate single-argument requests and responses allows for API evolution without breaking backward compatibility. For example, extending an API by including additional data in the request or response messages won't break existing servers or clients running old code.

## A simple Deephaven example

To illustrate how Deephaven uses Protobuf, consider the Protobuf definition for an [Input table](./input-tables.md). Input tables are tables with the special ability to add data manually via either the UI or programmatically. In the case of a client API, data can only be added programmatically. The source code shown below can be found [here](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/inputtable.proto).

```protobuf
/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
syntax = "proto3";

package io.deephaven.proto.backplane.grpc;

option java_multiple_files = true;
option optimize_for = SPEED;
option go_package = "github.com/deephaven/deephaven-core/go/internal/proto/inputtable";

import "deephaven_core/proto/ticket.proto";

/*
 * This service offers methods to manipulate the contents of input tables.
 */
service InputTableService {
    /*
     * Adds the provided table to the specified input table. The new data to add must only have
     * columns (name, types, and order) which match the given input table's columns.
     */
    rpc AddTableToInputTable(AddTableRequest) returns (AddTableResponse) {}

    /*
    * Removes the provided table from the specified input tables. The tables indicating which rows
    * to remove are expected to only have columns that match the key columns of the input table.
    */
    rpc DeleteTableFromInputTable(DeleteTableRequest) returns (DeleteTableResponse) {}

}

message AddTableRequest {
    Ticket input_table = 1;
    Ticket table_to_add = 2;
}

message AddTableResponse {

}

message DeleteTableRequest {
    Ticket input_table = 1;
    Ticket table_to_remove = 2;
}

message DeleteTableResponse {

}
```

The best way to understand this example is to break it down into its components:

### Syntax, packages, options, and imports

The first line of the file specifies the Protobuf version being used. In this case, it specifies `proto3`, the latest version of Protobuf.

Just below, the `package` statement prevents name clashes between protocol message types. In this case, this prevents name clashes with other protocol message types in the `io.deephaven.proto.backplane.grpc` package.

The `option` statements dictate custom behavior to provide additional metadata. In this case, the options specify:

- `java_multiple_files`: Generates separate Java files for each message type.
- `optimize_for`: Optimizes the generated code for speed.
- `go_package`: Specifies the Go package name for the generated code.

The `import` statement enables the use of definitions found in another `proto` file. In this case, it imports the `Ticket` message type from the `deephaven_core/proto/ticket.proto` file. This allows you to use anything defined in `ticket.proto` in this file. In particular, the `Ticket` message type is used in requests to add and delete tables from input tables.

### `InputTableService`

The `InputTableService` service defines the methods that can be called remotely. In this case, it defines two methods:

- `AddTableToInputTable`: Adds a table to an input table.

This accepts a request of type `AddTableRequest` and returns a response of type `AddTableResponse`.

- `DeleteTableFromInputTable`: Deletes a table from an input table.

This accepts a request of type `DeleteTableRequest` and returns a response of type `DeleteTableResponse`.

### Messages

The `InputTableService` service uses four different messages:

- `AddTableRequest`: The request message for the `AddTableToInputTable` method. It contains two fields: `input_table` and `table_to_add`, both of type `Ticket`. These contain information about the input table and the table that a user wishes to add to it.
- `AddTableResponse`: The response message for the `AddTableToInputTable` method. It does not contain any fields, as the response is empty. Empty messages are useful for indicating a request was successful.
- `DeleteTableRequest`: The request message for the `DeleteTableFromInputTable` method. It contains two fields: `input_table` and `table_to_remove`, both of type `Ticket`. These contain information about the input table and the table that a user wishes to remove from it.
- `DeleteTableResponse`: The response message for the `DeleteTableFromInputTable` method. It does not contain any fields, as the response is empty. Empty messages are useful for indicating a request was successful.

### Result

This `proto` file defines a service that allows a client API to add and remove tables of data from input tables. It can generate standardized code in many different languages, all of which could be used to communicate with a Deephaven server to make these requests.

## A more complex Deephaven example

Another illustration of Deephaven's use of Protobuf is the Console API. It defines how users running a client API can run commands remotely. The Console API is far more complex than the Input Table API - the latter is a simple request/response API for a table type meant to mimic tables in spreadsheets. The former enables interaction with the Deephaven console, which is a complex system that allows users to run commands, check server health, use autocomplete, get logs, and more. The full source code for `console.proto` can be found [here](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/console.proto). It is collapsed by default due to its length.

<details>

<summary>console.proto</summary>

```protobuf
/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
syntax = "proto3";

package io.deephaven.proto.backplane.script.grpc;

option java_multiple_files = true;
option optimize_for = SPEED;
option go_package = "github.com/deephaven/deephaven-core/go/internal/proto/console";

import "deephaven_core/proto/ticket.proto";
import "deephaven_core/proto/application.proto";

/*
 * Console interaction service
*/
service ConsoleService {
    rpc GetConsoleTypes(GetConsoleTypesRequest) returns (GetConsoleTypesResponse) {}
    rpc StartConsole(StartConsoleRequest) returns (StartConsoleResponse) {}
    rpc GetHeapInfo(GetHeapInfoRequest) returns (GetHeapInfoResponse) {}

    rpc SubscribeToLogs(LogSubscriptionRequest) returns (stream LogSubscriptionData) {}

    rpc ExecuteCommand(ExecuteCommandRequest) returns (ExecuteCommandResponse) {}
    rpc CancelCommand(CancelCommandRequest) returns (CancelCommandResponse) {}

    rpc BindTableToVariable(BindTableToVariableRequest) returns (BindTableToVariableResponse) {}

    /*
     * Starts a stream for autocomplete on the current session. More than one console,
     * more than one document can be edited at a time using this, and they can separately
     * be closed as well. A given document should only be edited within one stream at a
     * time.
     */
    rpc AutoCompleteStream(stream AutoCompleteRequest) returns (stream AutoCompleteResponse) {}
    rpc CancelAutoComplete(CancelAutoCompleteRequest) returns (CancelAutoCompleteResponse) {}

    /*
     * Half of the browser-based (browser's can't do bidirectional streams without websockets)
     * implementation for AutoCompleteStream.
     */
    rpc OpenAutoCompleteStream(AutoCompleteRequest) returns (stream AutoCompleteResponse) {}
    /*
     * Other half of the browser-based implementation for AutoCompleteStream.
     */
    rpc NextAutoCompleteStream(AutoCompleteRequest) returns (BrowserNextResponse) {}
}


message GetConsoleTypesRequest {
    // left empty for future compatibility
}
message GetConsoleTypesResponse {
    repeated string console_types = 1;
}

message StartConsoleRequest {
    io.deephaven.proto.backplane.grpc.Ticket result_id = 1;
    string session_type = 2;
}
message StartConsoleResponse {
    io.deephaven.proto.backplane.grpc.Ticket result_id = 1;
}

message GetHeapInfoRequest {
    // left empty for future compatibility
}
message GetHeapInfoResponse {
    // Returns the maximum amount of memory that the Java virtual machine will attempt to use.
    // If there is no inherent limit then the value Long.MAX_VALUE will be returned.
    // the maximum amount of memory that the virtual machine will attempt to use, measured in bytes
    int64 max_memory = 1 [jstype=JS_STRING];

    // Returns the total amount of memory in the Java virtual machine. The value returned by this method may vary over time, depending on the host environment.
    // Note that the amount of memory required to hold an object of any given type may be implementation-dependent.
    // the total amount of memory currently available for current and future objects, measured in bytes.
    int64 total_memory = 2 [jstype=JS_STRING];

    // Returns the amount of free memory in the Java Virtual Machine. Calling the gc method may result in increasing the value returned by freeMemory.
    // an approximation to the total amount of memory currently available for future allocated objects, measured in bytes.
    int64 free_memory = 3 [jstype=JS_STRING];
}

// Presently you get _all_ logs, not just your console. A future version might take a specific console_id to
// restrict this to a single console.
message LogSubscriptionRequest {
//    Ticket console_id = 1;
    // If a non-zero value is specified, represents the timestamp in microseconds since the unix epoch when
    // the client last saw a message. Technically this might skip messages if more than one message was
    // logged at the same microsecond that connection was lost - to avoid this, subtract one from the last
    // seen message's micros, and expect to receive some messages that have already been seen.
    int64 last_seen_log_timestamp = 1 [jstype=JS_STRING];
    repeated string levels = 2;
}
message LogSubscriptionData {
    int64 micros = 1 [jstype=JS_STRING];
    string log_level = 2;
    string message = 3;
    reserved 4;//if we can scope logs to a script session
    //    Ticket console_id = 4;
}

message ExecuteCommandRequest {
    enum SystemicType {
        NOT_SET_SYSTEMIC = 0;
        EXECUTE_NOT_SYSTEMIC = 1;
        EXECUTE_SYSTEMIC = 2;
    }

    io.deephaven.proto.backplane.grpc.Ticket console_id = 1;
    reserved 2;//if script sessions get a ticket, we will use this reserved tag
    string code = 3;

    // If set to `EXECUTE_SYSTEMIC` the command will be executed systemically.  Failures in systemic code
    // are treated as important failures and cause errors to be reported to the io.deephaven.engine.table.impl.util.AsyncClientErrorNotifier.
    // If this is unset it is treated as `EXECUTE_NOT_SYSTEMIC`
    optional SystemicType systemic = 4;
}

message ExecuteCommandResponse {
    string error_message = 1;
    io.deephaven.proto.backplane.grpc.FieldsChangeUpdate changes = 2;
}
message BindTableToVariableRequest {
    io.deephaven.proto.backplane.grpc.Ticket console_id = 1;
    reserved 2;//if script sessions get a ticket, we will use this reserved tag
    string variable_name = 3;
    io.deephaven.proto.backplane.grpc.Ticket table_id = 4;
}
message BindTableToVariableResponse {
}

message CancelCommandRequest {
    io.deephaven.proto.backplane.grpc.Ticket console_id = 1;
    io.deephaven.proto.backplane.grpc.Ticket command_id = 2;
}
message CancelCommandResponse {

}

message CancelAutoCompleteRequest {
    io.deephaven.proto.backplane.grpc.Ticket console_id = 1;
    int32 request_id = 2;
}

message CancelAutoCompleteResponse {

}

message AutoCompleteRequest {
    io.deephaven.proto.backplane.grpc.Ticket console_id = 5;
    int32 request_id = 6;
    oneof request {
        // Starts a document in a given console - to end, just close the stream, the server will hang up right away
        OpenDocumentRequest open_document = 1;

        // Modifies the document that autocomplete can be requested on
        ChangeDocumentRequest change_document = 2;

        // Requests that a response be sent back with completion items
        GetCompletionItemsRequest get_completion_items = 3;

        // Request for help about the method signature at the cursor
        GetSignatureHelpRequest get_signature_help = 7;

        // Request for help about what the user is hovering over
        GetHoverRequest get_hover = 8;

        // Request to perform file diagnostics
        GetDiagnosticRequest get_diagnostic = 9;

        // Closes the document, indicating that it will not be referenced again
        CloseDocumentRequest close_document = 4;
    }
}
message AutoCompleteResponse {
    int32 request_id = 2;
    bool success = 3;
    oneof response {
        GetCompletionItemsResponse completion_items = 1;
        GetSignatureHelpResponse signatures = 4;
        GetHoverResponse hover = 5;
        GetPullDiagnosticResponse diagnostic = 6;
        GetPublishDiagnosticResponse diagnostic_publish = 7;
    }
}

message BrowserNextResponse {
}

message OpenDocumentRequest {
    io.deephaven.proto.backplane.grpc.Ticket console_id = 1 [deprecated=true];
    TextDocumentItem text_document = 2;
}
message TextDocumentItem {
    string uri = 1;
    string language_id = 2;
    int32 version = 3;
    string text = 4;
}

message CloseDocumentRequest {
    io.deephaven.proto.backplane.grpc.Ticket console_id = 1 [deprecated=true];
    VersionedTextDocumentIdentifier text_document = 2;
}

message ChangeDocumentRequest {
    io.deephaven.proto.backplane.grpc.Ticket console_id = 1 [deprecated=true];
    VersionedTextDocumentIdentifier text_document = 2;
    repeated TextDocumentContentChangeEvent content_changes = 3;

    message TextDocumentContentChangeEvent {
        DocumentRange range = 1;
        int32 range_length = 2;
        string text = 3;
    }
}
message DocumentRange {
    Position start = 1;
    Position end = 2;
}
message VersionedTextDocumentIdentifier {
    string uri = 1;
    int32 version = 2;
}
message Position {
    int32 line = 1;
    int32 character = 2;
}

message MarkupContent {
    string kind = 1;
    string value = 2;
}

message GetCompletionItemsRequest {
    io.deephaven.proto.backplane.grpc.Ticket console_id = 1 [deprecated=true];

    CompletionContext context = 2;
    VersionedTextDocumentIdentifier text_document = 3;
    Position position = 4;

    int32 request_id = 5 [deprecated=true];
}
message CompletionContext {
    int32 trigger_kind = 1;
    string trigger_character = 2;
}
message GetCompletionItemsResponse {
    repeated CompletionItem items = 1;

    // Maintained for backwards compatibility. Use the same field on AutoCompleteResponse instead
    int32 request_id = 2 [deprecated=true];
    // Maintained for backwards compatibility. Use the same field on AutoCompleteResponse instead
    bool success = 3 [deprecated=true];
}
message CompletionItem {
    int32 start = 1;
    int32 length = 2;
    string label = 3;
    int32 kind = 4;
    string detail = 5;
    reserved 6; // Old documentation as a string. Was never used by us
    bool deprecated = 7;
    bool preselect = 8;
    TextEdit text_edit = 9;
    string sort_text = 10;
    string filter_text = 11;
    int32 insert_text_format = 12;
    repeated TextEdit additional_text_edits = 13;
    repeated string commit_characters = 14;
    MarkupContent documentation = 15;
}
message TextEdit {
    DocumentRange range = 1;
    string text = 2;
}

message GetSignatureHelpRequest {
    SignatureHelpContext context = 1;
    VersionedTextDocumentIdentifier text_document = 2;
    Position position = 3;
}
message SignatureHelpContext {
    int32 trigger_kind = 1;
    optional string trigger_character = 2;
    bool is_retrigger = 3;
    GetSignatureHelpResponse active_signature_help = 4;
}

message GetSignatureHelpResponse {
    repeated SignatureInformation signatures = 1;
    optional int32 active_signature = 2;
    optional int32 active_parameter = 3;
}

message SignatureInformation {
    string label = 1;
    MarkupContent documentation = 2;
    repeated ParameterInformation parameters = 3;
    optional int32 active_parameter = 4;
}

message ParameterInformation {
    string label = 1;
    MarkupContent documentation = 2;
}

message GetHoverRequest {
    VersionedTextDocumentIdentifier text_document = 1;
    Position position = 2;
}

message GetHoverResponse {
    MarkupContent contents = 1;
    DocumentRange range = 2;
}

message GetDiagnosticRequest {
    VersionedTextDocumentIdentifier text_document = 1;
    optional string identifier = 2;
    optional string previous_result_id = 3;
}

message GetPullDiagnosticResponse {
    string kind = 1;
    optional string result_id = 2;
    repeated Diagnostic items = 3;
}

message GetPublishDiagnosticResponse {
    string uri = 1;
    optional int32 version = 2;
    repeated Diagnostic diagnostics = 3;
}

message Diagnostic {
    enum DiagnosticSeverity {
        NOT_SET_SEVERITY = 0;
        ERROR = 1;
        WARNING = 2;
        INFORMATION = 3;
        HINT = 4;
    }

    enum DiagnosticTag {
        NOT_SET_TAG = 0;
        UNNECESSARY = 1;
        DEPRECATED = 2;
    }

    message CodeDescription {
        string href = 1;
    }

    DocumentRange range = 1;
    DiagnosticSeverity severity = 2;
    optional string code = 3;
    optional CodeDescription code_description = 4;
    optional string source = 5;
    string message = 6;
    repeated DiagnosticTag tags = 7;
    optional bytes data = 9;
}

message FigureDescriptor {
    optional string title = 1;
    string title_font = 2;
    string title_color = 3;

    int64 update_interval = 7 [jstype=JS_STRING];

    int32 cols = 8;
    int32 rows = 9;

    repeated ChartDescriptor charts = 10;

    // Deprecated: not set by the server anymore, this is replaced by the object fetch mechanism
    reserved 11;

    // Deprecated: not set by the server anymore, this is replaced by the object fetch mechanism
    reserved 12;

    repeated string errors = 13;

    message ChartDescriptor {
        enum ChartType {
            XY = 0;
            PIE = 1;
            OHLC = 2 [deprecated=true];
            CATEGORY = 3;
            XYZ = 4;
            CATEGORY_3D = 5;
            TREEMAP = 6;
        }
        int32 colspan = 1;
        int32 rowspan = 2;

        repeated SeriesDescriptor series = 3;
        repeated MultiSeriesDescriptor multi_series = 4;
        repeated AxisDescriptor axes = 5;

        ChartType chart_type = 6;

        optional string title = 7;
        string title_font = 8;
        string title_color = 9;

        bool show_legend = 10;
        string legend_font = 11;
        string legend_color = 12;

        bool is3d = 13;

        int32 column = 14;
        int32 row = 15;
    }

    enum SeriesPlotStyle {
        BAR = 0;
        STACKED_BAR = 1;
        LINE = 2;
        AREA = 3;
        STACKED_AREA = 4;
        PIE = 5;
        HISTOGRAM = 6;
        OHLC = 7;
        SCATTER = 8;
        STEP = 9;
        ERROR_BAR = 10;
        TREEMAP = 11;
    }

    message SeriesDescriptor {
        SeriesPlotStyle plot_style = 1;

        string name = 2;

        optional bool lines_visible = 3;
        optional bool shapes_visible = 4;

        bool gradient_visible = 5;
        string line_color = 6;
        //TODO (deephaven-core#774) finish this field or remove it from the DSL
        //    string line_style = 7;
        reserved 7;
        optional string point_label_format = 8;
        optional string x_tool_tip_pattern = 9;
        optional string y_tool_tip_pattern = 10;

        string shape_label = 11;
        optional double shape_size = 12;
        string shape_color = 13;
        string shape = 14;

        repeated SourceDescriptor data_sources = 15;
    }
    message MultiSeriesDescriptor {
        SeriesPlotStyle plot_style = 1;

        string name = 2;

        StringMapWithDefault line_color = 3;

        StringMapWithDefault point_color = 4;

        BoolMapWithDefault lines_visible = 5;

        BoolMapWithDefault points_visible = 6;

        BoolMapWithDefault gradient_visible = 7;

        StringMapWithDefault point_label_format = 8;

        StringMapWithDefault x_tool_tip_pattern = 9;

        StringMapWithDefault y_tool_tip_pattern = 10;

        StringMapWithDefault point_label = 11;

        DoubleMapWithDefault point_size = 12;

        StringMapWithDefault point_shape = 13;

        repeated MultiSeriesSourceDescriptor data_sources = 14;

    }
    message StringMapWithDefault {
        optional string default_string = 1;
        repeated string keys = 2;
        repeated string values = 3;
    }
    message DoubleMapWithDefault {
        optional double default_double = 1;
        repeated string keys = 2;
        repeated double values = 3;
    }
    message BoolMapWithDefault {
        optional bool default_bool = 1;
        repeated string keys = 2;
        repeated bool values = 3;
    }
    message AxisDescriptor {
        enum AxisFormatType {
            CATEGORY = 0;
            NUMBER = 1;
        }
        enum AxisType {
            X = 0;
            Y = 1;
            SHAPE = 2;
            SIZE = 3;
            LABEL = 4;
            COLOR = 5;
        }
        enum AxisPosition {
            TOP = 0;
            BOTTOM = 1;
            LEFT = 2;
            RIGHT = 3;
            NONE = 4;
        }

        string id = 1;

        AxisFormatType format_type = 2;

        AxisType type = 3;

        AxisPosition position = 4;

        bool log = 5;
        string label = 6;
        string label_font = 7;
        string ticks_font = 8;
        optional string format_pattern = 9;
        string color = 10;
        double min_range = 11;
        double max_range = 12;
        bool minor_ticks_visible = 13;
        bool major_ticks_visible = 14;
        int32 minor_tick_count = 15;
        optional double gap_between_major_ticks = 16;
        repeated double major_tick_locations = 17;
        double tick_label_angle = 18;
        bool invert = 19;
        bool is_time_axis = 20;
        BusinessCalendarDescriptor business_calendar_descriptor = 21;
    }
    message BusinessCalendarDescriptor {
        enum DayOfWeek {
            SUNDAY = 0;
            MONDAY = 1;
            TUESDAY = 2;
            WEDNESDAY = 3;
            THURSDAY = 4;
            FRIDAY = 5;
            SATURDAY = 6;
        }
        message BusinessPeriod {
            string open = 1;
            string close = 2;
        }
        message Holiday {
            LocalDate date = 1;
            repeated BusinessPeriod business_periods = 2;
        }
        message LocalDate {
            int32 year = 1;
            int32 month = 2;
            int32 day = 3;
        }
        string name = 1;
        string time_zone = 2;
        repeated DayOfWeek business_days = 3;
        repeated BusinessPeriod business_periods = 4;
        repeated Holiday holidays = 5;
    }
    message MultiSeriesSourceDescriptor {
        string axis_id = 1;

        SourceType type = 2;

        int32 partitioned_table_id = 3;
        string column_name = 4;
    }

    enum SourceType {
        X = 0;
        Y = 1;
        Z = 2;
        X_LOW = 3;
        X_HIGH = 4;
        Y_LOW = 5;
        Y_HIGH = 6;
        TIME = 7;
        OPEN = 8;
        HIGH = 9;
        LOW = 10;
        CLOSE = 11;
        SHAPE = 12;
        SIZE = 13;
        LABEL = 14;
        COLOR = 15;
        PARENT = 16;
        HOVER_TEXT = 17;
        TEXT = 18;
    }
    message SourceDescriptor {
        string axis_id = 1;

        SourceType type = 2;

        int32 table_id = 3;
        int32 partitioned_table_id = 4;
        string column_name = 5;

        string column_type = 6;

        OneClickDescriptor one_click = 7;
    }
    message OneClickDescriptor {
        repeated string columns = 1;
        repeated string column_types = 2;
        bool require_all_filters_to_display = 3;
    }
}
```

</details>

## Generate Python code from Protobuf

Once you have defined your Protobuf files, you can generate code for any of its supported languages. Doing so requires the [`protoc`](https://protobuf.dev/installation/) compiler, which compiles `proto` files into source code. Once you have `protoc` installed, you can generate Python code from Protobuf files via the following command:

```bash
protoc --proto_path=<PROTO_PATH> --python_out=<OUTPUT_DIR> <PROTO_FILE_1> <PROTO_FILE_2> ...
```

Where:

- `<PROTO_PATH>`: The path to the directory containing your `proto` files.
- `<OUTPUT_DIR>`: The directory where you want to save the generated Python code.
- `<PROTO_FILE_1>`, `<PROTO_FILE_2>`, ...: The `proto` files you want to compile.

## Related documentation

- [Connect to a Kafka stream](./data-import-export/kafka-stream.md)
- [Deephaven's design](../conceptual/deephaven-design.md)
- [The Deephaven Core API](../conceptual/deephaven-core-api.md)
- [Protobuf](https://protobuf.dev/)
- [gRPC](https://grpc.io/)
