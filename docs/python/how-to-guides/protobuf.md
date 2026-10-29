---
title: Protocol Buffers and remote procedure calls
---

[Protobuf](https://protobuf.dev/), short for Protocol Buffers, is a language-neutral, platform-neutral mechanism for serializing structured data. You define how data is structured once, and Protobuf generates source code from that definition in multiple languages. Programs in each language use that code to serialize and deserialize the data consistently across different systems.

Protobuf serializes data to a compact binary format, which is faster to encode and decode than text-based formats like JSON or XML. That efficiency matters for Deephaven, which is designed to handle large amounts of data in real time. The requests and responses that drive a client's session stay small and fast. The table data itself travels in Arrow's columnar format, as described [below](#protobuf-and-grpc-in-deephaven).

Deephaven's client APIs talk to the server through [gRPC](https://grpc.io/), Google's open-source remote procedure call (RPC) framework. gRPC services are defined in Protobuf, so this guide covers both. Protobuf is one of several options for users who wish to build their own Deephaven client API. If you build your own client, these service definitions are what you program against.

This guide explains what Protobuf and RPC are and what they do. It walks through two Deephaven `.proto` (Protobuf) files and shows how to generate Python code from them. Then it uses that code to authenticate with a Deephaven server and run code there.

Most Python users don't need to generate code. The [`pydeephaven`](/core/client-api/python/code/pydeephaven.html) package already includes it for every Deephaven service. Generate code yourself when you write your own `.proto` definitions or build a client without `pydeephaven`. For more detailed and specific guidance on Protobuf and gRPC, see:

- [Protobuf programming guides](https://protobuf.dev/programming-guides/)
- [gRPC documentation](https://grpc.io/docs/)

## Protobuf and gRPC in Deephaven

gRPC uses Protobuf to describe its services and the messages they exchange, so programs written in different languages can call each other. This matters for Deephaven, which is designed to work with multiple languages and platforms. For example, the [Python client API](../getting-started/pyclient-quickstart.md), written in Python, communicates with the Deephaven server, written in Java.

gRPC and Protobuf let you use multiple client APIs with the same Deephaven server. For example, you can use the [Java client API](./java-client.md) to connect to a Deephaven server and run queries while you use the [Python client API](../getting-started/pyclient-quickstart.md) to check the results of those queries.

Table data itself moves between servers and clients over [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) and Deephaven's [Barrage](../conceptual/what-is-barrage.md) protocol, both of which are built on gRPC. gRPC's support for bidirectional streaming is a key feature of Deephaven's architecture and design. Barrage uses it for subscriptions. A client sends its subscription request, and the server streams the ticking table's updates back on the same call.

## Protobuf concepts

The concepts in this section belong to Protobuf itself. You can also use Protobuf without gRPC. For example, Deephaven can [read Kafka topics whose payloads are Protobuf-encoded](./data-import-export/kafka-stream.md#read-kafka-topic-in-protobuf-format).

### Messages

[Messages](https://protobuf.dev/reference/protobuf/proto3-spec/#message_definition) are structured data types that define schemas for serialized data. Similar to classes and structs, these contain fields with names, types, and unique numbers used for encoding and decoding.

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

[Enums](https://protobuf.dev/reference/protobuf/proto3-spec/#enum_definition) are a way to define a set of named values. They are useful for defining a fixed set of options for a field in a message. To define an enum, write the `enum` keyword followed by the enum's name and its values.

The following example defines an enum called `PhoneType` with four values: `PHONE_TYPE_UNSPECIFIED`, `MOBILE`, `HOME`, and `WORK`. In proto3, an enum's first value must be `0`. It's also the value a field has when it isn't set, so the example reserves `0` for "unspecified". The `PhoneNumber` message then declares a field of type `PhoneType`, so each phone number records which kind of number it is.

```protobuf
syntax = "proto3";

enum PhoneType {
    PHONE_TYPE_UNSPECIFIED = 0; // Value 0: No type set
    MOBILE = 1;                 // Value 1: Mobile phone
    HOME = 2;                   // Value 2: Home phone
    WORK = 3;                   // Value 3: Work phone
}

message PhoneNumber {
    string number = 1;  // Field 1: The phone number
    PhoneType type = 2; // Field 2: Which kind of number it is
}
```

## RPC concepts

RPC lets a program call a procedure on a remote system as if it were local. In gRPC, Protobuf defines the structure of each call's request and response and serializes them.

### Services

[Services](https://protobuf.dev/reference/protobuf/proto3-spec/#service_definition) define a set of RPC methods that a client can call remotely. To define a service, write the `service` keyword followed by the service's name and its methods.

The following example defines a service called `Greeter` with one RPC method, `SayHello`, which takes a `HelloRequest` message containing a name and returns a `HelloResponse` message containing a greeting.

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
> It's best practice to define separate messages for requests and responses, as the `HelloRequest` and `HelloResponse` messages in the example above do. Separate single-argument request and response messages let an API evolve without breaking backward compatibility. For example, extending an API by including additional data in the request or response messages won't break existing servers or clients running old code.

## Read a Deephaven `.proto` file: input tables

To illustrate how Deephaven uses Protobuf, consider the Protobuf definition for an [input table](./input-tables.md). [Input tables](./input-tables.md) are tables that users can add data to, either through the UI or programmatically. A client API adds data programmatically, as shown in [Send data to Deephaven from a Python client](./client-input-tables.md).

The following excerpt from [`inputtable.proto`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/inputtable.proto) on GitHub shows the service and its request and response messages. The full file also defines validation-error, column-metadata, and column-restriction messages, which the excerpt omits.

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
import "google/protobuf/any.proto";

/*
 * This service offers methods to manipulate the contents of input tables.
 */
service InputTableService {
    /*
     * Adds the provided table to the specified input table. The new data to add must only have
     * columns (name, types, and order) which match the given input table's columns.
     *
     * If the data is invalid, an error response will be returned with details containing an
     * InputTableValidationErrorList.
     */
    rpc AddTableToInputTable(AddTableRequest) returns (AddTableResponse) {}

    /*
    * Removes the provided table from the specified input tables. The tables indicating which rows
    * to remove are expected to only have columns that match the key columns of the input table.
    *
    * If the data is invalid, an error response will be returned with details containing an
    * InputTableValidationErrorList.
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

The following sections break this example into its components.

### Syntax, packages, options, and imports

The `syntax` statement at the top of `inputtable.proto` specifies the version of the Protobuf language the file uses. In this case, it specifies `proto3`.

Just below, the `package` statement prevents name clashes between protocol message types. In this case, the file's types belong to the `io.deephaven.proto.backplane.grpc` package, so they don't clash with types of the same name in other packages.

The `option` statements control how the Protobuf compiler, [`protoc`](https://protobuf.dev/installation/), generates code from the file. In this case, the options specify:

- `java_multiple_files`: Generates a separate Java file for each top-level message, enum, and service instead of nesting them in one outer class.
- `optimize_for`: Optimizes the generated code for speed.
- `go_package`: Specifies the Go import path for the generated code.

An `import` statement lets this file use the definitions in another `.proto` file. The file has two imports:

- `deephaven_core/proto/ticket.proto` defines the [`Ticket`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/ticket.proto) message type. A [`Ticket`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/ticket.proto) is an opaque identifier that the server uses to find an object it holds, such as a table. The input table requests use [`Ticket`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/ticket.proto) fields to name the tables involved.
- `google/protobuf/any.proto` is one of Protobuf's standard definitions. The omitted metadata messages use it.

### `InputTableService`

The [`InputTableService`](/core/client-api/protobuf/) service defines the methods a client can call remotely. In this case, it defines two methods:

- `AddTableToInputTable`: Adds the rows of a table to an [input table](./input-tables.md). In a keyed input table, an added row whose key matches an existing row replaces that row. It accepts an `AddTableRequest` and returns an `AddTableResponse`.
- `DeleteTableFromInputTable`: Removes rows from a keyed [input table](./input-tables.md). [Input tables](./input-tables.md) without key columns, such as [append-only](../conceptual/table-types.md#specialization-1-append-only) and [blink](../conceptual/table-types.md#specialization-3-blink) input tables, reject it. It accepts a `DeleteTableRequest` and returns a `DeleteTableResponse`.

### Request and response messages

The `InputTableService` service uses four different messages:

- `AddTableRequest`: The request message for the `AddTableToInputTable` method. Its two [`Ticket`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/ticket.proto) fields name the tables involved. `input_table` is the [input table](./input-tables.md), and `table_to_add` holds the rows to add. The client first uploads those rows under an export ticket it chooses, for example with an [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) `DoPut`. It then passes that ticket as `table_to_add`.
- `AddTableResponse`: The response message for the `AddTableToInputTable` method. It has no fields. Receiving it tells the client that the request succeeded.
- `DeleteTableRequest`: The request message for the `DeleteTableFromInputTable` method. It contains two fields of type [`Ticket`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/ticket.proto). `input_table` identifies the [input table](./input-tables.md), and `table_to_remove` identifies a table of keys for the rows to remove, uploaded the same way. The key table must contain every key column of the [input table](./input-tables.md) and no other columns. The server deletes the rows whose keys match.
- `DeleteTableResponse`: The response message for the `DeleteTableFromInputTable` method. It has no fields. Receiving it tells the client that the request succeeded.

If a request fails, the server returns a gRPC error instead of a response message. When an `AddTableToInputTable` request fails validation on specific values, the error's details contain an `InputTableValidationErrorList`, one of the messages the excerpt omits. It describes each error, including its row and column when known. A failed `DeleteTableFromInputTable` request returns an error message without these details, even though the file's comment on that method says otherwise.

### What the file defines

The `inputtable.proto` file defines a service that lets a client API add rows to [input tables](./input-tables.md) and remove rows from keyed [input tables](./input-tables.md). The [`protoc`](https://protobuf.dev/installation/) compiler can [generate code from it](#generate-python-code-from-protobuf) in many languages, and a client written in any of them can use that code to send these requests to a Deephaven server.

## Streaming RPCs: the console service

Deephaven's [`ConsoleService`](/core/client-api/protobuf/) is the gRPC service behind the web IDE's code console. The [console service guide](./configuration/console-service.md) shows how to configure it. `ConsoleService` is more complex than the input table service, which only adds and removes rows with simple request/response calls. `ConsoleService` lets a client run commands on the server, cancel them, check heap usage, use autocomplete, subscribe to logs, and more.

The following excerpt shows the `ConsoleService` service definition. See the full [`console.proto`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/console.proto) on GitHub for the messages it uses.

```protobuf
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
```

The `stream` keyword marks a request or response as a stream of messages instead of a single message. Where `stream` appears in a method sets the method's kind. `ConsoleService` uses three kinds of RPC method:

- **Unary** (no `stream`): The client sends one request and receives one response. Most of the service's methods are unary. For example, a client calls `ExecuteCommand` with code to run. Once the code finishes, the server responds with any error message and the variables the code changed.
- **Server streaming** (`returns (stream …)`): The client sends one request, and the server sends back a stream of responses. `SubscribeToLogs` returns a stream of `LogSubscriptionData` messages, one for each log entry.
- **Bidirectional streaming** (`stream` on both the request and the response): The client and server each send a stream of messages over one call. With `AutoCompleteStream`, the client sends an `AutoCompleteRequest` message each time the user opens, edits, or closes a document. It also sends one to ask for completions, signature help, hover text, or diagnostics. The server sends `AutoCompleteResponse` messages back on the same call.

gRPC also supports client streaming (`stream` on the request only), which `ConsoleService` doesn't use.

Browsers can't open bidirectional gRPC streams without WebSockets, so the service also offers a two-part alternative to `AutoCompleteStream`. `OpenAutoCompleteStream` opens a server stream of responses, and `NextAutoCompleteStream` sends each later request as a separate unary call.

## Generate Python code from Protobuf

[`protoc`](https://protobuf.dev/installation/) can generate code from a `.proto` file for any language it supports. A gRPC client needs two kinds of generated code: classes for the messages, and a stub for each service that calls its RPC methods.

You don't need to generate code to use Deephaven's own services from Python. The [`pydeephaven`](/core/client-api/python/code/pydeephaven.html) package already includes generated code for every Deephaven `.proto` file. The code is in the `deephaven_core.proto` package. For example, the input table service stub is `deephaven_core.proto.inputtable_pb2_grpc.InputTableServiceStub`.

To generate the code yourself, install the [`grpcio-tools`](https://pypi.org/project/grpcio-tools/) package, which includes the [`protoc`](https://protobuf.dev/installation/) compiler and the gRPC Python plugin:

```bash
pip install grpcio-tools
```

Then run [`protoc`](https://protobuf.dev/installation/) through the package's `grpc_tools.protoc` module. The following command generates the message classes (`--python_out`) and the service stubs (`--grpc_python_out`) for the input table example above and the `ticket.proto` file it imports:

```bash
python -m grpc_tools.protoc -I=<PROTO_ROOT> --python_out=<OUTPUT_DIR> --grpc_python_out=<OUTPUT_DIR> <PROTO_ROOT>/deephaven_core/proto/ticket.proto <PROTO_ROOT>/deephaven_core/proto/inputtable.proto
```

Where:

- `<PROTO_ROOT>`: The directory that contains the `deephaven_core` directory. In a clone of the [deephaven-core repository](https://github.com/deephaven/deephaven-core), this is `proto/proto-backplane-grpc/src/main/proto`.
- `<OUTPUT_DIR>`: An existing directory where you want to save the generated Python code.

Deephaven's `.proto` files import each other by paths that start at `<PROTO_ROOT>`, such as `deephaven_core/proto/ticket.proto`. If `-I` points at the `deephaven_core/proto` directory instead, `protoc` can't resolve those imports.

The generated modules import each other as `deephaven_core.proto.<name>_pb2`, so add `<OUTPUT_DIR>` to your Python path (for example, with `PYTHONPATH`). Don't install the result alongside [`pydeephaven`](/core/client-api/python/code/pydeephaven.html), which provides its own `deephaven_core` package.

To generate code for other Deephaven services, add their `.proto` files, and the files they import, to the end of the command.

## Call a Deephaven service from Python

This section puts the pieces together. It generates code for `ConsoleService`, authenticates with a Deephaven server, and runs code there. The code it runs creates a ticking table, which keeps updating on the server like any Deephaven table. Every call uses only the generated code and the `grpcio` package, with no [`pydeephaven`](/core/client-api/python/code/pydeephaven.html).

The example needs a Deephaven server that uses [pre-shared key (PSK) authentication](./authentication/auth-psk.md). The examples use the key `my-secret-psk`. To run a server with that key in Docker:

```bash
docker run --rm -p 10000:10000 -e START_OPTS="-Dauthentication.psk=my-secret-psk" ghcr.io/deephaven/server:latest
```

The server must also allow console code execution, which it does by default. See [Configure the console service](./configuration/console-service.md).

### Generate the console service code

`console.proto` imports `ticket.proto` and `application.proto`, so generate all three. Run these commands from the directory where you want the `generated` folder:

```bash
pip install grpcio grpcio-tools
mkdir generated
python -m grpc_tools.protoc -I=<PROTO_ROOT> --python_out=generated --grpc_python_out=generated <PROTO_ROOT>/deephaven_core/proto/ticket.proto <PROTO_ROOT>/deephaven_core/proto/application.proto <PROTO_ROOT>/deephaven_core/proto/console.proto
```

Run the Python code in the following steps as one script, with `generated` on the Python path. For example, run `PYTHONPATH=generated python client.py`.

### Authenticate with a pre-shared key

A client authenticates by sending an `authorization` header with its first call. For PSK authentication, the header's value is the authentication handler's class name, a space, and the key. The server checks the key and creates a session. It then returns a bearer token in the response's own `authorization` header. Every later call sends that token instead of the key.

The following code opens a channel, creates a `ConsoleService` stub, and makes a first call with the key. `GetConsoleTypes` is a simple unary call that returns the server's script language. The code then reads the bearer token from the response headers.

<!-- This test is skipped because it needs Python code generated from the Deephaven .proto files and a running Deephaven server with PSK authentication. -->

```python skip-test
import grpc
from deephaven_core.proto import console_pb2, console_pb2_grpc, ticket_pb2

channel = grpc.insecure_channel("localhost:10000")
console = console_pb2_grpc.ConsoleServiceStub(channel)

psk = "io.deephaven.authentication.psk.PskAuthenticationHandler my-secret-psk"
response, call = console.GetConsoleTypes.with_call(
    console_pb2.GetConsoleTypesRequest(),
    metadata=[("authorization", psk)],
)
print(response.console_types)

bearer = dict(call.initial_metadata())["authorization"]
auth = [("authorization", bearer)]
```

The server can rotate the token. Each response carries the current token in its `authorization` header. A client that runs longer than the session timeout must keep sending the newest one, as [`pydeephaven`](/core/client-api/python/code/pydeephaven.html) does. Older clients called `SessionService.NewSession` instead. `session.proto` marks that method deprecated in favor of these headers or Flight's handshake.

### Start a console

A console is a script session on the server. `StartConsole` takes a ticket for the new console and the console's language. These are its messages in [`console.proto`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/console.proto):

```protobuf
message StartConsoleRequest {
    io.deephaven.proto.backplane.grpc.Ticket result_id = 1;
    string session_type = 2;
}
message StartConsoleResponse {
    io.deephaven.proto.backplane.grpc.Ticket result_id = 1;
}
```

The client chooses the ticket. An export ticket is the byte `e` followed by a positive export number, written as a 4-byte little-endian integer. Each new export in a session needs a new number. This example creates only one export, so it uses `1`.

<!-- This test is skipped because it needs Python code generated from the Deephaven .proto files and a running Deephaven server with PSK authentication. -->

```python skip-test
export_id = 1
console_ticket = ticket_pb2.Ticket(
    ticket=b"e" + export_id.to_bytes(4, "little", signed=True)
)

started = console.StartConsole(
    console_pb2.StartConsoleRequest(result_id=console_ticket, session_type="python"),
    metadata=auth,
)
```

### Run code on the server

`ExecuteCommand` runs code in the console and responds once the code finishes. Its response reports any error and the variables the code changed:

```protobuf
message ExecuteCommandResponse {
    string error_message = 1;
    io.deephaven.proto.backplane.grpc.FieldsChangeUpdate changes = 2;

    // Start timestamp for command execution, in nanoseconds since the unix epoch.
    int64 start_timestamp = 3 [jstype=JS_STRING];
    // End timestamp for command execution, in nanoseconds since the unix epoch.
    int64 end_timestamp = 4 [jstype=JS_STRING];
}
```

The following code creates a ticking table with [`time_table`](../reference/table-operations/create/timeTable.md) and prints a message on the server. It then prints the name and type of each variable the code created.

<!-- This test is skipped because it needs Python code generated from the Deephaven .proto files and a running Deephaven server with PSK authentication. -->

```python skip-test
code = """
from deephaven import time_table
ticking = time_table("PT1s").update("X = ii * 2")
print("Created a ticking table")
"""

result = console.ExecuteCommand(
    console_pb2.ExecuteCommandRequest(console_id=started.result_id, code=code),
    metadata=auth,
)
print(result.error_message or "No error")
for field in result.changes.created:
    print(field.field_name, field.typed_ticket.type)
```

### Stream the server's output

`SubscribeToLogs` is a server-streaming call. The server first replays the log entries it has buffered, then sends new entries as they arrive. The following code asks for `STDOUT` entries only, prints the first one, and then cancels the stream and closes the channel.

<!-- This test is skipped because it needs Python code generated from the Deephaven .proto files and a running Deephaven server with PSK authentication. -->

```python skip-test
logs = console.SubscribeToLogs(
    console_pb2.LogSubscriptionRequest(levels=["STDOUT"]),
    metadata=auth,
)
for entry in logs:
    print(entry.log_level, entry.message.strip())
    break

logs.cancel()
channel.close()
```

### Output

The script prints the server's console type, the result of the command, the table it created, and the line the command printed:

```text
['python']
No error
ticking Table
STDOUT Created a ticking table
```

The `ticking` table now updates once a second on the server. To read its rows, a client uses [Arrow Flight and Barrage](../conceptual/what-is-barrage.md) instead of `ConsoleService`.

The session lasts until its token expires. When a session ends, the server releases everything the session exported. Variables that your code created, such as `ticking`, aren't exports. They stay in the server's script session after your session ends. To end your session right away, call `CloseSession` on the `SessionService` defined in [`session.proto`](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/session.proto).

For a complete client, read how [`pydeephaven`](/core/client-api/python/code/pydeephaven.html) calls these services. For example, its input table support calls `InputTableServiceStub.AddTableToInputTable` ([source](https://github.com/deephaven/deephaven-core/blob/main/py/client/pydeephaven/_input_table_service.py)).

## Related documentation

- [Deephaven's design](../conceptual/deephaven-design.md)
- [The Deephaven Core API](../conceptual/deephaven-core-api.md)
- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Create and use input tables](./input-tables.md)
- [Configure the console service](./configuration/console-service.md)
- [Configure and use pre-shared key authentication](./authentication/auth-psk.md)
- [Python client quickstart](../getting-started/pyclient-quickstart.md)
- [Send data to Deephaven from a Python client](./client-input-tables.md)
- [Extract data with a Java client](./java-client.md)
- [Connect to a Kafka stream](./data-import-export/kafka-stream.md)
- [`pydeephaven` API reference](/core/client-api/python/code/pydeephaven.html)
- [Deephaven Protobuf API reference](/core/client-api/protobuf/)
- [Protobuf](https://protobuf.dev/)
- [gRPC](https://grpc.io/)
