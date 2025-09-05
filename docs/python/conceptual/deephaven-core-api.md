---
title: Designing the Deephaven Core API
sidebar_label: Core API design
---

<div className="comment-title">

Bringing incremental table updates to your data backplane

</div>

The Deephaven team has long been fascinated with two related software problems:

1. How to best serve use cases driven by real-time data, within a general purpose, table-oriented data system.

2. How to provide the efficient transport of real-time table-oriented data over the wire, both (a) between nodes of the system contemplated above and (b) between disparate systems, without the cost of serialization.

The first challenge catalyzed the development of Deephaven’s [incremental table update model](./table-update-model.md).

The second remained an open challenge until mid-2020. At that time, philosophies and solutions provided by [gRPC](https://grpc.io/) and [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) illuminated a compelling path forward.

## gRPC: A High Performance Universal RPC framework

[gRPC](https://grpc.io/) is a modern Remote Procedure Call (RPC) framework that is platform and environment agnostic. Open source communities have adopted gRPC with much success. Developers can define complex APIs using simple service definitions that are immediately transpilable to more than a dozen target languages supported officially and by community. Client and server implementations have the freedom to select the programming language that suits their use-case best.

Services can choose from a variety of request-response communication patterns:

| RPC Type                    | Number of Client Messages | Number of Server Messages |
| --------------------------- | ------------------------- | ------------------------- |
| Unary                       | 1                         | 1                         |
| Server-Streaming            | 1                         | None or Many              |
| Client-Streaming            | None or Many              | 1                         |
| Bidirectionally-Streaming\* | None or Many              | None or Many              |

_\* There is no framework restriction on whether the client or server controls the conversation._

## Apache Arrow: A Cross-Language Platform For Analytics

[Apache Arrow](https://arrow.apache.org/) (Arrow) defines a language-independent columnar memory format for flat and hierarchical data. The in-memory format is optimized for performing analytics on modern hardware (including GPUs). Arrow is widely adopted, yielding bountiful access to examples and support.

## Apache Arrow Flight: An Arrow IPC Framework

[Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) (Flight) provides an interprocess communication (IPC) model that is built around Arrow’s in-memory model and communicates over gRPC. Flight organizes communication around streams of Arrow record batches. Most Flight implementations intrinsically support zero-copy writes and zero-copy reads, removing unnecessary translation costs when shipping data from one server to another.

Flight’s simple service is defined as follows:

- `Handshake(stream Handshake): stream HandshakeResponse`

  An optional lightweight stream to support implementation specific authentication.

- `ListFlights(Criteria): stream FlightInfo`

  Request a list of available Flight Streams given the provided criteria.

- `GetFlightInfo(FlightDescriptor): FlightInfo`

  Request information about how the Flight can be consumed.

- `GetSchema(FlightDescriptor): SchemaResult`

  Get the Schema describing the columnar format, indicating the wire format of FlightData’s body.

- `DoGet(Ticket): stream FlightData`

  Retrieve a single stream of `FlightData` associated with the referenced ticket.

- `DoPut(stream FlightData): stream PutResult`

  Upload a stream of `FlightData` to the server.

- `DoExchange(stream FlightData): stream FlightData`

  Open a bidirectional data channel with the server. This enables client and server to send and receive arbitrary Arrow data in a single logical stream. Typically the purpose of the conversation is identified by attaching application specific metadata to the client’s initial request.

- `DoAction(Action): stream Result`

  Provides a mechanism for opaque request and result that can be offered by the server without deeper integration with gRPC.

- `ListActions(): stream ActionType`

  This enables application specific flight clients to understand the capabilities of one instance of a Flight server versus another.

### Incremental updates in Arrow Flight

Flight has been designed to maximize throughput for structured data sent via IPC. Deephaven brings a rich toolset for processing real-time table operations, driven by incremental updates, and therefore requires a means to describe table changes. Flight does not natively have a mechanism to describe incrementally changing datasets. It does, however, allow API implementers to provide their own metadata to augment the built-in data transfer capabilities.

Deephaven utilizes application metadata to incorporate an update model alongside the native Arrow payloads. This extension is called [Barrage](https://github.com/deephaven/barrage), as an homage to all that we’ve inherited from the Arrow team’s work.

## Enter the Deephaven Core API

The Deephaven Core API is composed of several complementary modules:

- Arrow Flight Service - high-performance data transport based on Arrow data.
- Table Service - a table manipulation API that mirrors Deephaven’s internal compute API.
- Session Service - management of a client’s session and resources.
- Console Service - support to drive a REPL from a client.
- Application Service - inspired by ListFlights, but exposes a wider variety of types.

The [Flight](#apache-arrow-flight-an-arrow-ipc-framework) and [Table](#doaction-via-the-table-service) services are the foundational elements of the Deephaven Core API.

## Introducing `BarrageUpdateMetadata`

> [!NOTE]
> This article does not provide comprehensive documentation or usage instructions. For more details, please see the [Barrage documentation](/barrage/docs).

The primary metadata object that Barrage uses to describe an incremental update is called `BarrageUpdateMetadata`. There are three sub-problems that need to be solved to orchestrate a sufficient solution to the larger problem at hand.

- First, there must be a model, agnostic of transport, that solves the more general concepts around incremental updates.
- Second, there must be a way to partition large payloads into smaller ones to support the very real limits imposed by the details of any gRPC targeted programming language.
- Last, there must be a mechanism to efficiently support client applications that are driven by human interaction.

### Incremental updates

The subset of `BarrageUpdateMetadata`’s fields aimed at solving this sub-problem are:

```flatbuffer
added_rows: [byte];
removed_rows: [byte];
shift_data: [byte];
added_rows_included: [byte];
mod_column_nodes: [BarrageModColumnMetadata];
```

The details of this sub-problem, including [Deephaven’s approach to incremental updates](./table-update-model.md), are out of scope for this article. Deephaven’s model, developed to address real-world use cases in production deployments, is unquestionably a unique and empowering approach to building modern data-driven systems. This design has been applied in a variety of contexts, from driving complex multi-million dollar options market-making strategies to simulating the behavior of cryptographic networks. Read [our introduction to Deephaven](./deephaven-overview.md) for a discussion of what users of Deephaven have accomplished under the incremental update model.

### Transport framing

The subset of `BarrageUpdateMetadata`’s fields aimed at solving this sub-problem are:

```flatbuffer
num_add_batches: ushort;
num_mod_batches: ushort;
```

Flight’s `DoGet` and `DoExchange` RPCs are structured as a stream of `FlightData`. `FlightData` that contains an Arrow payload will be referred to as a `RecordBatch`. The ordering of `RecordBatch`es is not necessarily meaningful at Flight’s service definition layer, but it isn’t unreasonable for integrations to define and declare an explicit ordering. `DoGet` and `DoExchange` return a stream of `FlightData` to enable partitioning large data sets into smaller working sets. Barrage uses this feature in order to support sending multiple record batches, initially for snapshots, and subsequently for updated data.

Barrage breaks each periodic incremental update payload into a sequence of `RecordBatch`es. The first `RecordBatch` contains the `BarrageUpdateMetadata` payload. This metadata contains `num_add_batches` and `num_mod_batches`, which are needed to reconstruct and apply the incremental update. This count includes the payload attached to the provided metadata. All added row `RecordBatch`es are sent prior to modified row `RecordBatch`es. Both of these counts may be zero depending on what the server is trying to communicate.

### Data view framing

The subset of `BarrageUpdateMetadata`’s fields aimed at solving this sub-problem are:

```flatbuffer
  first_seq: long;
  last_seq: long;

  is_snapshot: bool;
  effective_viewport: [byte];
  effective_column_set: [byte];
```

Barrage is designed to support two primary use cases:

1. The first use case targets server-to-server processing, where the client will want to subscribe to, and maintain, the entire state of the source table.
2. The second use case targets user-facing interactive applications, where the user only needs the data that can be shown on the screen (afterall, there are physical limits that make scroll-bars convenient).

Barrage describes a first-class concept called a Viewport. A viewport is a window over the source table described by ranges of row positions and a set of columns. This maps one-to-one to the data that the user can interact with (directly or indirectly).

A common problem with IPC transports is the asynchronous nature of client-request vs server-response (or vice versa). To minimize the total payload sent from the server to the client, the server expects the client to maintain the state that they have subscribed to. When a client requests a viewport change, the client needs to know when the server has begun to respect that request. Additionally, if a client quickly changes its mind (e.g., the user wasn’t done scrolling), the server assumes the client doesn’t want to be notified of data changes that are no longer in the most recently requested viewport.

When a client initiates or changes its subscription, the server sends a payload with the metadata parameter `is_snapshot` set to `true`. Only when this parameter is true are `effective_viewport` and `effective_column_set` included on the payload. This enables the client to keep track of all data that is within the server-respected viewport. A snapshot will be sent whether or not it includes additional Arrow content (e.g., reducing the size of the requested viewport). The server will not resend Arrow content that overlaps between the previous viewport and the new viewport.

Often there is a different granularity of incremental updates that are desired when sending payloads over the network compared to within the same process. We typically prefer many small updates over any alternative; however, busily-updated data sets may be a source of performance issues. When transporting updates over a network, the frequency and size of updates become an even more immediate concern: machine-to-machine communication is more limited than CPU to RAM communication. Barrage subscriptions aggregate incremental updates to help reduce this noise. This update interval can be tuned by default (via JVM property `barrage.updateInterval`), just as the Deephaven internal engine’s tick frequency can be tuned (via JVM property [`PeriodicUpdateGraph.targetCycleDurationMillis`](/core/javadoc/io/deephaven/engine/updategraph/impl/PeriodicUpdateGraph.html)), as well as explicitly configured on the initial subscription request (via flatbuffer field `BarrageSubscriptionRequest.update_interval_ms`).

The payload parameters `first_seq` and `last_seq` can be a useful aid to those who attempt to reconcile differences between server and client state (an uncommon task, but useful for client implementation debugging).

## Javascript support

You may have noticed that even Deephaven’s web user-interface processes incremental updates. The Deephaven Javascript Client is a true gRPC client and receives `FlightData` payloads. However, browsers do not support client-streaming over HTTP/2 at this time. To enable client-streaming-like capabilities, Deephaven simulates the transport level concepts by splitting bidirectional streams (e.g., `DoExchange`) into a server-streaming request followed by a series of unary-requests. The client attaches HTTP2 headers to identify the simulated stream, the sequence, and whether or not it is done sending data. The server then provides ordered delivery to, and reuses, the server-streaming implementation.

## `DoAction` via the Table Service

Whereas we could have used Flight’s `DoAction` to send a command to the server telling it to sort Table `X` by column `Y` and filter column `A` by value `B`, the Deephaven team felt that it didn’t give enough structure to the queries that Deephaven supports.

The Deephaven Core API Table service enables a user to build an entire directed-acyclic-graph (DAG) of requests in a single batch gRPC call to the server. Being able to define and ship your application’s logic to the server is extremely powerful. Client or server implementations may refactor the DAG payload for optimization or canonicalization purposes.

All of the manipulations that you see Deephaven’s browser-based client perform are delegated to the server. This offloads heavy workload from the user’s machine, while maintaining the illusion that their data is right there, at their fingertips.

The Table Service was written to truly complement the Flight Service. Every Deephaven query engine operation may depend on one or more upstream tables. Any dependent tables, and the resulting table, are identified with Flight tickets. The user is able to refer to these tickets in subsequent requests, including requests to the Flight Service. If a required upstream operation has not yet completed, the server waits for pending work to be performed before attempting to execute the subsequent operation. When the user is done with a result, they simply release the ticket. When all operations (including queued operations!) that depend on that ticket no longer require access to its resources, then those resources are freed up.

Adding yet more value, a user who has full access to the deployment may implement their own `TicketResolver` to integrate any other existing source of data with the Deephaven Core API. For example, one could create a `TicketResolver` that parameterizes a SQL template, fetches that data from a SQL server, and exports a static Deephaven Table backed by this result.

## Get started

If you are brand new to Deephaven:

- Try out the REPL experience on our hosted demo site. [Coming Soon]
- Run a dockerized version locally by following the [quick start](../getting-started/docker-install.md) guide.

If you want to try out any of our work-in-progress client libraries:

- Try out the [Python client](/core/client-api/python/) library.
- Try out the Java client library. [Documentation coming soon!]
- Try out the C++ client library. [Documentation coming soon!]
- Try out the Javascript client library. [Documentation coming soon!]
- Check out the [gRPC service definitions](https://github.com/deephaven/deephaven-core/tree/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto) for even more self-help fun.

If you have an existing Flight Client:

- `ListFlights` will describe Tables bound to variables in the REPL and bound via [Application Mode](../how-to-guides/application-mode.md).
- `DoGet` will fetch a snapshot of the entire Table (unsupported columns will be stringified).
- `DoPut` can be used to upload your own static content.
- `DoExchange` can be used to subscribe to incremental updates.

We intend to [integrate with Flight’s Auth](https://github.com/deephaven/deephaven-core/issues/997), but at this time you will need to integrate with the session management as described by the [gRPC definition](https://github.com/deephaven/deephaven-core/blob/main/proto/proto-backplane-grpc/src/main/proto/deephaven_core/proto/session.proto).

`DoPut`, as of this writing, requires providing an Arrow Schema metadata tag with the key `deephaven:type` for each uploaded column. Nested columns are not yet supported. Note that we do not yet support the entire suite of Arrow data types. At this time, we refer you to the [source](https://github.com/deephaven/deephaven-core/blob/1c5012626a1a6064382a97dddbc65c547de49715/grpc-api/src/main/java/io/deephaven/grpc_api/barrage/util/BarrageSchemaUtil.java#L182) to describe what the server expects and supports. These details will change as we improve our overall support for Apache Arrow, so please search for this metadata key in the source to find the most up-to-date details.

`DoExchange` requires that you understand our [update model](./table-update-model.md) as well as the wire formats of the metadata required to materialize the resulting state after applying incremental updates. If this interests you, the [Barrage](/barrage/docs) documentation is the most appropriate place to dip your feet.

## Contribute back to Arrow Flight

The Deephaven Community Core team has engaged with the Arrow team to improve incremental-update support and efficiency. The team looks forward to working together on the following project ideas:

- An asynchronous callback pattern for Flight users in Java/C++.
- A way to describe a `RecordBatch` that supports skipping empty columns.
- A way to express that columns in a `RecordBatch` may differ in length.
- Our solution to client-streaming requests for browser clients, which might be generally applicable.

[Let us know](https://github.com/deephaven/deephaven-core/discussions) what else you think the team can contribute!

## Related documentation

- [Deephaven Overview](./deephaven-overview.md)
- [Table update model](./table-update-model.md)
- [Deephaven Barrage](https://github.com/deephaven/barrage)
- [How to run scripts before launch with Application Mode](../how-to-guides/application-mode.md)
