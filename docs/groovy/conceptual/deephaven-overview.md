---
title: Deephaven overview
description: "Deephaven is a query engine and software stack centered on 'live dataframes,' a groundbreaking abstraction: tables whose rows update as their source data changes. It lets you compose queries like building blocks and apply the same operations to real-time and batch data."
hide_title: true
---

import { QuickstartCTA } from '@theme/deephaven/CTA';

<div className="padding-top--xl" />

# Deephaven overview

<div className="text--warning padding-top--xl">

## What is Deephaven?

</div>

Deephaven is a query engine and software stack centered on "live dataframes," a groundbreaking abstraction: tables whose rows update as their source data changes. You write queries with familiar patterns from batch tools like SQL, pandas, Polars, and R. Like a query plan in SQL or a lazy query in Polars, a Deephaven query forms a [directed acyclic graph (DAG)](./dag.md). Unlike a batch system, Deephaven moves each change ("delta") through that DAG, so downstream results update incrementally without any change-tracking code from you.

Live dataframes give Deephaven two properties that set it apart: composability and range.

### Composability

In Deephaven, a query's output tables stay live, so another query can use them as inputs and keep updating as they change. That query can run in the same engine or in [another engine across the network](../how-to-guides/use-uris.md). This "building block" approach lets you connect real-time microservices, unlocking countless new use cases for real-time data.

### Range

Deephaven bridges real-time and batch data, letting you apply the same operations to both and combine them in one query. Its range extends across the stack:

- First-class interfaces in Python, Groovy, and Java, plus experimental [SQL support](../how-to-guides/data-import-export/execute-sql-queries.md).
- Calls from queries to Java libraries and to your own Groovy and Java functions on real-time data, without first copying it into a static snapshot.
- Calls from Python sessions to popular Python libraries, such as [PyTorch](/core/docs/how-to-guides/use-pytorch/) and [TensorFlow](/core/docs/how-to-guides/use-tensorflow/), on live tables, with results that update as the data changes.
- Wire protocols, such as [Barrage](./what-is-barrage.md), that let clients and other engines subscribe to a query's live tables and build on them across the network.
- [gRPC](https://grpc.io/)-based client APIs in Python, Java, C++, C#, JavaScript, R, and Go (see the [Community subsystems](#whats-the-difference-between-the-community-and-enterprise-stacks) list for each) for building and running queries on a server across the network.
- Front-end tools for real-time data:
  - The [JavaScript API](/core/client-api/javascript/modules/dh.html), for building your own real-time web front ends.
  - Browser-based grid and plotting components that let you scroll through and manipulate tables with billions of records.
  - [Deephaven Express](/core/plotly/docs/), for real-time plotting in Python sessions.
  - [deephaven.ui](/core/ui/docs/), a Python library for building real-time dashboards in Python sessions.
- Excel and [Jupyter](/core/docs/how-to-guides/jupyter/) integrations that bring Deephaven's server-side capabilities and live updates to these tools.

Because the stack covers ingestion from many [data sources](#what-are-the-data-sources), computation, and display, it empowers teams to focus on business problems instead of getting bogged down in utility engineering.

<div className="text--info padding-top--xl">

## What can you use Deephaven for?

</div>

Deephaven fits streaming, batch, and hybrid analytics and applications. People turn to Deephaven when their work involves:

- Integrating real-time and dynamic data into applications or analytics.
- Performing [time-series](../how-to-guides/joins-timeseries-range.md) and [relational](../how-to-guides/joins-exact-relational.md) operations on large [Iceberg](../how-to-guides/data-import-export/iceberg.md) or [Parquet](../how-to-guides/data-import-export/parquet-import.md) datasets, [Kafka](../how-to-guides/data-import-export/kafka-stream.md) streams, or combinations of these.
- Running [SQL queries](../how-to-guides/data-import-export/execute-sql-queries.md) on live tables, with results that update as the data changes.
- Running custom Groovy, Python, or Java code alongside table operations.
- Building client applications in C++ and other languages.
- [Linking multiple real-time query engines](../how-to-guides/use-uris.md) for parallel processing, pipelining, or microservice-driven analytical tasks.

Deephaven's engine supports everything from simple transformations to machine learning and autonomous applications. The stack also supports large-scale data distribution, connected applications, and advanced real-time visualizations and dashboards.

Deephaven is the perfect fit for workloads in IoT, personalization, capital markets, social media, blockchain, crypto, gaming, e-commerce, industrial telemetry, power and energy, and healthcare. The examples below show what Deephaven's capabilities look like in practice.

### Example use cases

- _Streaming analytics_: Connect to [Kafka](../how-to-guides/data-import-export/kafka-stream.md) streams and run [relational](../how-to-guides/joins-exact-relational.md) and [time-series](../how-to-guides/joins-timeseries-range.md) joins, [aggregations](../how-to-guides/dedicated-aggregations.md), and calculations that call your own functions and classes. Publish the derived, updating data downstream.
- _Device telemetry_: Link IoT devices to Deephaven for real-time summaries, [aggregations](../how-to-guides/dedicated-aggregations.md), visualizations, interactions, and alerts.
- _Batch data exploration_: Analyze [Parquet](../how-to-guides/data-import-export/parquet-import.md) files in S3 or other cloud storage with responsive tools on the Deephaven web UI or embedded real-time [Jupyter](/core/docs/how-to-guides/jupyter/) widgets.
- _Data science at scale_: In Python sessions, use Python libraries directly on updating tables or through snapshots mapped to [NumPy](/core/docs/how-to-guides/use-numpy/) and [pandas](/core/docs/how-to-guides/use-pandas/).
- _Real-time alerts_: Integrate with the [JavaScript API](/core/client-api/javascript/modules/dh.html) to send alerts to mobile apps.
- _Web scraping and integration_: Write web scrapers in Groovy, Python, or Java to ingest XML, store records persistently, and perform real-time [aggregations](../how-to-guides/dedicated-aggregations.md) alongside historical data.
- _Wall Street expertise_: Wall Street professionals trust Deephaven for trading-signal research, quantitative modeling, simulations, risk management, monitoring, reporting, and automated actions on real-time and historical market data.

<div className="text--primary padding-top--xl">

## What are the data sources?

</div>

Deephaven aligns with modern trends in persisted, in-memory, and streaming data formats, so it works across platforms. It reads partitioned, columnar data in place from [Parquet](../how-to-guides/data-import-export/parquet-import.md) files, reads and writes [Iceberg](../how-to-guides/data-import-export/iceberg.md) tables, and consumes event streams from [Kafka](../how-to-guides/overview-kafka.md) and Kafka-compatible platforms such as Redpanda. It also exchanges data with other systems over [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md).

Deephaven integrates with many other data sources by bringing them into memory as ordinary Deephaven tables. These sources include [pandas](/core/docs/how-to-guides/use-pandas/) dataframes, [NumPy](/core/docs/how-to-guides/use-numpy/) arrays, [CSVs](../how-to-guides/data-import-export/csv-import.md), JSON, [JDBC databases](../how-to-guides/data-import-export/execute-sql-queries.md#read-query-results-from-an-external-database), change data capture (CDC) streams, and R dataframes. You can join, manipulate, persist, or consume them as needed.

For many scenarios, direct integration with upstream applications is ideal. Upstream applications can push data into Deephaven through its client APIs, which are listed under [Range](#range). Deephaven can also poll an upstream source on a schedule you choose, with a [function-generated table](../how-to-guides/function-generated-tables.md).

You can store unstructured data as blobs in Deephaven table columns. Machine learning tools can then extract structured values from those blobs, and you can analyze the results with the same table operations as any other data.

<div className="text--success padding-top--xl">

## Isn't Deephaven like...?

</div>

"Yes." Probably. There are plenty of elephants in this room.

It's natural to map new tools against familiar ones. Deephaven operates in a landscape shaped by Flink, Spark Streaming, KX, Influx, Tinybird, RisingWave, and Materialize, so it certainly shares some capabilities and concepts with such technologies. It also exchanges data with many of them through shared formats and protocols such as [Kafka](../how-to-guides/overview-kafka.md), [Parquet](../how-to-guides/data-import-export/parquet-import.md), [Iceberg](../how-to-guides/data-import-export/iceberg.md), and [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md).

What sets Deephaven apart is its live dataframes. Together with the [composability and range](#what-is-deephaven) they provide, live dataframes let Deephaven excel in:

- _Ease of use_: Deephaven simplifies complex workflows for developers and data scientists.
- _Real-time Python integrations_: Python libraries for analytics and data science run directly on real-time data in Python sessions.
- _Rich joins_: [Relational](../how-to-guides/joins-exact-relational.md) and [time-series joins](../how-to-guides/joins-timeseries-range.md), such as [as-of joins](../reference/table-operations/join/aj.md), update incrementally on live data. [Range joins](../reference/table-operations/join/range-join.md) are the exception: they currently require static tables.
- _Dynamic UI/UX_: User interfaces and dashboards respond and update as the data changes.

<div className="text--warning padding-top--xl">

## What's under the hood?

</div>

Deephaven has delivered software components that are both unique and necessary for the streaming, batch, and hybrid use cases listed in [What can you use Deephaven for?](#what-can-you-use-deephaven-for). Some represent full solutions to what other industry players consider open problems.

- An incremental update model, driven by the [update graph](./table-update-model.md), that propagates each cycle's changes through the [DAG](./dag.md) of dependent tables, so most operations process only the rows a change affects.
- A high-performance Java engine bound tightly with native CPython and [NumPy](/core/docs/how-to-guides/use-numpy/) through [jpy](https://github.com/jpy-consortium/jpy), a Python-Java bridge that Deephaven helps maintain.
- An array-oriented architecture that allows for vectorized operations, amortized costs, and columnar data transfer.
- An architecture that runs user code in-process as part of engine evaluation, rather than shipping data to the client.
- Ticking-table subscriptions. A ticking table (a live dataframe) is one whose rows change as new data arrives. A subscription streams the table's changes, batched at a configurable [update interval](./what-is-barrage.md#update-intervals-and-batching), from the server's [DAG](./dag.md) to clients, dataframe-based tools, and other engine processes over [Barrage](./what-is-barrage.md), an [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md) extension.
- Subscription support in the [Java](../how-to-guides/java-client.md), [C++](/core/client-api/cpp/), C#, and [JavaScript](/core/client-api/javascript/modules/dh.html) clients, and in the [Python client](/core/client-api/python/) with the `pydeephaven-ticking` package.

<div className="text--danger padding-top--xl">

## What's the difference between the Community and Enterprise stacks?

</div>

Deephaven's Enterprise product is like an F1 car, with its Community products as the subsystems. The analogy offers a clear perspective on Deephaven's software development and delivery strategy. A racecar's success relies on its subsystems, such as the engine, transmission, chassis, suspension, aerodynamics, and tires. Each one plays a critical and often differentiating role.

Deephaven develops and releases these "subsystems" openly to the community. Examples include:

- [deephaven-core](https://github.com/deephaven/deephaven-core): The engine driving real-time data operations.
- [jpy](https://github.com/jpy-consortium/jpy): The Python-Java bridge that binds the engine to CPython.
- [Barrage](https://github.com/deephaven/barrage): The [Arrow Flight](../how-to-guides/data-import-export/arrow-flight.md) extension that streams ticking data to clients.
- Polyglot client APIs: [gRPC](https://grpc.io/)-based client libraries in [Python](/core/client-api/python/), [Java](../how-to-guides/java-client.md), [C++](/core/client-api/cpp/), [JavaScript](/core/client-api/javascript/modules/dh.html), [R](/core/client-api/r/), [Go](https://pkg.go.dev/github.com/deephaven/deephaven-core/go), and C#.
- [Web UI](https://github.com/deephaven/web-client-ui): A real-time grid browser application framework.
- [deephaven.ui](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/ui): A Python library for building real-time dashboards, with programmatic layouts and live parameterized queries.

These subsystems are sufficient for smaller projects that don't require advanced clustering, large-scale fanout of live data to many consumers, sophisticated access controls, or high-grade reliability. Community users and their organizations can embed these tools into their workflows and infrastructure to effectively meet their needs.

People want gear that "just works." Community users benefit from Enterprise reliability work. Deephaven fixes the edge cases its Enterprise customers find in production, and when an edge case is in a Community component, the fix ships in Community too.

For larger, mission-critical workloads, the metaphor evolves. Teams typically require a complete car, not just individual components. These teams have three primary options:

1. Build their own car using Deephaven's subsystems.
2. Upgrade their existing car by integrating selected Deephaven subsystems.
3. Leverage the Deephaven Enterprise product, a customizable, high-performance, fully packaged solution delivering the capabilities of an elite F1 car.

This dual-stack approach gives flexibility and accessibility for a wide range of use cases, from community projects to enterprise-scale solutions.

For the technical differences between Community and Enterprise, see the [Deephaven Enterprise documentation](/enterprise).

<div className="text--info padding-top--xl">

## Next steps

</div>

- Install Deephaven and run your first queries with the [Quickstart](../getting-started/quickstart.md).
- Learn the core concepts step by step in the [Crash Course](../getting-started/crash-course/overview.md).
- Bring in data from [Kafka](../how-to-guides/data-import-export/kafka-stream.md), [Parquet](../how-to-guides/data-import-export/parquet-import.md), or [Iceberg](../how-to-guides/data-import-export/iceberg.md).
- See how changes flow through queries in the [incremental update model](./table-update-model.md).
- Follow new features and integrations in the [release notes](https://github.com/deephaven/deephaven-core/releases) and the [deephaven-core GitHub repository](https://github.com/deephaven/deephaven-core).

<div className="row">
<QuickstartCTA />
</div>
