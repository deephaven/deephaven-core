---
title: Deephaven Overview
description: "Deephaven is a query engine and software stack featuring 'live dataframes' for real-time, structured data. It offers composability and a broad range."
hide_title: true
---

import { QuickstartCTA } from '@theme/deephaven/CTA';

<div className="padding-top--xl" />

# Deephaven Overview

<div className="text--warning padding-top--xl">

## What is Deephaven?

</div>

Deephaven is a query engine and software stack centered on "live dataframes," a groundbreaking abstraction for real-time, structured data. Live dataframes let you write queries using familiar patterns from batch-processing tools like SQL, pandas, polars, and R. Similar to these tools, queries in Deephaven form a Directed Acyclic Graph (DAG). However, unlike traditional batch systems, Deephaven seamlessly moves real-time table changes ("deltas") through the DAG, enabling incremental updates without extra engineering effort.

Deephaven’s two superpowers:

<div className="comment-title">

_Composability_

</div>

Deephaven uniquely allows the outputs of one query to always serve as inputs for another downstream query. This modular "building block" approach enables seamless integration of real-time microservices, even across networks, unlocking countless new use cases for real-time data.

<div className="comment-title">

_Range_

</div>
Deephaven bridges real-time and batch data, letting you apply the same operations to both while easily combining them. It supports first-class interfaces in SQL, Python, and Java, along with low-level APIs for popular Python libraries, user-defined functions, and wire protocols for extending DAGs and managing resources remotely. Additionally, it offers a robust JS API, powerful browser-based grid and plotting components, and a rich Python dashboarding library for creating real-time dashboards. This comprehensive stack empowers teams to focus on solving business problems without getting bogged down in utility engineering.

<div className="text--danger padding-top--xl">

## Why use Deephaven?

</div>

People turn to Deephaven when their work involves:

- Integrating real-time and dynamic data into applications or analytics.
- Performing time-series and relational operations on large Iceberg or Parquet datasets, Kafka streams, or combinations of these.
- Running SQL queries on connected tables that update in real time.
- Tackling complexity with custom Python, Java, or C++ code that runs seamlessly alongside table operations.
- Linking multiple real-time query engines for parallel processing, pipelining, or microservice-driven analytical tasks.

<div className="text--info padding-top--xl">

## What can you use Deephaven for?

</div>

<div className="comment-title">

Developers and data scientists rely on Deephaven for analytics and applications across streaming, batch, and hybrid use cases. Its versatile engine supports everything from simple transformations to machine learning and autonomous applications. At the same time, the stack enables large-scale data distribution, connected applications, and advanced real-time visualizations and dashboards.

</div>
*Industries Where Deephaven Excels*

Deephaven is the perfect fit for workloads in IoT, personalization, capital markets, social media, blockchain, crypto, gaming, e-commerce, industrial telemetry, power & energy, and healthcare.

_Example Use Cases_

- _Streaming Analytics_: Connect to Kafka streams, perform joins and calculations, and publish derived, updating data downstream.
- _IoT Applications_: Link IoT devices to Deephaven for real-time summaries, aggregates, visualizations, interactions, and alerts.
- _Batch Data Exploration_: Analyze Parquet files in S3 or marketplaces with responsive tools on the Deephaven Web UI or embedded real-time Jupyter widgets.
- _Hybrid Workflows_: Combine Iceberg tables, Kafka streams, vendor APIs, and Arrow buffers. Use Python libraries to conduct data science at scale, either directly on updating tables or through snapshots mapped to NumPy and Pandas.
- _Custom Streaming Pipelines_: Build Kafka-based pipelines with inline Python functions and Java classes for joins, aggregations, and logic. Integrate with the JS API for mobile app alerts and impulse signals.
- _Web Scraping and Integration_: Write web scrapers in Python or Java to ingest XML, store records persistently, and perform real-time aggregations alongside historical data.
- _Wall Street Expertise_: Deephaven is trusted by Wall Street professionals for tasks like automated actions, signal farming, quantitative modeling, simulations, risk management, monitoring, and reporting, covering real-time and historical data use cases.

<div className="text--primary padding-top--xl">

## What are the data sources?

</div>

Deephaven aligns with modern trends in persisted, in-memory, and streaming data formats, enabling seamless cross-platform use. It provides in-situ access to partitioned, columnar data sources like Parquet and Arrow Flight, along with modern event streams such as Kafka, Redpanda, and Solace. Deephaven offers uniquely powerful read-and-write integrations with Iceberg.

Deephaven seamlessly integrates with a variety of data sources, bringing them into memory where they are handled just like native formats. These sources—including Pandas dataframes, NumPy arrays, CSVs, JSON, XML, ODBC, CDC, and R dataframes—can be joined, manipulated, persisted, or consumed as needed.

For many scenarios, direct integration with upstream applications is ideal. Deephaven provides Java, Python, and C++ APIs, along with flexible engine features, allowing custom binaries to be pushed into memory or polled as required.

Unstructured data can be stored as blobs within Deephaven tables. Modern machine learning tools can extract structured values and analytics from this data, enabling powerful insights directly within Deephaven services.

<div className="text--success padding-top--xl">

## Isn’t Deephaven like...?

</div>

“Yes.” Probably. There are plenty of elephants in this room.

It's natural to map new tools against familiar ones. Deephaven operates in a landscape shaped by Flink, Spark Streaming, Kx, Influx, Tiny Bird, RisingWave, and Materialize, so it certainly shares some capabilities and concepts with such technologies. It also interoperates with them.

What sets Deephaven apart, however, is its unique live dataframes, enabling it to excel in:

- _Composability_: Seamlessly connect queries like building blocks.
- _Ease of Use_: Simplify complex workflows for developers and data scientists.
- _Real-Time Python Integrations_: Effortlessly leverage Python libraries for analytics and data science.
- _Limitless Joins_: Handle even the most complex joins, including time-series joins, easily and without limitation.
- _Dynamic UI/UX_: Deliver responsive, updating user interfaces and dashboards.

<div className="text--warning padding-top--xl">

## What’s under the hood?

</div>

Deephaven has delivered software components that are both unique and necessary for anyone who cares about the use cases described above. Some represent full solutions to what other industry players consider open problems.

- Graph-based update model that allows efficient incremental computation of results.
- Unified abstraction for streaming and batch data.
- A high-performance Java engine bound tightly with native CPython, NumPy, and SciPy through a JPy bridge (which Deephaven helps maintain).
- Array-oriented architecture that allows for vectorized operations, amortized costs, and modern data transfer.
- Flexible architecture that allows user code to be executed in-process as part of engine evaluation, rather than shipping data to the client.
- gRPC-based APIs in Python, Java, C++, C#, JavaScript, R, and Go to extend the update graph across the network (to clients and other engine processes).
- Arrow-Flight extension to deliver ticking and updating data to the dataframe-driven community.
- JS Web components that support scrolling and manipulating tables with billions of records (filter, sort, add columns, change input tables, create roll-ups, etc.).
- Excel and Jupyter integrations that provide Deephaven’s server-side capabilities and live updates to these popular tools.
- Integrations with Python libraries like PyTorch and Tensor Flow to support high-end machine learning and AI on real-time data without requiring snapshots.

Though these breakthrough solutions are the headline, people want gear that “just works.” The community is well served by the countless edge cases that have surfaced and been addressed in service of Deephaven’s Enterprise customers.

<div className="text--danger padding-top--xl">

## What’s the difference between the Community and Enterprise stacks?

</div>

<div className="comment-title">

Deephaven’s Enterprise product is like an F1 car, with its Community products as the subsystems.

</div>

The analogy of an F1 car offers a clear perspective on Deephaven’s software development and delivery strategy. A racecar's success relies on its subsystems—engine, transmission, chassis, suspension, aerodynamics, and tires — each playing a critical and often differentiating role.

Deephaven develops and releases these "subsystems" openly to the community. Examples include:

- [deephaven-core](https://github.com/deephaven/deephaven-core): The engine driving real-time data operations.
- [Jpy](https://github.com/jpy-consortium/jpy): A bidirectional bridge between Python and Java.
- [Barrage](https://github.com/deephaven/barrage): A high-performance wire protocol.
- Polyglot Client APIs: Enabling multi-language integrations; [gRPC](/core/javadoc/io/deephaven/proto/backplane/grpc/SessionServiceGrpc.html), [Python](/core/client-api/python/), [Java](/core/javadoc/), [C++](/core/client-api/cpp/), [JavaScript](/core/client-api/javascript/modules/dh.html), [R](/core/client-api/r/), [Go](https://pkg.go.dev/github.com/deephaven/deephaven-core/go), and C#.
- Web-UI: A real-time grid browser application framework.
- [deephaven-ui](https://github.com/deephaven/deephaven-plugins/tree/main/plugins/ui): A Python library for programmatic layouts and live parameterized queries.

These subsystems are sufficient for smaller projects that don’t require advanced clustering, fanout capabilities, sophisticated access controls, or high-grade reliability. Community users and their organizations can embed these tools into their workflows and infrastructure to effectively meet their needs.

For larger, mission-critical workloads, the metaphor evolves. Teams typically require a complete car, not just individual components. These teams have three primary options:

1. Build their own car using Deephaven’s subsystems.
2. Upgrade their existing car by integrating selected Deephaven subsystems.
3. Leverage the Deephaven Enterprise product, a customizable, high-performance, fully-packaged solution delivering the capabilities of an elite F1 car.

This dual-stack approach ensures flexibility and accessibility for a wide range of use cases, from community projects to enterprise-scale solutions.

The technical details of the differentiation between Community and Enterprise are articulated [here](/enterprise).

<div className="text--danger padding-top--xl">

## Deephaven continues to add features, integrations, and improvements

</div>

The Deephaven team is currently investing in future-leaning integrations with Kafka and Iceberg, a slick VS Code experience, deeper support for real-time, expandable pivot tables, an array of widgets and experiences for programmatic dashboards, and many other features and enhancements.

Stay up-to-date with Deephaven's evolution by visiting the Core [GitHub repository](https://github.com/deephaven/deephaven-core).
