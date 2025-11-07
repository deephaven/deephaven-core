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

<div className="comment-title">

Deephaven is a query engine that excels at handling real-time data, both on its own and in conjunction with static volumes, at scale.

</div>

A query engine sits above your data sources, and is used to execute queries and produce results from your data that can then be used by users, applications, or emitted as a materialized view. Deephaven is also a suite of technologies around that engine. These integrations, experiences, and APIs provide a turnkey framework that enable people to be immediately productive. Open APIs, source and sink hookups, interactive web-UIs, and collaborative utilities are part of that suite. These components enable people to use Deephaven as either a tool within their established kit, or as a central system that powers their stack.

Deephaven is new to the open community, but it is not new. The engine has evolved and been battle-hardened under the care of a series of capital markets players - bulge bracket hedge funds, bank trading desks, exchanges - in service of a large range of use cases, many in the critical path of quantitative finance. The framework has breadth because hundreds of developers and quants have demanded features. It’s fast, because they’d have it no other way.

<div className="text--danger padding-top--xl">

## Why use Deephaven?

</div>

People use Deephaven when their work involves any of the following:

- Real-time and other dynamic data in the applications or analytics they’re building.
- Time series and relational operations to deliver top performance on large static volumes, dense ticking data, or combinations of the two.
- Complexity that requires custom Python, Java, C++ to be compiled alongside table operations.

Sophisticated use cases often require a series of capabilities, not just narrow differentiators and stellar performance. In such scenarios, breadth of utility and general ease-of-use matter a lot. Deephaven provides its most substantial value to users in this dimension. Simply stated, the whole is greater than the sum of the parts.

It’s not jingle-worthy, but perhaps “being good at a lot of things” is Deephaven’s superpower.

<div className="text--info padding-top--xl">

## What can you use Deephaven for?

</div>

<div className="comment-title">

Developers and data scientists use Deephaven for big data use cases that drive analytics and applications. It can be employed for a wide range of purposes -- from simple transformations through machine-learning.

</div>

IOT, market-tech, capital markets, blockchain, crypto, gaming, e-commerce, industrial telemetry, healthcare, and public policy all present workloads where Deephaven is the perfect fit.

- Connect to a series of Kafka streams. Join them. Do calculations. Publish derived, updating data downstream.
- Hook up IOT devices to Deephaven. Create summary and aggregate results. Build real-time visualizations, interactions, and alerts.
- Point Deephaven at Parquet files in S3 or marketplaces. In the Deephaven Console on the web, explore the data programmatically, or via the highly responsive interactive table and plotting tools. Inherit the benefits of Parquet’s nesting features.
- Connect Deephaven to a combination of CSVs, Kafka streams, vendor data-publishing APIs, and Arrow buffers (via Flight). Pip install your favorite Python libraries. Do data science at scale either directly on (updating) Deephaven tables or with snapshots mapped to [NumPy arrays](https://numpy.org/doc/2.1/reference/generated/numpy.ndarray.html) and [Pandas dataframes](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
- Make available a series of Kafka or RedPandas streams. Write your own custom Python functions and Java classes. Combine these in-line with joins, aggregations, and logic to model alerts. Use the JS API to deliver impulse signals to a mobile distribution app.
- Write a web scraper (in Python or Java) within Deephaven. Ingest pertinent XML. Write records to a persistent store at some interval. Do aggregations on real-time data and join with history.

Deephaven’s Wall Street users depend on its capabilities for automated actions, signal farming, quantitative modeling, simulations, risk, monitoring, and reporting. The range of use cases is meaningful, and big money is on the line.

<div className="text--primary padding-top--xl">

## What are the data sources?

</div>

Deephaven embraces the modern trend toward persisted, in-memory, and streaming formats that can be used across platforms. It delivers in-situ access to partitioned, columnar data sources (like Parquet and Arrow Flight) as well as modern event streams (like Kafka, RedPandas, and [soon] Solace and Chronicle).

Many other data sources can be integrated to deliver data in-memory. From there, these are handled identically to the aforementioned formats and can be joined, manipulated, exhausted or persisted as the user designates. Pandas dataframes, NumPy arrays, CSVs, JSON, XML, [soon] ODBC, CDC, and R dataframes are examples of these.

Many use cases, however, are best served by connecting upstream applications directly. Deephaven has APIs in Java, Python, and C++ for this, and flexible engine features allow for custom binaries to be pushed to Deephaven’s memory or to be polled.

Unstructured data are stored as blobs within Deephaven tables. Users can use modern ML tools to pull structured values and analytics from this data - all within Deephaven services.

<div className="text--success padding-top--xl">

## Isn’t Deephaven like……… ?

</div>

“Yes.” Probably yes. There are a lot of elephants in this room.

It is natural to place the new on a map framed by the familiar. Deephaven enters an arena defined by Kafka, Spark, Influx, Redshift, BigQuery, Snowflake, Postgres, and dozens of other players - open-source, commercial, large and small.

A Venn diagram of industry capabilities would likely be multi-dimensional and would certainly be messy. Like others, Deephaven overlaps with the aforementioned technologies to varying degrees.

- Deephaven is best of breed with real-time data. But streaming analytics is Spark’s tagline.
- Deephaven’s architecture benefits from the separation of storage from compute. So does Snowflake’s.
- Deephaven is first class at manipulating streams and delivering derived, updating datasets to consumers. Kafka ksqlDB claims the same.
- Deephaven’s performance with time series is elite. Influx, Timescale, Prometheus, Druid somewhat battle for that turf.
- Deephaven is natural for data science and machine learning at scale. This places it in an arena with Jupyter and Pandas.

Though Deephaven users might argue it is the best alternative for a broad range of applications, Deephaven stands out as the only solution if you (i) have streaming data, (ii) need to do on-the-fly joins, and (iii) want to easily send derived, ticking data downstream; or (iv) if you’d like to interactively explore live sets in a browser; or (v) if you’d like to blissfully assume that anything supported in batch is also supported for streams.

<div className="text--warning padding-top--xl">

## What’s under the hood?

</div>

Deephaven has delivered software components that are both unique and necessary for anyone who cares about the use cases described above. Some represent full solutions to what other industry players consider open problems.

- Graph based update model that allows efficient incremental computation of results.
- Unified abstraction for streaming and batch data.
- High-performance Java engine bound tightly with native CPython, [NumPy](/core/docs/how-to-guides/use-numpy), and SciPy through a JPy bridge (that Deephaven helps maintain).
- Array-oriented architecture that allows for vectorized operations, amortized costs, and modern data transfer.
- Flexible architecture that allows user code to be executed in-process as part of engine evaluation, rather than shipping data to the client.
- gRPC-based APIs for extending the update graph across the network (to clients and other engine processes).
- Arrow-Flight extension to deliver ticking and updating data to the dataframe-driven community.
- JS Web components that support scrolling and manipulating tables with billions of records (filter, sort, add columns, (soon) change input tables, create roll-ups, etc.).
- (soon) Jupyter kernel that provides Deephaven’s server-side capabilities, and table and plot widgets for interacting with real-time, massive, and stateful results.
- (soon) Integrations with Python libraries like PyTorch and Tensor Flow to support high-end machine learning on real-time data without the need for snapshots.

Though these breakthrough solutions are the headline, people want gear that “just works.” The community is well served by the countless edge cases that have surfaced and been addressed in service of Deephaven’s initial Wall Street user base.

<div className="text--danger padding-top--xl">

## What’s yet to come?

</div>

<div className="comment-title">

The team behind Deephaven’s initial launch is committed to supporting its users and to incorporating their feedback into the project.

</div>

A vision of Deephaven’s evolution can be found on its [GitHub wiki](https://github.com/deephaven/deephaven-core/wiki). It outlines further work related to source, sink, and data science integrations; clustering and map-reduce infrastructure; and programmatic builders and input experiences to make dashboards more luxurious and flexible.

That said, the community will dynamically establish Deephaven’s roadmap, both via consensus and collaboration, and through derivative work.

<div className="row padding-vert--xl">

<QuickstartCTA/>

</div>
