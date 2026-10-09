---
title: Kafka overview
description: Deephaven provides a suite of tools that makes Kafka integration easy. Learn how to connect to Kafka streams, consume messages, and process data in real time.
hide_table_of_contents: true
---

import { CoreTutorialCard } from '@theme/deephaven/core-docs-components';

<div className="comment-title">

Deephaven provides a suite of tools that makes Kafka integration easy.

</div>

<hr className="margin-bottom--lg" />

<div className="row">

<CoreTutorialCard to="/core/docs/conceptual/kafka-basic-terms">

## Kafka basic terms

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/data-import-export/kafka-stream">

## Connect to a Kafka stream

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/data-import-export/write-your-own-custom-parser-for-kafka">

## Write a custom Kafka parser

</CoreTutorialCard>

</div>

<div className="comment-title">

Event stream, meet real-time engine

</div>

Deephaven consumes Kafka topics into live tables, which update as new events arrive, and publishes tables to Kafka topics. A table consumed from Kafka uses the same table operations as a static table, so the code you write to explore historical data also processes the live feed.

This page walks through an example. It explores historical data stored in Parquet files to build a baseline of expected service use, then [consumes a Kafka feed](#consume-the-kafka-feed) and compares each live sample to that baseline to flag anomalies. After the example, two sections point to [Application Mode](./application-mode.md) for running the same code as a live feed and list what you can do with a live table once you have one.

## A Kafka example in Deephaven

Consider the problem of analyzing usage-level anomalies for a service running in the cloud. The service backend runs as many processes on a machine pool, and each process produces telemetry for the requests it serves. The metric of interest is service use: the number of requests received in each 10-second sampling interval. For the purpose of this example, we assume a consolidated Kafka feed exists that aggregates overall system use in 10-second intervals.

Each Kafka event belongs to a topic and a partition, and carries a timestamp, a key, and a value. See [Kafka basic terms](../conceptual/kafka-basic-terms.md) for what each of these means. A single event in this feed looks like this:

| Topic        | Partition | Timestamp | Key         | Value           |
| ------------ | --------- | --------- | ----------- | --------------- |
| `ServiceUse` | `0`       | `t0`      | `MySvcName` | `TotalRequests` |

_`TotalRequests` above indicates the total number of requests received across all machines in the single 10-second period defined by `t0`. The Kafka value is an [Avro](../conceptual/kafka-basic-terms.md#avro) record that stores this count as a 64-bit integer in its `Value` field._

A historical capture of the feed is also stored as Parquet files. The files form a [key-value partitioned Parquet directory](./data-import-export/parquet-import.md#partitioned-parquet-directories) with one subdirectory per day, named like `Date=2021-05-01`. The capture adds a `Date` column to the columns listed above. `Date` is the partitioning column, and its values are strings in `'YYYY-MM-DD'` form.

Our goal is to flag periods when overall system use falls outside a baseline of expected use. Defining that baseline is our first step.

### Explore historical data

We begin by exploring historical data in the Deephaven IDE console. We load the historical data with the two lines of Groovy below:

```groovy skip-test
import io.deephaven.parquet.table.ParquetTools

svcUse = ParquetTools.readTable("/path/to/parquet/data/root/dir").where("Key = `MySvcName`")
```

This defines a Deephaven table called `svcUse` and opens it as a separate panel in the IDE, initially showing the first rows of the table. The [`where`](../reference/table-operations/filter/where.md) filter keeps only the rows for `MySvcName`, the same service that we later filter the live feed to.

The tabular view is useful to get an initial sense of the data. Scrolling through the view stays responsive, even though the capture holds one row every 10 seconds for many months.

[`readTable`](../reference/data-import-export/Parquet/readTable.md) does _not_ read all the data from the Parquet files. It loads only the metadata. Deephaven loads table data into memory only when a downstream operation pulls rows, such as scrolling through a table view or computing a derived table.

We push the scrollbar to the end of the available range to look at the most recent data. The numbers look comfortably bigger than what we saw in the first block of rows, which reflects organic growth in user adoption for our service. We expect this growth, and it is what we want to monitor.

After a bit more browsing, we switch to a graphical view for more perspective. We want a line graph of the most recent months of data, so we filter on the `Date` partitioning column. Replace the date below with one about four months before your latest data:

```groovy skip-test
svcUseLast4Months = svcUse.where("Date > `2021-05-01`")
```

With the panel for this new table selected, we click on **Table Options** and pick [**Chart Builder**](./user-interface/chart-builder.md) from the menu. A simple line chart is enough for now. The graph shows marked seasonality. It looks like a "time of day, day of week" pattern. To confirm our guess, we define a derived table adding a few columns:

```groovy skip-test
import java.time.Instant

// Truncate to the start of the 10s period
secs = { Instant ts -> 10L * (long) (ts.toEpochMilli() / 10000) }

svcUseDecorated = svcUse.updateView(
    "Secs = (long) secs(Timestamp)",
    "OrdinalDay = (long) (Secs / (24 * 60 * 60))",
    "SecondsInDay = Secs % (24 * 60 * 60)",
    "OrdinalWeek = (long) (OrdinalDay / 7)",
    "DayOfWeek = OrdinalDay % 7"
)
```

Like the Parquet read, [`updateView`](../reference/table-operations/select/update-view.md) does not compute its new columns up front. It computes a value only when a later operation, like a UI view or a chained computation, reads it. The derived table also adds no memory cost for the pre-existing columns, because it reads them from the base table instead of copying their data.

The code above also calls the Groovy closure `secs` inside column expressions. Because `updateView` doesn't store results, the query engine calls `secs` again every time an operation reads a `Secs` value, including when it computes `OrdinalDay`, `SecondsInDay`, and `DayOfWeek`.

Filtering this new table and graphing the results confirms the seasonality. Holidays are the exception, because their daily pattern resembles a Sunday's. A production model would need to account for holidays, but this example ignores them.

To isolate the seasonality effects and more clearly observe the overall trend over time, we create an aggregation by week for the whole series:

```groovy skip-test
byWeek = svcUseDecorated.view("Value", "OrdinalWeek").sumBy("OrdinalWeek")
```

Graphing the weekly totals in `byWeek`, either with the Chart Builder or in code with the [plotting API](./plotting/api-plotting.md), gives us a clear picture of the organic service-use growth over time.

### Build a baseline

A complete baseline could overlay two simple models: one that captures the seasonality and one that captures the growth trend. This example builds only the seasonality model.

We build the seasonality model as a derived table that averages the last four samples matching the same time of day and day of week, as a tentative baseline. After we consume the live feed, we compare each live sample to this baseline in [Compare live data to the baseline](#compare-live-data-to-the-baseline).

Creating this table involves doing [aggregations](./combined-aggregations.md) and [filtering](./use-filters.md). This page doesn't cover these operations in detail. In general terms, the code below restricts samples to the four weeks before the last midnight, and then aggregates by the `DayOfWeek` and `SecondsInDay` columns:

```groovy skip-test
import io.deephaven.time.DateTimeUtils
import static io.deephaven.api.agg.Aggregation.AggAvg

tz = DateTimeUtils.timeZone("ET") // Replace with your time zone
lastMidnightSecs = secs(DateTimeUtils.atMidnight(DateTimeUtils.now(), tz))
svcUseLast4Weeks = svcUseDecorated.where(
    "Secs >= lastMidnightSecs - 4 * 7 * 24 * 60 * 60",
    "Secs < lastMidnightSecs"
)

svcUseLast4WeeksAvg = svcUseLast4Weeks.aggBy(
    [AggAvg("Last4Avg = Value")],
    "DayOfWeek", "SecondsInDay"
)
```

### Consume the Kafka feed

Next, we get live samples to compare against. Ingesting the Kafka feed into a live Deephaven table gives us a table that looks and feels like our previous tables for historical data.

As the note under the sample event says, each Kafka value in the `ServiceUse` topic is an Avro record. The consumer fetches the record's schema by name, `service_use_record`, from a [schema registry](../conceptual/kafka-basic-terms.md#formats) at the `schema.registry.url` address. See [Read Kafka topic in Avro format](./data-import-export/kafka-stream.md#read-kafka-topic-in-avro-format) for details. The record's `Value` field becomes the `Value` column. The Kafka key holds the service name and becomes the `ServiceName` column:

```groovy skip-test
import io.deephaven.kafka.KafkaTools
import io.deephaven.engine.table.impl.BlinkTableTools

kafkaProps = new Properties()
kafkaProps.put('bootstrap.servers', 'kafkahost:9092')
kafkaProps.put('schema.registry.url', 'http://regsvchost:8081')

liveUse = BlinkTableTools.blinkToAppendOnly(
    KafkaTools.consumeToTable(
        kafkaProps,
        "ServiceUse",
        KafkaTools.ALL_PARTITIONS,
        KafkaTools.ALL_PARTITIONS_DONT_SEEK,
        KafkaTools.Consume.simpleSpec('ServiceName', java.lang.String),
        KafkaTools.Consume.avroSpec('service_use_record'),
        KafkaTools.TableType.blink()
    ).where("ServiceName = `MySvcName`")
)
```

The consumer produces a [blink table](../conceptual/table-types.md#specialization-3-blink), which keeps only the rows from the current [update cycle](../conceptual/table-update-model.md), so events that the `where` filter drops are never stored. [`BlinkTableTools.blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md) then keeps every matching row.

Tables derived from live tables by most query operations are also live. Deephaven updates them incrementally where it can, applying the rows added, modified, or removed in the parent table to the previous result instead of recomputing from scratch. For example, when new rows arrive, the `where` filter above evaluates only those rows instead of filtering the whole table again. See the [Deephaven table update model](../conceptual/table-update-model.md) for details.

### Compare live data to the baseline

Now we are ready to decorate the live data with the four-week average `svcUseLast4WeeksAvg` from [Build a baseline](#build-a-baseline). First, we derive the join-key columns `DayOfWeek` and `SecondsInDay` from the `KafkaTimestamp` column that the consumer adds to the live table:

```groovy skip-test
liveUseWithLast4WeeksAvg = liveUse.updateView(
    "Secs = (long) secs(KafkaTimestamp)",
    "SecondsInDay = Secs % (24 * 60 * 60)",
    "DayOfWeek = ((long) (Secs / (24 * 60 * 60))) % 7"
).naturalJoin(
    svcUseLast4WeeksAvg, "DayOfWeek, SecondsInDay"
).updateView("PredictedDiff = Value - Last4Avg", "PredictedPct = 100 * Value / Last4Avg")
```

The operation above does a [natural join](../reference/table-operations/join/natural-join.md) between the live table and the static table of historical averages. It then adds two new columns that compare each value to its baseline. The join matches each live row to its baseline by `DayOfWeek` and `SecondsInDay`. As new rows arrive in the live table, the join updates its result incrementally.

Finally, we add a filter that keeps only the rows where the sample differs from the baseline by more than 5%:

```groovy skip-test
useAnomalies = liveUseWithLast4WeeksAvg.where("abs(PredictedPct - 100) > 5")
```

`useAnomalies` is a live table of flagged samples. Each time a new Kafka event arrives that differs from its baseline by more than 5%, a row appears in it.

## From exploration to modeling to deployment

The [Kafka example](#a-kafka-example-in-deephaven) built a model that flags service-use anomalies by comparing live Kafka samples to a four-week historical average. The next step is to implement and deploy production-quality code that gives the organization a live feed from that model. Traditionally, this means a change of language, tools, and processes. It can also mean handing the work from one person to another, which adds friction and cost. Any later change to the model or a bug fix repeats those costs. Separate codebases for modeling and deployment also raise the question of whether they implement the same thing, and teams seldom test for it.

Deephaven removes the split between modeling code and deployment code. You can save the same chain of query operations you used to build the model as a script and run it as a live feed with [Application Mode](./application-mode.md).

## Act on live tables

What can you do with a live table once you have one? Beyond deriving more tables from it, you can:

- [Publish a live table as a Kafka topic](../reference/data-import-export/Kafka/produceFromTable.md).
- Make a live table available for subscription from another Deephaven server over [Barrage](https://github.com/deephaven/barrage), for example with [URIs](./use-uris.md). Read more about the protocol in the [Deephaven Core API concept guide](../conceptual/deephaven-core-api.md).
- View live tables and plots in the Deephaven IDE as panels that you can arrange and filter.
- Integrate any Java or Groovy library on the classpath, because your Groovy code runs in the same JVM as the Deephaven query engine. For example, trigger alerts in your incident response platform when a metric crosses a tolerance threshold, send notifications to a messaging application, or place orders in an automated ordering system.

## Related documentation

- [Kafka basic terms](../conceptual/kafka-basic-terms.md)
- [Connect to a Kafka stream](./data-import-export/kafka-stream.md)
- [Write your own custom parser for Kafka](./data-import-export/write-your-own-custom-parser-for-kafka.md)
- [Application Mode](./application-mode.md)
- [`consumeToTable`](../reference/data-import-export/Kafka/consumeToTable.md)
- [`produceFromTable`](../reference/data-import-export/Kafka/produceFromTable.md)
