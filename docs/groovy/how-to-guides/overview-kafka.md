---
title: Kafka overview
description: Deephaven provides a suite of tools that makes Kafka integration easy. Learn how to connect to Kafka streams, consume messages, and process data in real time.
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

## Event stream, meet real-time engine

Event-based applications have succeeded in providing a path forward for traditional transactional workloads at scale. Many have replaced monolithic database systems, running business operations instead as event processors connected by pub/sub platforms like Kafka. These platforms offer delivery guarantees strong enough to support transactions and recovery, and they scale horizontally across many machines.

This transformation has created an opportunity. In monolithic systems, you typically see only the end state, which is the result of a transaction. Those systems don't usually expose how the transaction got there, including its inputs and the triggers that started it, as data that other applications can read.

In contrast, pub/sub systems capture the triggers, because the triggers are already modeled as events. Other applications, not only the main production systems, can read those same events, and adding another reader costs little.

Diagnostics transformed the practice of medicine in the 20th century as blood tests, imaging, endoscopy, and biopsies offered physicians an insider's view of a patient's body. Live data is bringing a similar revolution to business operations.

Analyzing these events poses a different challenge from running the transactions themselves. Event platforms decouple producers from consumers, so events no longer queue behind one monolithic system. Event flows multiply and adapt faster to changing operational needs. Kafka deployments carry a growing number of operational feeds, each connecting one system to another or a few systems to a few others.

The hard problem is no longer building scalable pipelines that model business operations. It is detecting trends in those feeds, distilling insights from them, and delivering those insights as derived feeds that executable models produce in real time. Since trends change quickly, a fast explore-model-deploy cycle is critical.

**Enter Deephaven.** Deephaven was born from the need for fast data-driven R&D cycles for quantitative finance in the capital markets industry.

In capital markets, success depends not only on the ability to innovate, but on the speed of innovation. Teams slow down when siloed people do data exploration, model fitting, backtesting, implementation, and deployment as separate activities, in languages and tools that don't mix. Having different representations for streaming and historical data only compounds the problem.

Deephaven built its engine for a world where feeds are fundamental and live and historical data share a common vocabulary. Deephaven provides one [table operations](../conceptual/table-operations-overview.md) library for both streams and [static tables](../conceptual/table-types.md#static-tables), in your language of choice. The same operations run in-process in the data engine or remotely from a client, and they use popular, interoperable data formats.

The rest of this page walks through the explore and model steps of the explore-model-deploy cycle on one example:

1. Explore a [historical Parquet capture](./data-import-export/parquet-import.md) of a Kafka feed and build a baseline from it.
2. Consume the live feed with the same table operations, and flag live samples that depart from the baseline.
3. See how the same code deploys, and what you can do with the resulting feed.

## A Kafka example in Deephaven

Consider the problem of detecting anomalies in the use of a service running in the cloud. The service backend runs as many processes on a machine pool, and each process produces telemetry for the requests it serves. The metric of interest is service use: the number of requests received in each 10-second sampling interval. For the purpose of this example, we assume a consolidated Kafka feed exists that aggregates overall service use in 10-second intervals.

Each Kafka event belongs to a topic and a partition, and carries an offset, a timestamp, a key, and a value. See [Kafka basic terms](../conceptual/kafka-basic-terms.md) for what each of these means. A single event in this feed looks like this. The table leaves out the offset, which this example doesn't use:

| Topic        | Partition | Timestamp | Key         | Value           |
| ------------ | --------- | --------- | ----------- | --------------- |
| `ServiceUse` | `0`       | `t0`      | `MySvcName` | `TotalRequests` |

_`TotalRequests` above indicates the total number of requests received across all machines in the single 10-second period defined by `t0`. The Kafka value is an [Avro](../conceptual/kafka-basic-terms.md#avro) record that stores this count as a 64-bit integer in its `Value` field._

A historical capture of the feed is also stored as Parquet files. The files form a [key-value partitioned Parquet directory](./data-import-export/parquet-import.md#read-a-key-value-partitioned-parquet-directory) with one subdirectory per day, named like `Date=2021-05-01`. The capture adds a `Date` column to the columns listed above. `Date` is the partitioning column, and its values are strings in `'YYYY-MM-DD'` form.

Our goal is to flag periods when overall service use falls outside a baseline of expected use. To define that baseline, we first explore the historical data.

### Explore historical data

We begin by exploring historical data in the Deephaven IDE console. We load the historical data with the two lines of Groovy below:

```groovy skip-test
import io.deephaven.parquet.table.ParquetTools

svcUse = ParquetTools.readTable("/path/to/parquet/data/root/dir").where("Key = `MySvcName`")
```

This defines a Deephaven table called `svcUse` and opens it as a separate panel in the IDE, initially showing the first rows of the table. The [`where`](../reference/table-operations/filter/where.md) filter keeps only the rows for `MySvcName`, the same service that we later filter the live feed to.

The tabular view is useful to get an initial sense of the data. Scrolling through the view stays responsive, even though the capture holds one row every 10 seconds for many months.

[`readTable`](../reference/data-import-export/Parquet/readTable.md) does _not_ read all the data from the Parquet files. It loads only the metadata. Deephaven loads table data into memory only when a downstream operation pulls rows, such as scrolling through a table view or computing a derived table. The [`where`](../reference/table-operations/filter/where.md) filter in the same line is one such operation: it reads the `Key` column to find the matching rows, but no other column.

We push the scrollbar to the end of the available range to look at the most recent data. The numbers look comfortably bigger than what we saw in the first block of rows, which reflects organic growth in user adoption for our service. We expect this growth. What we want to flag is use that departs from the expected pattern, not the growth itself.

After a bit more browsing, we switch to a graphical view for more perspective. We want a line graph of the most recent months of data, so we [filter](../reference/table-operations/filter/where.md) on the `Date` partitioning column. Replace the date below with one about four months before your latest data:

```groovy skip-test
svcUseLast4Months = svcUse.where("Date > `2021-05-01`")
```

With the panel for this new table selected, we click **Table Options** and pick [**Chart Builder**](./user-interface/chart-builder.md) from the menu. A simple line chart is enough for now. The graph shows marked seasonality. It looks like a "time of day, day of week" pattern. To confirm our guess, we define a derived table adding a few columns:

```groovy skip-test
import java.time.Instant
import io.deephaven.time.DateTimeUtils

tz = DateTimeUtils.timeZone("ET") // Replace with your time zone

// Truncate to the start of the 10s period
secs = { Instant ts -> 10L * (long) (ts.toEpochMilli() / 10000) }

svcUseDecorated = svcUse.updateView(
    "Secs = (long) secs(Timestamp)",
    "OrdinalDay = (long) (Secs / (24 * 60 * 60))",
    "OrdinalWeek = (long) (OrdinalDay / 7)",
    "SecondsInDay = 10 * (int) (secondOfDay(Timestamp, tz, true) / 10)",
    "DayOfWeek = dayOfWeekValue(Timestamp, tz)"
)
```

`Secs` is the event timestamp in epoch seconds, truncated to the start of its 10-second sampling period. The four-week window in [Build a baseline](#build-a-baseline) filters on it. `OrdinalDay` and `OrdinalWeek` number the days and weeks since the Unix epoch, and `OrdinalWeek` later groups the samples by week.

`SecondsInDay` and `DayOfWeek` are the keys that later match each live sample to its baseline:

- Both use the time zone `tz`.
- The `true` argument makes [`secondOfDay`](../reference/time/datetime/secondOfDay.md) count seconds by local clock time, so a sample at a given local time gets the same keys on either side of a daylight saving time change.
- [`dayOfWeekValue`](../reference/query-language/query-library/auto-imported/time.md) returns the day of the week as a number.
- `SecondsInDay` is truncated to the start of its 10-second sampling period.

Like [`readTable`](../reference/data-import-export/Parquet/readTable.md), [`updateView`](../reference/table-operations/select/update-view.md) does not compute its new columns up front:

- It computes a value only when a later operation, like a UI view or a chained computation, reads it.
- It doesn't copy the data in the pre-existing columns. It reads them from the base table.
- It calls the Groovy closure `secs` inside [formulas](./formulas.md) again every time an operation reads a `Secs` value, including when it computes `OrdinalDay`.

If repeated calls to `secs` are costly, use [`update`](../reference/table-operations/select/update.md) instead, which computes each value once and stores it.

[Filtering](./use-filters.md) this new table and graphing the results confirms the seasonality. Holidays are the exception, because their daily pattern resembles the one for Sundays. A production model would need to account for holidays, but this example ignores them.

To isolate the seasonality effects and more clearly observe the overall trend over time, we keep the `Value` and `OrdinalWeek` columns with [`view`](../reference/table-operations/select/view.md) and total them by week with [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md):

```groovy skip-test
byWeek = svcUseDecorated.view("Value", "OrdinalWeek").sumBy("OrdinalWeek")
```

Graphing the weekly totals in `byWeek`, either with the [Chart Builder](./user-interface/chart-builder.md) or in code with the [plotting API](./plotting/api-plotting.md), gives us a clear picture of the organic service-use growth over time. Weeks are counted in UTC from the Unix epoch, which was a Thursday, so each week runs from Thursday through Wednesday, UTC. The first and last weeks of the capture are usually partial, so their totals are lower. Ignore those two weeks when you read the trend.

### Build a baseline

A complete baseline could overlay two simple models: one that captures the seasonality and one that captures the growth trend. This example builds only the seasonality model.

As a tentative baseline, we build the seasonality model as a derived table. For each time of day and day of week, it averages the matching samples from the last four weeks. That is usually four samples, one per week. After we consume the live feed, we compare each live sample to this baseline in [Compare live data to the baseline](#compare-live-data-to-the-baseline).

Creating this table involves more [aggregations](./combined-aggregations.md) and [filtering](./use-filters.md). The example assumes the Parquet capture is current through the last midnight. The code below:

- Gets the current time with [`now`](../reference/time/datetime/now.md).
- Finds the last midnight in `tz` with [`atMidnight`](../reference/time/datetime/atMidnight.md).
- Keeps the samples from the four weeks before the last midnight with [`where`](../reference/table-operations/filter/where.md).
- Averages them by the `DayOfWeek` and `SecondsInDay` columns with [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) and [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md).

```groovy skip-test
import io.deephaven.time.DateTimeUtils
import static io.deephaven.api.agg.Aggregation.AggAvg

lastMidnight = DateTimeUtils.atMidnight(DateTimeUtils.now(), tz)
// Midnight 28 days earlier on the local calendar, so a daylight saving time change doesn't shift the window
windowStart = lastMidnight.atZone(tz).minusDays(28).toInstant()
lastMidnightSecs = secs(lastMidnight)
windowStartSecs = secs(windowStart)
svcUseLast4Weeks = svcUseDecorated.where(
    "Secs >= windowStartSecs",
    "Secs < lastMidnightSecs"
)

svcUseLast4WeeksAvg = svcUseLast4Weeks.aggBy(
    [AggAvg("Last4Avg = Value")],
    "DayOfWeek", "SecondsInDay"
)
```

### Consume the Kafka feed

We are ready now to get live samples to compare against. One call to [`consumeToTable`](../reference/data-import-export/Kafka/consumeToTable.md) ingests the Kafka feed into a [live](../conceptual/table-types.md#table-type-summary) Deephaven table, and the result is powerful: _the generated table looks and feels like our previous tables for historical data_.

The consumer maps each event in the `ServiceUse` topic to columns:

- The Kafka key holds the service name and becomes the `ServiceName` column.
- The Kafka value is an [Avro](../conceptual/kafka-basic-terms.md#avro) record, as the note under the sample event says. The consumer fetches its schema, `service_use_record`, from the [schema registry](../conceptual/kafka-basic-terms.md#formats) at the `schema.registry.url` address. The record's `Value` field becomes the `Value` column.
- The event's partition, offset, and timestamp become the `KafkaPartition`, `KafkaOffset`, and `KafkaTimestamp` columns. `KafkaTimestamp` holds the same field the historical capture stores as `Timestamp`.

For details on the Avro format, see [Read Kafka topic in Avro format](./data-import-export/kafka-stream.md#read-kafka-topic-in-avro-format). The code below creates the [live table](../conceptual/table-types.md#table-type-summary):

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

The code runs in three stages:

1. The consumer produces a [blink table](../conceptual/table-types.md#specialization-3-blink), which keeps only the rows from the current [update cycle](../conceptual/table-update-model.md).
2. The [`where`](../reference/table-operations/filter/where.md) filter runs on the [blink table](../conceptual/table-types.md#specialization-3-blink). Rows for other services never reach the next stage and disappear at the end of the cycle.
3. [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md) keeps every matching row across cycles, so `liveUse` accumulates every `MySvcName` sample consumed since the table was created.

### Compare live data to the baseline

Now we are ready to decorate the live data with the four-week average `svcUseLast4WeeksAvg` from [Build a baseline](#build-a-baseline). First, we derive the join-key columns `DayOfWeek` and `SecondsInDay` from the `KafkaTimestamp` column:

```groovy skip-test
liveUseWithLast4WeeksAvg = liveUse.updateView(
    "SecondsInDay = 10 * (int) (secondOfDay(KafkaTimestamp, tz, true) / 10)",
    "DayOfWeek = dayOfWeekValue(KafkaTimestamp, tz)"
).naturalJoin(
    svcUseLast4WeeksAvg, "DayOfWeek, SecondsInDay"
).updateView("PredictedDiff = Value - Last4Avg", "PredictedPct = 100 * Value / Last4Avg")
```

The operation above does a [natural join](../reference/table-operations/join/natural-join.md) between the [live table](../conceptual/table-types.md#table-type-summary) and the [static table](../conceptual/table-types.md#static-tables) of historical averages. The [join](../reference/table-operations/join/natural-join.md) matches each live row to its baseline by `DayOfWeek` and `SecondsInDay`, and updates its result incrementally as new rows arrive in the [live table](../conceptual/table-types.md#table-type-summary). The code then adds `PredictedDiff` and `PredictedPct`, which compare each value to its baseline.

Finally, we add a [`where`](../reference/table-operations/filter/where.md) filter that keeps only the rows where the sample differs from the baseline by more than 5%:

```groovy skip-test
useAnomalies = liveUseWithLast4WeeksAvg.where("abs(PredictedPct - 100) > 5")
```

`useAnomalies` is a [live table](../conceptual/table-types.md#table-type-summary) of flagged samples. Each time a new `MySvcName` event arrives that differs from its baseline by more than 5%, a row appears in it.

### How the live tables behave

Three points about [live tables](../conceptual/table-types.md#table-type-summary) matter in this example:

1. [Live tables](../conceptual/table-types.md#table-type-summary) change as new data arrives. Each `MySvcName` event that the consumer reads from the Kafka topic appears in `liveUse`.
2. [Live tables](../conceptual/table-types.md#table-type-summary) and [static tables](../conceptual/table-types.md#static-tables) share one table API. The [`updateView`](../reference/table-operations/select/update-view.md) and [`where`](../reference/table-operations/filter/where.md) calls above are the same operations the historical exploration used, and [`naturalJoin`](../reference/table-operations/join/natural-join.md) joins the [live table](../conceptual/table-types.md#table-type-summary) to a [static table](../conceptual/table-types.md#static-tables) with the same call that joins two [static tables](../conceptual/table-types.md#static-tables). [Blink tables](../conceptual/table-types.md#unsupported-operations) don't support a few operations.
3. Tables derived from [live tables](../conceptual/table-types.md#table-type-summary) by most [query operations](../conceptual/table-operations-overview.md) are also live. Deephaven updates them incrementally where it can, applying the rows added, modified, or removed in the parent table to the previous result instead of recomputing from scratch. For example, the [`where`](../reference/table-operations/filter/where.md) filter that produces `useAnomalies` evaluates only newly arrived rows, rather than refiltering every row that [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md) has kept. See the [Deephaven table update model](../conceptual/table-update-model.md) for details.

## From exploration to modeling to deployment

The [Kafka example](#a-kafka-example-in-deephaven) built a model that flags service-use anomalies by comparing live Kafka samples to a four-week historical average. The next step is to implement and deploy production-quality code that gives the organization a live feed from that model.

Traditionally, as the introduction notes, that step is costly:

- The work changes languages, tools, and processes, and often passes from one person to another. Each change adds friction and cost.
- Every later model refinement or bug fix pays those costs again.
- Separate codebases for modeling and deployment raise the question of whether they implement the same thing, and teams seldom test for it.

But what if the code we wrote to model the problem could also run the production feed? **It can.** Save the same chain of [query operations](../conceptual/table-operations-overview.md) as a script and run it with Deephaven's [Application Mode](./application-mode.md).

A long-running feed needs two changes that the example skips:

- Refresh the baseline on a schedule. The script computes `lastMidnight` once, when it runs, so the four-week window doesn't move forward on its own. A [function-generated table](./function-generated-tables.md) can rebuild the baseline on an interval.
- Bound the [live table](../conceptual/table-types.md#table-type-summary). [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md), as called here, keeps every row it receives. A [ring table](../conceptual/table-types.md#create-a-ring-table-from-a-blink-table) from [`RingTableTools.of`](../reference/table-operations/create/ringTable.md) keeps only the latest rows.

## From live feed to action

What can we do with a feed in Deephaven? We can publish derived feeds, build dashboards that inform decisions, and take action from code.

1. Publish a [live table](../conceptual/table-types.md#table-type-summary) as a Kafka topic with [`produceFromTable`](../reference/data-import-export/Kafka/produceFromTable.md), or let another Deephaven server subscribe to it. [Barrage](../conceptual/what-is-barrage.md), Deephaven's extension of the Arrow Flight protocol, carries the [table update model](../conceptual/table-update-model.md) over the network. Read more in the [Deephaven Core API concept guide](../conceptual/deephaven-core-api.md).
2. Arrange the [live tables](../conceptual/table-types.md#table-type-summary) and plots your Groovy scripts create into dashboards in the Deephaven IDE, alongside [filter controls](./user-interface/filters.md) such as [Input Filters](./user-interface/filters.md#input-filters) and [Dropdown Filters](./user-interface/filters.md#dropdown-filter). Deephaven's dashboarding draws on its experience with risk modeling and compliance monitoring in capital markets.
3. Import other libraries for your language to act on the feed, because your code runs inside the Deephaven data engine. Define and monitor metrics against tolerance thresholds and trigger alerts in your organization's incident response platform. Send notifications to a messaging application. Place orders in an automated ordering system.

## Why use Deephaven with Kafka

1. Explore interactively, build models, and ship the same code. Prototyping a Kafka application in Deephaven also produces the code you deploy.
2. Use one compute model for live and historical data.
3. Build complex answers from the bottom up out of the results of smaller queries.
4. Express intermediate results as tables for the clarity of your model. Operations such as [`where`](../reference/table-operations/filter/where.md) and [`updateView`](../reference/table-operations/select/update-view.md) reuse their parent table's column data instead of copying it.
5. Process streams without explicitly handling batches and time windows.
6. Call your own code from inside a query's [formulas](./formulas.md), not only before or after a query runs. Your code runs in the data engine instead of moving data to your client and back.
7. Run [table operations](../conceptual/table-operations-overview.md) as a script in the engine, or send them from a Deephaven client application to run on the server. See the [Deephaven Core API concept guide](../conceptual/deephaven-core-api.md).

## Related documentation

- [Kafka basic terms](../conceptual/kafka-basic-terms.md)
- [Deephaven's table types](../conceptual/table-types.md)
- [Table operations overview](../conceptual/table-operations-overview.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Designing the Deephaven Core API](../conceptual/deephaven-core-api.md)
- [What is Barrage?](../conceptual/what-is-barrage.md)
- [Connect to a Kafka stream](./data-import-export/kafka-stream.md)
- [Write your own custom parser for Kafka](./data-import-export/write-your-own-custom-parser-for-kafka.md)
- [Read Parquet files into Deephaven tables](./data-import-export/parquet-import.md)
- [Chart Builder](./user-interface/chart-builder.md)
- [Filter controls](./user-interface/filters.md)
- [Built-in plotting API](./plotting/api-plotting.md)
- [Multi-aggregation](./combined-aggregations.md)
- [Filter table data](./use-filters.md)
- [Formulas](./formulas.md)
- [Application Mode](./application-mode.md)
- [Generate tables with Groovy functions](./function-generated-tables.md)
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`atMidnight`](../reference/time/datetime/atMidnight.md)
- [`blinkToAppendOnly`](../reference/table-operations/create/blink-to-append-only.md)
- [`consumeToTable`](../reference/data-import-export/Kafka/consumeToTable.md)
- [`dayOfWeekValue`](../reference/query-language/query-library/auto-imported/time.md)
- [`naturalJoin`](../reference/table-operations/join/natural-join.md)
- [`now`](../reference/time/datetime/now.md)
- [`produceFromTable`](../reference/data-import-export/Kafka/produceFromTable.md)
- [`readTable`](../reference/data-import-export/Parquet/readTable.md)
- [`RingTableTools.of`](../reference/table-operations/create/ringTable.md)
- [`secondOfDay`](../reference/time/datetime/secondOfDay.md)
- [`sumBy`](../reference/table-operations/group-and-aggregate/sumBy.md)
- [`timeZone`](../reference/time/datetime/timeZone.md)
- [`update`](../reference/table-operations/select/update.md)
- [`updateView`](../reference/table-operations/select/update-view.md)
- [`view`](../reference/table-operations/select/view.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`KafkaTools` Javadoc](/core/javadoc/io/deephaven/kafka/KafkaTools.html)
- [`BlinkTableTools` Javadoc](/core/javadoc/io/deephaven/engine/table/impl/BlinkTableTools.html)
- [`ParquetTools` Javadoc](/core/javadoc/io/deephaven/parquet/table/ParquetTools.html)
- [`DateTimeUtils` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html)
