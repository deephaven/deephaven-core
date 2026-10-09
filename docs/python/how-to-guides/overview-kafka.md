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

Event Stream, meet Real-Time Engine

</div>

Event-based applications have succeeded in providing a path forward for traditional transactional workloads at scale. Many have replaced monolithic database systems, running business operations instead as event processors connected by pub/sub platforms like Kafka. These platforms provide enough delivery guarantees to absorb transactional semantics and recovery provisions, while enabling unprecedented horizontal scaling.

This transformation has created an opportunity. In monolithic systems, you only get to see the end state, the result of a transaction. The information about "how we got there", the inputs to the transaction itself, the triggers, are never recorded. In contrast, pub/sub systems capture the triggers (since they are already modeled as events), and not only for the main production systems: at a small incremental cost, another application can see the same events. Diagnostics transformed the practice of medicine in the 20th century as blood tests, imaging, endoscopy and biopsies offered physicians an insider's view of a patient's body. A similar revolution is happening today in business operations as they become informed by live data.

But the nature of this analysis challenge is different. Event platforms decouple producers from consumers. Liberated from the need to serialize in front of a single monolithic system door, event flows multiply and adapt much faster to satisfy evolving operational needs. The trend in Kafka deployments is an ever increasing number of operational, system-to-system (1-to-1 or small-n-to-small-n) specific feeds. The problem boundary has moved from how to build scalable processing pipelines that model business operations, to detecting trends, distilling insights, and capturing and disseminating those insights as actionable information via executable models that produce derived feeds in real time. Since trends change quickly, a fast explore-model-deploy cycle is critical.

**Enter Deephaven.** Deephaven was born from the need for fast data-driven R&D cycles for quantitative finance in the capital markets industry. Market trends change quickly. Success is driven not only by the ability to innovate, but by the speed of innovation. It is a handicap to have data exploration, model fitting, backtesting, implementation and deployment done as separate activities by siloed people in languages and tools that don't mix. Having different representations for streaming and historical data only compounds the problem. The Deephaven engine was developed and has evolved to serve a world where feeds are fundamental and both live and historical data share a common vocabulary. To succeed in that world, Deephaven provides common abstractions for streams and static tables via a unified table operations library, in one's language of choice, for code running both in-process within the data engine or externally as a client, all while using popular and interoperable data formats.

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

We begin by exploring historical data in the Deephaven IDE console. We load the historical data with the two lines of Python below:

```python skip-test
from deephaven.parquet import read

svc_use = read("/path/to/parquet/data/root/dir").where(filters=["Key = `MySvcName`"])
```

This defines a Deephaven table called `svc_use` and opens it as a separate panel in the IDE, initially showing the first rows of the table. The [`where`](../reference/table-operations/filter/where.md) filter keeps only the rows for `MySvcName`, the same service that we later filter the live feed to.

The tabular view is useful to get an initial sense of the data. Scrolling through the view stays responsive, even though the capture holds one row every 10 seconds for many months.

[`read`](../reference/data-import-export/Parquet/readTable.md) does _not_ read all the data from the Parquet files. It loads only the metadata. Deephaven loads table data into memory only when a downstream operation pulls rows, such as scrolling through a table view or computing a derived table.

We push the scrollbar to the end of the available range to look at the most recent data. The numbers look comfortably bigger than what we saw in the first block of rows, which reflects organic growth in user adoption for our service. We expect this growth, and it is what we want to monitor.

After a bit more browsing, we switch to a graphical view for more perspective. We want a line graph of the most recent months of data, so we filter on the `Date` partitioning column. Replace the date below with one about four months before your latest data:

```python skip-test
svc_use_last4months = svc_use.where(filters=["Date > `2021-05-01`"])
```

With the panel for this new table selected, we click on **Table Options** and pick [**Chart Builder**](./user-interface/chart-builder.md) from the menu. A simple line chart is enough for now. The graph shows marked seasonality. It looks like a "time of day, day of week" pattern. To confirm our guess, we define a derived table adding a few columns:

```python skip-test
from deephaven.time import to_j_time_zone

TZ = to_j_time_zone("ET")  # Replace with your time zone


def secs(ts) -> int:
    return 10 * (ts.toEpochMilli() // 10000)  # Truncate to the start of the 10s period


svc_use_decorated = svc_use.update_view(
    formulas=[
        "Secs = secs(Timestamp)",
        "OrdinalDay = (long) (Secs / (24 * 60 * 60))",
        "OrdinalWeek = (long) (OrdinalDay / 7)",
        "SecondsInDay = 10 * (int) (secondOfDay(Timestamp, TZ, true) / 10)",
        "DayOfWeek = dayOfWeekValue(Timestamp, TZ)",
    ]
)
```

Like the Parquet read, [`update_view`](../reference/table-operations/select/update-view.md) does not compute its new columns up front. It computes a value only when a later operation, like a UI view or a chained computation, reads it. The derived table also doesn't copy the data in the pre-existing columns: it reads them from the base table.

The code above also calls the Python function `secs` inside column expressions. Because `update_view` doesn't store results, the query engine calls `secs` again every time an operation reads a `Secs` value, including when it computes `OrdinalDay`.

`SecondsInDay` and `DayOfWeek` use the time zone `TZ`, rounded to the 10-second sampling period, so a sample keeps the same keys at the same local time on either side of a daylight saving time change. The baseline and the live data later use the same keys.

Filtering this new table and graphing the results confirms the seasonality. Holidays are the exception, because their daily pattern resembles the one for Sundays. A production model would need to account for holidays, but this example ignores them.

To isolate the seasonality effects and more clearly observe the overall trend over time, we create an aggregation by week for the whole series:

```python skip-test
by_week = svc_use_decorated.view(formulas=["Value", "OrdinalWeek"]).sum_by(
    by=["OrdinalWeek"]
)
```

Graphing the weekly totals in `by_week`, either with the Chart Builder or in code with [Deephaven Express](/core/plotly/docs/), gives us a clear picture of the organic service-use growth over time.

### Build a baseline

A complete baseline could overlay two simple models: one that captures the seasonality and one that captures the growth trend. This example builds only the seasonality model.

We build the seasonality model as a derived table that averages the last four samples matching the same time of day and day of week, as a tentative baseline. After we consume the live feed, we compare each live sample to this baseline in [Compare live data to the baseline](#compare-live-data-to-the-baseline).

Creating this table involves doing [aggregations](./combined-aggregations.md) and [filtering](./use-filters.md). This page doesn't cover these operations in detail. In general terms, the code below restricts samples to the four weeks before the last midnight, and then aggregates by the `DayOfWeek` and `SecondsInDay` columns:

```python skip-test
import jpy
from deephaven.time import dh_now
from deephaven import agg

_DateTimeUtils = jpy.get_type("io.deephaven.time.DateTimeUtils")

LAST_MIDNIGHT = _DateTimeUtils.atMidnight(dh_now(), TZ)
# Midnight 28 days earlier on the local calendar, so a daylight saving time change doesn't shift the window
WINDOW_START = LAST_MIDNIGHT.atZone(TZ).minusDays(28).toInstant()
LAST_MIDNIGHT_SECS = secs(LAST_MIDNIGHT)
WINDOW_START_SECS = secs(WINDOW_START)
svc_use_last4weeks = svc_use_decorated.where(
    filters=[
        "Secs >= WINDOW_START_SECS",
        "Secs < LAST_MIDNIGHT_SECS",
    ]
)

svc_use_last4weeks_avg = svc_use_last4weeks.agg_by(
    [agg.avg(cols=["Last4Avg = Value"])], by=["DayOfWeek", "SecondsInDay"]
)
```

### Consume the Kafka feed

We are ready now to get live samples to compare against. Ingesting the Kafka feed to a live Deephaven table is simple, and the result is powerful: _the generated table is a live table that looks and feels like our previous tables for historical data_.

As the note under the sample event says, each Kafka value in the `ServiceUse` topic is an Avro record. The consumer fetches the record's schema by name, `service_use_record`, from a [schema registry](../conceptual/kafka-basic-terms.md#formats) at the `schema.registry.url` address. See [Read Kafka topic in Avro format](./data-import-export/kafka-stream.md#read-kafka-topic-in-avro-format) for details. The record's `Value` field becomes the `Value` column. The Kafka key holds the service name and becomes the `ServiceName` column:

```python skip-test
from deephaven.stream.kafka import consumer as kc
from deephaven.stream import blink_to_append_only
import deephaven.dtypes as dht

live_use = blink_to_append_only(
    kc.consume(
        {
            "bootstrap.servers": "kafkahost:9092",
            "schema.registry.url": "http://regsvchost:8081",
        },
        "ServiceUse",
        key_spec=kc.simple_spec("ServiceName", dht.string),
        value_spec=kc.avro_spec("service_use_record"),
        table_type=kc.TableType.blink(),
    ).where(filters=["ServiceName = `MySvcName`"])
)
```

The consumer produces a [blink table](../conceptual/table-types.md#specialization-3-blink), which keeps only the rows from the current [update cycle](../conceptual/table-update-model.md), so events that the `where` filter drops stay in it only for that cycle and are not kept. [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md) then keeps every matching row.

There are a few important points about live tables that deserve more explanation:

1. Live tables are dynamically updated and change as new data arrives. In our example above, as new events are consumed from the Kafka topic, they are reflected in the table.
2. All table methods, operations and functions work identically on live tables as on static tables. No separate vocabularies or concepts.
3. Moreover, tables derived from live tables by most query operations are also live. Deephaven updates them incrementally where it can, applying the rows added, modified, or removed in the parent table to the previous result instead of recomputing from scratch. For example, when new rows arrive, the `where` filter above evaluates only those rows instead of filtering the whole table again. See the [Deephaven table update model](../conceptual/table-update-model.md) for details.

### Compare live data to the baseline

Now we are ready to decorate the live data with the four-week average `svc_use_last4weeks_avg` from [Build a baseline](#build-a-baseline). First, we derive the join-key columns `DayOfWeek` and `SecondsInDay` from the `KafkaTimestamp` column that the consumer adds to the live table:

```python skip-test
live_use_with_last4weeks_avg = (
    live_use.update_view(
        formulas=[
            "SecondsInDay = 10 * (int) (secondOfDay(KafkaTimestamp, TZ, true) / 10)",
            "DayOfWeek = dayOfWeekValue(KafkaTimestamp, TZ)",
        ]
    )
    .natural_join(svc_use_last4weeks_avg, on=["DayOfWeek", "SecondsInDay"])
    .update_view(
        formulas=[
            "PredictedDiff = Value - Last4Avg",
            "PredictedPct = 100 * Value / Last4Avg",
        ]
    )
)
```

The operation above does a [natural join](../reference/table-operations/join/natural-join.md) between the live table and the static table of historical averages. It then adds two new columns that compare each value to its baseline. The join matches each live row to its baseline by `DayOfWeek` and `SecondsInDay`. As new rows arrive in the live table, the join updates its result incrementally.

Finally, we add a filter that keeps only the rows where the sample differs from the baseline by more than 5%:

```python skip-test
use_anomalies = live_use_with_last4weeks_avg.where(
    filters=["abs(PredictedPct - 100) > 5"]
)
```

`use_anomalies` is a live table of flagged samples. Each time a new Kafka event arrives that differs from its baseline by more than 5%, a row appears in it.

## From exploration to modeling to deployment

We have a clear model idea developed now. Naturally, our next step is implementation and deployment of production quality code that can give our organization a feed for the model we just created. Traditionally, this will imply change of language, tools and processes, even perhaps including handing the baton from one person to another in the organization, with all the friction and incremental costs implied. These costs are amplified by any future need to refine the model or bug fixing. Separate codebases for modeling and deployment also open the question for how to ensure they implement the same thing (although seldom any testing is done to this effect).

But what if we could run the same code we developed to model the problem to actually implement the resulting feed? **We can.** The same table definitions we used as a chain of query operations can be saved as a script and executed under Deephaven's [Application Mode](../how-to-guides/application-mode.md).

## Live table (or feed) to action

What can we do with a feed in Deephaven? We can compute derived feeds, we can inform decisions, we can take action.

1. [Publish a live table as a Kafka topic](../reference/data-import-export/Kafka/produce.md) or make it available for subscription from another Deephaven data engine process. The data engine implements a specialization of the Arrow Flight protocol that allows extending the efficient Deephaven table update model over the network: [Barrage](https://github.com/deephaven/barrage). Read more about this in our [Deephaven Core API concept guide](../conceptual/deephaven-core-api.md).
2. The Deephaven IDE can be scripted to create rich dashboards that include graphs, tabular data, and programmable graphical elements like filter selection widgets and buttons executing arbitrary code. Deephaven has collected significant experience in dashboarding from the complex needs of risk modeling and compliance monitoring in capital markets.
3. Since code runs in the Deephaven data engine as a library accessible from your language of choice, you can import any libraries in that language to integrate functionality. Define and monitor metrics against tolerance thresholds and trigger alerts in your organization's Incident Response Platform. Send notifications to a messaging application. Place orders in an automated ordering system.

## Try Deephaven

1. Try interactively, generate ideas and create models, ship code: prototype Kafka applications quickly, productize even quicker (it's already done).
2. Leverage a uniform compute model for live and historical data that enables problem decomposition. Build complex answers from the bottom up from the results of smaller queries. Express intermediate results as tables for the clarity of your model without the memory and computational cost of multiple copies of the data. Move away from explicitly handling batches and time windows for processing streams.
3. Run code not only between queries, but inline with a query, as part of query result computation. Instead of moving the data to your client application and back, embed your code in the data engine, either by running as a script in the engine itself, or as a Deephaven client application operating on table proxies.

## Related documentation

- [Kafka basic terms](../conceptual/kafka-basic-terms.md)
- [Connect to a Kafka stream](./data-import-export/kafka-stream.md)
- [Write your own custom parser for Kafka](./data-import-export/write-your-own-custom-parser-for-kafka.md)
- [Application Mode](./application-mode.md)
- [`consume`](../reference/data-import-export/Kafka/consume.md)
- [`produce`](../reference/data-import-export/Kafka/produce.md)
