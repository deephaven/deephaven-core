---
title: Kafka Overview
description: Deephaven provides a suite of tools that makes Kafka integration easy. Learn how to connect to Kafka streams, consume messages, and process data in real-time.
hide_table_of_contents: true
---

import { CoreTutorialCard } from '@theme/deephaven/core-docs-components';

<div className="comment-title">

Deephaven provides a suite of tools that makes Kafka integration easy.

</div>

<hr className="margin-bottom--lg" />

<div className="row">

<CoreTutorialCard to="/core/docs/conceptual/kafka-in-deephaven">

## Kafka in Deephaven

</CoreTutorialCard>

<CoreTutorialCard to="/core/docs/how-to-guides/data-import-export/kafka-stream">

## Connect to a Kafka stream

</CoreTutorialCard>

</div>

<div className="comment-title">

Event Stream, meet Real-Time Engine

</div>

Event-based applications have succeeded in providing a path forward for traditional transactional workloads at scale. Many have replaced monolithic database systems, running business operations instead as event processors connected by pub/sub platforms like Kafka. These platforms provide enough delivery guarantees to absorb transactional semantics and recovery provisions, while enabling unprecedented horizontal scaling.

This transformation has created an opportunity. In monolithic systems, you only get to see the end state, the result of a transaction. The information about “how we got there”, the inputs to the transaction itself, the triggers, are never recorded. In contrast, pub/sub systems capture the triggers (since they are already modeled as events), and not only for the main production systems: at a small incremental cost, another application can see the same events. Diagnostics transformed the practice of medicine in the 20th century as blood tests, imaging, endoscopy and biopsies offered physicians an insider's view of a patient's body. A similar revolution is happening today in business operations as they become informed by live data.

But the nature of this analysis challenge is different. Event platforms decouple producers from consumers. Liberated from the need to serialize in front of a single monolithic system door, event flows multiply and adapt much faster to satisfy evolving operational needs. The trend in Kafka deployments is an ever increasing number of operational, system-to-system (1-to-1 or small-n-to-small-n) specific feeds. The problem boundary has moved from how to build scalable processing pipelines that model business operations, to detecting trends, distilling insights, and capturing and disseminating those insights as actionable information via executable models that produce derived feeds in real time. Since trends change quickly, a fast explore-model-deploy cycle is critical.

**Enter Deephaven.** Deephaven was born from the need for fast data-driven R&D cycles for quantitative finance in the capital markets industry. Market trends change quickly. Success is driven not only by the ability to innovate, but by the speed of innovation. It is a handicap to have data exploration, model fitting, backtesting, implementation and deployment done as separate activities by siloed people in languages and tools that don’t mix. Having different representations for streaming and historical data only compounds the problem. The Deephaven engine was developed and has evolved to serve a world where feeds are fundamental and both live and historical data share a common vocabulary. To succeed in that world, Deephaven provides common abstractions for streams and static tables via a unified table operations library, in one’s language of choice, for code running both in-process within the data engine or externally as a client, all while using popular and interoperable data formats.

## A Kafka example in Deephaven

Consider the problem of analyzing usage level anomalies for a service running in the cloud. The backend for this service is deployed as a number of processes running on top of a machine pool, each process producing telemetry data for the requests it has served. For our metric of interest -- service use -- samples are collected as the number of requests received in fixed sampling intervals of 10 seconds. For the purpose of this example, we will assume a consolidated Kafka feed exists that aggregates overall system use in 10 second intervals. A single event in this feed looks like:

| Topic        | Partition | Timestamp | Key         | Value           |
| ------------ | --------- | --------- | ----------- | --------------- |
| `ServiceUse` | `0`       | `t0`      | `MySvcName` | `TotalRequests` |

_`TotalRequests` above indicates the total number of requests received across all machines in the single 10 second period defined by `t0`, encoded as a 64 bit integer._

Aside from the feed itself, a historical data capture for the feed exists in the form of Parquet files, organized via a table directory structure that partitions by day. The historical feed capture adds a `Date` column to the columns listed above, as a simple string in the `'YYYY-MM-DD'` form, which is used as partitioning key.

Our goal is to be able to flag anomalies in overall system use level. The intention is to clearly define periods where the system is outside a baseline of expected use. Finding a suitable definition for this baseline is our first step.

We begin by exploring historical data in the Deephaven Console. We load the historical data with the two lines of Python below:

```python skip-test
from deephaven.parquet import read

svc_use = read("/path/to/parquet/data/root/dir")
```

This defines a Deephaven table called `svc_use` and creates a table view for it as a separate panel in the web UI, initially showing a few scores of rows from the beginning of the table. The tabular view is useful to get an initial sense of the data. Browsing by scrolling through the view is responsive, despite the amount of data involved. We push the scrollbar to the end of the available range to look at the most recent data. The numbers look comfortably bigger than what we saw in the first block of rows, which reflects organic growth in user adoption for our service. This is expected and is what we want to monitor with this work.

It is important to note that when we execute `read` to load the historical data, the statement did _not_ read all the data from the Parquet files, instead only loading the relevant metadata. Actual table data is realized in memory only as operations downstream pull rows from it (such as scrolling through a table view or calculations for derived tables requiring values).

After a bit more browsing, we switch to a graphical view for more perspective. We want a line graph for the last 4 months of data. We create a filtered version of the table:

```python skip-test
svc_use_last4months = svc_use.where(filters=["Date > `20210501`"])
```

With the panel for this new table selected, we click on **Table Options** and pick **Chart Builder** from the menu (alternatively, we can type `svc_use_last4months.plot()`). A simple line chart will do for now. From the graph, we realize the data has marked seasonality; we believe it may follow a “time of the day - day of the week” pattern. To confirm our guess, we define a derived table adding a few columns:

```python skip-test
def secs(ts):
    return 10 * int(ts.getMillis() / 10000)  # Note we round to a 10s period


svc_use_decorated = svc_use.update_view(
    formulas=[
        "OrdinalDay=secs(Timestamp) / (24*60*60)",
        "SecondsInDay=secs(Timestamp) % (24*60*60)",
        "OrdinalWeek=OrdinalDay / 7",
        "DayOfWeek=OrdinalDay % 7",
    ]
)
```

Similarly to how the loading of data from the partitioned Parquet files does not happen until specific rows are pulled by downstream operations, here column values for the result of `update_view` will be computed and materialized in memory only as needed by some later operation, like a UI view or a chained computation. There is also no additional memory cost in the derived table for the pre-existing columns: they exist only as references to row ranges in the base table.

Note also in the code above, the Python native function `secs` is mixed into column expressions. We are giving code to the query engine for future evaluation when calculating query operation results.

Some filtering from this new table combined with graphing confirms our intuition about seasonality; exceptions to this rule happen on holidays where the daily pattern resembles the one for Sundays. We make a note for later to incorporate this complexity in our model, and ignore it for now.

To isolate the seasonality effects and more clearly observe the overall trend over time, we create an aggregation by week for the whole series:

```python skip-test
by_week = svc_use_decorated.view(formulas=["Value", "OrdinalWeek"]).sum_by(
    by=["OrdinalWeek"]
)
```

Graphing this table gives us a clear picture of the organic service use growth over time. We think of overlaying two simple models for our baseline: one for capturing the seasonality, and a second one to capture the growth trend.

We explore the idea by defining several derived tables and looking at graphs and browsing tabular data for them:

- A table to represent the average as defined above as a tentative baseline.
- A table to compare live samples arriving right now to its baseline.

Creating a table to represent the average for the last four values that match time of day and day of week involves doing aggregations and filtering. As table operations go, these are slightly more complicated and a detailed description is beyond the scope of this document. In general terms, the code below restricts samples to the last 4 weeks, and then aggregates by the `SecondsInDay` column:

<!--TODO: link to overviews -->

```python skip-test
import deephaven.time as dhtu

TZ = dhtu.time_zone("ET")  # replace by correct time zone.
LAST_MIDNIGHT_SECS = secs(dhtu.at_midnight(dhtu.now(), TZ))
svc_use_last4weeks = svc_use_decorated.where(
    "secs(Timestamp) >= LAST_MIDNIGHT_SECS - 4*7*24*60*60"
)

from deephaven import agg as agg

svc_use_last4_weeks_avg = svc_use_last4weeks.aggBy(
    [agg.avg(cols=["Last4Avg=Value"])], "OrdinalDay", "SecondsInDay"
)
```

We are ready now to get live samples to compare against. Ingesting the Kafka feed to a live Deephaven table is simple, and the result is powerful: _the generated table is a live table that looks and feels like our previous tables for historical data_.

This example assumes we have an Avro schema defined for the Kafka `Value` field in the `ServiceUse` topic, and we are reading it from a schema service under the name `service_use_record`:

```python skip-test
from deephaven import kafka_consumer as ck
from deephaven.stream.kafka.consumer import TableType, KeyValueSpec
import deephaven.dtypes as dht

live_use = ck.consume(
    {"bootstrap.servers": "kafkahost:9002", "schema.registry.url": "http://regsvchost"},
    "ServiceUse",
    key_spec=ck.simple_spec("ServiceName", dht.string),
    value_spec=ck.avro_spec("service_use_record"),
    table_type=TableType.Append,
).where(filters=["ServiceName =`MySvcName`"])
```

> [!NOTE]
> This query can be written more efficiently by applying the `where` operation before collecting events. As written, `consume` collects events immediately as they arrive (`table_type=TableType.append()`). The more efficient version requires using `table_type=TableType.blink()`, applying the filter, and then using `.blinkToAppendOnly()` after the filter. For more information, see our reference guide for [`consume`](../reference/data-import-export/Kafka/consume.md).

There are a few important points about live tables that deserve more explanation:

1. Live tables are dynamically updated and change as new data arrives. In our example above, as new events are consumed from the Kafka topic, they are reflected in the table.
2. All table methods, operations and functions work identically on live tables as on static tables. No separate vocabularies or concepts.
3. Moreover, derived tables defined by queries on live tables are also live. Query operation results are calculated efficiently by considering previous results and state and incrementally applying row adds, modifies and deletes as appropriate for the operation (see our concept guide on the [Deephaven table update model](../conceptual/table-update-model.md) for details). For example, the filtering done by the `where` operation above is implemented by processing added (and more generally, potentially removed or modified) rows to its parent table. Instead of recomputing the result of the whole filter every time the parent table changes, it updates the previous result with the relevant change information (add, modifies and deletes) from the base table.

Now we are ready to decorate the live data with the last 4 samples average we calculated previously:

```python skip-test
live_use_with_last4weeks_avg = live_use.natural_join(
    svc_use_last4weeks_avg, "SecondsInDay"
).update_view("PredictedDiff=Value-Last4Avg", "PredictedPct=100*Value/Last4Avg")
```

The operation above does a natural join between the live table and the static table containing our calculations of averages from historical data, and then adds two new columns that compare the value to the baseline. Note for the natural join there is no need to specify a time window; the join is a full blown, fully capable join operation that will incrementally recalculate and add to the result as new rows arrive to the live input table.

We finish by defining a live table that only has rows where the sample exceeds the baseline for more than 5% by adding a filter:

```python skip-test
use_anomalies = live_use_with_last4weeks_avg.where(
    filters=["abs(PredictedPct-100) > 5"]
)
```

## From exploration to modeling to deployment

We have a clear model idea developed now. Naturally, our next step is implementation and deployment of production quality code that can give our organization a feed for the model we just created. Traditionally, this will imply change of language, tools and processes, even perhaps including handing the baton from one person to another in the organization, with all the friction and incremental costs implied. These costs are amplified by any future need to refine the model or bug fixing. Separate codebases for modeling and deployment also open the question for how to ensure they implement the same thing (although seldom any testing is done to this effect).

But what if we could run the same code we developed to model the problem to actually implement the resulting feed? **We can.** The same table definitions we used as a chain of query operations can be saved as a script and executed under Deephaven’s [Application Mode](../how-to-guides/application-mode.md).

## Live table (or feed) to action

What can we do with a feed in Deephaven? We can compute derived feeds, we can inform decisions, we can take action.

1. Publish a live table as a Kafka topic or make it available for subscription from another Deephaven data engine process. The data engine implements a specialization of the Arrow Flight protocol that allows extending the efficient Deephaven table update model over the network: [Barrage](https://github.com/deephaven/barrage). Read more about this in our [Deephaven Core API concept guide](../conceptual/deephaven-core-api.md).
2. The Deephaven Code Studio can be scripted to create rich dashboards that include graphs, tabular data, and programmable graphical elements like filter selection widgets and buttons executing arbitrary code. Deephaven has collected significant experience in dashboarding from the complex needs of risk modeling and compliance monitoring in capital markets.
3. Since code runs in the Deephaven data engine as a library accessible from your language of choice, you can import any libraries in that language to integrate functionality. Define and monitor metrics against tolerance thresholds and trigger alerts in your organization’s Incident Response Platform. Send notifications to a messaging application. Place orders in an automated ordering system.

## Try Deephaven

1. Try interactively, generate ideas and create models, ship code: prototype Kafka applications quickly, productize even quicker (it’s already done).
2. Leverage a uniform compute model for live and historical data that enables problem decomposition. Build complex answers from the bottom up from the results of smaller queries. Express intermediate results as tables for the clarity of your model without the memory and computational cost of multiple copies of the data. Move away from explicitly handling batches and time windows for processing streams.
3. Run code not only between queries, but inline with a query, as part of query result computation. Instead of moving the data to your client application and back, embed your code in the data engine, either by running as a script in the engine itself, or as a Deephaven client application operating on table proxies.

## Related documentation

- [Kafka in Deephaven](../conceptual/kafka-in-deephaven.md)
- [Connect to a Kafka stream](./data-import-export/kafka-stream.md)
- [`consume`](../reference/data-import-export/Kafka/consume.md)
