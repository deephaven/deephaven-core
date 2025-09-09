---
title: Performance tables cheat sheet
sidebar_label: Performance tables
---

Deephaven makes several performance log tables available.

> [!NOTE]
> The word "log" in this context refers to the append-only nature of these tables, not to
> log files in the traditional sense.
>
> These tables track various metrics across Deephaven, and in normal
> circumstances will be populated with raw performance data
> pertaining to your running queries and server state.

```python order=upl,qopl,qpl,pml,pil,ssl,qptt,qoptt
import deephaven.perfmon as pm

upl = pm.update_performance_log()
qopl = pm.query_operation_performance_log()
qpl = pm.query_performance_log()
pml = pm.process_metrics_log()
pil = pm.process_info_log()
ssl = pm.server_state_log()
qptt = pm.query_performance_tree_table()
qoptt = pm.query_operation_performance_tree_table()
```

Several derived tables are defined on top of them, typically by removing the suffix `_log` from the name; these derived tables
accumulate data and present it in more user-friendly column types (e.g., time units in fractional seconds instead of nanoseconds for
CPU time metrics), and as such are more useful for dashboards, where the `_log` tables are more useful as sources of raw data
for user-defined computation of performance statistics. For the derived tables in the list below that are related to query performance,
the functions that create them take one query evaluation number id as a parameter to show results for a particular query (where the `_log`
tables show data for all queries).

```python order=qup,qp,qop,ss
import deephaven.perfmon as pm

qup = pm.query_update_performance(1)
qp = pm.query_performance(1)
qop = pm.query_operation_performance(1)
ss = pm.server_state()
```

## Barrage Performance

Barrage is the name of our IPC table transport. There are two sets of statistics. The `subscription`
metrics are recorded for ticking subscriptions. The `snapshot` metrics are recorded for one-off
requests such as via an Arrow Flight `DoGet`.

Tables are identified by their `TableId` and `TableKey`. The `TableId` is determined by the source table's
`System.identityHashCode()`. The `TableKey` defaults to `Table#getDescription()` but can be overridden by setting a
table attribute (`withAttributes`)

```python order=null
import jpy
from deephaven import empty_table

t = empty_table(0)
table = jpy.get_type("io.deephaven.engine.table.Table")
attr_table = t.with_attributes({table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE: "MyTableKey"})
```

> [!NOTE]
> The web client applies transformations to every table that it subscribes to. If a table is also subscribed by a non-web
> client then statistics for the original table and the transformed table will both appear in the metrics table. Their
> `TableId` will differ. Most transformations clear the `TableKey` attribute, such as when a column is sorted or filtered
> by the user through the GUI.

Here are server-side flags that change the behavior of barrage metrics.

- `-DBarragePerformanceLog.enableAll`: record metrics for tables that do not have an explicit `TableKey` (default: `true`)
- `-DBarragePerformanceLog.cycleDurationMillis`: the interval to flush aggregated statistics (default: `60000` - once per minute)

You can access these tables as follows:

```python order=subs,snaps
import jpy

bpl = jpy.get_type(
    "io.deephaven.extensions.barrage.BarragePerformanceLog"
).getInstance()
subs = bpl.getSubscriptionTable()
snaps = bpl.getSnapshotTable()
```
