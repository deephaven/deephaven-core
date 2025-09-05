---
title: TableReplayer
---

`TableReplayer` is used to replay historical data with timestamps in a new, in-memory table.

## Syntax

```python syntax
TableReplayer(start_time: Union[Instant, str], end_time: Union[Instant, str]) -> Replayer
```

## Parameters

<ParamTable>
<Param name="start_time" type="Union[Instant, str]">

Historical data start time.

</Param>
<Param name="end_time" type="Union[Instant, str]">

Historical data end time.

</Param>
</ParamTable>

## Returns

A `Replayer` object that can be used to replay historical data.

## Methods

`TableReplayer` supports the following methods:

- [`add_table()`](/core/pydoc/code/deephaven.replay.html#deephaven.replay.TableReplayer.add_table) - Registers a table for replaying and returns the associated replay table.
- [`start()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#start()) - Starts replaying data.
- [`shutdown()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#shutdown()) - Shuts the replayer down.

## Example

The following example creates some fake historical data with timestamps and then replays it.

```python order=result,replayed_result ticking-table
from deephaven import new_table
from deephaven.column import datetime_col, int_col
from deephaven.replay import TableReplayer
from deephaven.time import to_j_instant

result = new_table(
    [
        datetime_col(
            "DateTime",
            [
                to_j_instant("2000-01-01T00:00:01 ET"),
                to_j_instant("2000-01-01T00:00:03 ET"),
                to_j_instant("2000-01-01T00:00:06 ET"),
            ],
        ),
        int_col("Number", [1, 3, 6]),
    ]
)

start_time = to_j_instant("2000-01-01T00:00:00 ET")
end_time = to_j_instant("2000-01-01T00:00:07 ET")

result_replayer = TableReplayer(start_time, end_time)

replayed_result = result_replayer.add_table(result, "DateTime")

result_replayer.start()
```

The following example replays two tables with the same replayer.

```python order=source_1,source_2,replayed_source_1,replayed_source_2 ticking-table
from deephaven.replay import TableReplayer
from deephaven import empty_table

source_1 = empty_table(20).update(["Timestamp = '2024-01-01T08:00:00 ET' + i * SECOND"])
source_2 = empty_table(25).update(
    ["Timestamp = '2024-01-01T08:00:00 ET' + i * (long)(0.8 * SECOND)"]
)

replayer = TableReplayer(
    start_time="2024-01-01T08:00:00 ET", end_time="2024-01-01T08:00:20 ET"
)

replayed_source_1 = replayer.add_table(table=source_1, col="Timestamp")
replayed_source_2 = replayer.add_table(table=source_2, col="Timestamp")
replayer.start()
```

## Related documentation

- [How to replay historical data](../../../how-to-guides/replay-data.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html)
- [Pydoc](/core/pydoc/code/deephaven.replay.html#deephaven.replay.TableReplayer)
