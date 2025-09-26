---
title: partitioned_transform
---

The `partitioned_transform` method applies a transformation function to _two_ partitioned tables in order to return a single partitioned table. Typically, the transformation function will apply one or more table operations before joining the two partitioned tables and returning the result.

> [!NOTE]
> If any table underlying the input partitioned tables change, a corresponding change will propagate to the result.

> [!WARNING]
> Each pair of constituents is processed independently of all other pairs, so care must be taken to ensure that join keys are consistent. For example, if you partition by column `A`, but join on column `B` where some values of column `B` are in one partition and other values in another partition, the Deephaven engine does not match those rows.

> [!WARNING]
> Table operations applied within the transformation function _must_ be done from within an [execution context](../../../conceptual/execution-context.md).

## Syntax

```python syntax
partitioned_transform(
  other: PartitionedTable,
  func: Callable[[Table, Table], Table]
) -> PartitionedTable
```

## Parameters

<ParamTable>
<Param name="other" type="PartitionedTable">

The other `PartitionedTable`, whose constituent tables will be passed in as the second argument to the provided function.

</Param>
<Param name="func" type="Callable[[Table, Table], Table]">

A function that takes two Tables as arguments and returns a new Table.

</Param>
</ParamTable>

## Returns

A `PartitionedTable`.

## Examples

### Basic example

The following example shows how the transformation takes two partitioned tables as input and returns one. It uses `partitioned_transform` to create a new partitioned table. The returned table, however, is just the first one passed into `transformer` as input. This example is not practical - it is only meant to show that the transformation function takes two tables as input and returns one.

```python order=result,t1,t2
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table

t1 = empty_table(10).update(["X = i % 2", "Y = randomDouble(0.0, 100.0)"])
t2 = empty_table(10).update(["X = i % 2", "Z = randomDouble(100.0, 500.0)"])

pt1 = t1.partition_by("X")
pt2 = t2.partition_by("X")

ctx = get_exec_ctx()


def transformer(t1, t2):
    with ctx:
        return t1


pt3 = pt1.partitioned_transform(other=pt2, func=transformer)

result = pt3.merge()
```

### Join two partitioned tables

The following example uses the same tables as the [previous example](#basic-example), but this time, `transformer` joins the two partitioned tables before returning the result.

```python order=result,t1,t2
from deephaven.execution_context import get_exec_ctx
from deephaven import empty_table

t1 = empty_table(10).update(["X = i % 2", "Y = randomDouble(0.0, 100.0)"])
t2 = empty_table(10).update(["X = i % 2", "Z = randomDouble(100.0, 500.0)"])

pt1 = t1.partition_by("X")
pt2 = t2.partition_by("X")

ctx = get_exec_ctx()


def transformer(t1, t2):
    with ctx:
        return t1.join(t2, "X")


pt3 = pt1.partitioned_transform(other=pt2, func=transformer)

result = pt3.merge()
```

### Inexact join

The following example creates a `trades` and a `quotes` table. The `partitioned_transform` applies `pt_asof_join`, which performs an [`aj`](../join/aj.md) on the two tables on the `Ticker` and `Timestamp` columns to find the quote at the time of a trade.

```python order=result,trades,quotes
from deephaven import new_table
from deephaven.column import string_col, int_col, double_col, datetime_col
from deephaven.time import to_j_instant
from deephaven.execution_context import get_exec_ctx

trades = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "AAPL", "IBM", "IBM"]),
        datetime_col(
            "Timestamp",
            [
                to_j_instant("2021-04-05T09:10:00 ET"),
                to_j_instant("2021-04-05T09:31:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:30:00 ET"),
            ],
        ),
        double_col("Price", [2.5, 3.7, 3.0, 100.50, 110]),
        int_col("Size", [52, 14, 73, 11, 6]),
    ]
)

quotes = new_table(
    [
        string_col("Ticker", ["AAPL", "AAPL", "IBM", "IBM", "IBM"]),
        datetime_col(
            "Timestamp",
            [
                to_j_instant("2021-04-05T09:11:00 ET"),
                to_j_instant("2021-04-05T09:30:00 ET"),
                to_j_instant("2021-04-05T16:00:00 ET"),
                to_j_instant("2021-04-05T16:30:00 ET"),
                to_j_instant("2021-04-05T17:00:00 ET"),
            ],
        ),
        double_col("Bid", [2.5, 3.4, 97, 102, 108]),
        int_col("BidSize", [10, 20, 5, 13, 23]),
        double_col("Ask", [2.5, 3.4, 105, 110, 111]),
        int_col("AskSize", [83, 33, 47, 15, 5]),
    ]
)

pt_trades = trades.partition_by("Ticker")
pt_quotes = quotes.partition_by("Ticker")

ctx = get_exec_ctx()


def pt_asof_join(quotes, trades):
    with ctx:
        return trades.aj(quotes, ["Ticker", "Timestamp"])


pt_new = pt_trades.partitioned_transform(other=pt_quotes, func=pt_asof_join)

result = pt_new.merge()
```

## Related documentation

- [Execution context](../../../conceptual/execution-context.md)
- [`empty_table`](../create/emptyTable.md)
- [`partition_by`](../group-and-aggregate/partitionBy.md)
- [`natural_join`](../join/natural-join.md)
- [`join`](../join/join.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.PartitionedTable.partitioned_transform)
