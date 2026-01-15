---
title: Query Parallelization
---

Deephaven automatically uses multiple CPU cores to speed up your queries through parallelization. A calculation that takes 10 seconds on one core might finish in 2-3 seconds when spread across 4 cores.

> [!TIP] > **Most queries need no changes to benefit from parallelization.** Deephaven parallelizes by default. The techniques in this guide are only needed for rare cases where you must control or limit parallelization - such as when using counters or accessing external resources that require sequential processing.

## How parallelization works

Deephaven parallelizes at two levels:

1. **Between tables**: When multiple tables need to update, Deephaven calculates them simultaneously on different cores.
2. **Within a table**: When calculating a single table's cells, Deephaven spreads the rows across multiple cores.

### Between tables

When you have independent tables, Deephaven computes them in parallel. In this example, three tables all derive from `trades`:

```python test-set=parallel order=trades,high_value,by_symbol,recent
from deephaven import time_table, agg

# Create a ticking data source
trades = time_table("PT1s").update(
    [
        "Symbol = `SYM` + (int)(i % 5)",
        "Price = 100 + randomGaussian(0, 10)",
        "Volume = randomInt(100, 10000)",
    ]
)

# These three tables compute simultaneously
high_value = trades.where("Price * Volume > 500000")
by_symbol = trades.agg_by([agg.sum_("TotalVolume = Volume")], "Symbol")
recent = trades.tail(100)
```

When new data arrives in `trades`, Deephaven computes all three derived tables at the same time on different CPU cores.

### Within a table

Within a single table, Deephaven splits the data into chunks and processes the chunks in parallel:

```python test-set=parallel order=large_table
from deephaven import empty_table

# Calculate prices for 1 million rows
large_table = empty_table(1_000_000).update(
    ["Price = i * 0.01", "Quantity = i % 1000", "Total = Price * Quantity"]
)
```

With 1 million rows and 4 CPU cores, each core calculates ~250,000 rows. The work finishes roughly 4x faster than if one core did everything.

## Most queries just work

Most queries parallelize correctly without any changes. The key requirement: **each row's calculation must not depend on other rows or external variables that change**.

These common patterns always parallelize correctly:

**Column arithmetic** with [`update`](../../reference/table-operations/select/update.md):

```python test-set=safe order=source
from deephaven import empty_table

source = empty_table(100).update(
    ["A = i * 2", "B = i + 10", "C = A * B", "D = sqrt(C)"]
)
```

**String operations** with [`update`](../../reference/table-operations/select/update.md):

```python test-set=safe order=source
from deephaven import empty_table

source = empty_table(100).update(
    [
        "FirstName = `User` + i",
        "LastName = `Name` + (i % 10)",
        "FullName = FirstName + ' ' + LastName",
    ]
)
```

**Conditional logic** with [`update`](../../reference/table-operations/select/update.md):

```python test-set=safe order=source
from deephaven import empty_table

source = empty_table(100).update(
    [
        "Value = i * 3.14",
        "Category = Value > 100 ? `High` : `Low`",
        "Tier = Value > 200 ? 1 : (Value > 100 ? 2 : 3)",
    ]
)
```

**Built-in functions**:

```python test-set=safe order=source,result
from deephaven import empty_table

source = empty_table(100).update("Timestamp = '2024-01-01T00:00:00 ET' + 'PT1m' * i")

result = source.update(
    [
        "Hour = hourOfDay(Timestamp, 'ET', false)",
        "Day = dayOfMonth(Timestamp, 'ET')",
        "NextDay = Timestamp + 'P1D'",
    ]
)
```

These work because each row's result depends only on that row's values. It doesn't matter which core calculates which row, or in what order - the answer is the same.

## When parallel execution causes problems

Some code produces incorrect results when run in parallel. This happens when:

- **The code modifies external variables.** If multiple cores read and write the same variable simultaneously, they interfere with each other.
- **The result depends on processing order.** If row 5's result depends on row 4 being processed first, parallel execution breaks this assumption.

### Example: A counter that breaks

This counter function modifies an external variable:

```python syntax
counter = 0

def get_next_id():
    global counter
    counter += 1
    return counter

# This will produce INCORRECT results
result = empty_table(100).update("ID = get_next_id()")
```

With parallel execution, multiple cores call `get_next_id()` simultaneously. Two cores might both read `counter = 5`, both increment to `6`, and both return `6` - skipping numbers and producing duplicates.

### How to fix it: Use `.with_serial()`

The [`.with_serial()`](../../conceptual/query-engine/parallelization.md#serialization) method forces a formula to run on one core, processing rows one at a time in order:

```python test-set=serial order=result
from deephaven import empty_table
from deephaven.table import Selectable

counter = 0


def get_next_id():
    global counter
    counter += 1
    return counter


# .with_serial() ensures correct sequential execution
col = Selectable.parse("ID = get_next_id()").with_serial()
result = empty_table(100).update(col)
```

**Trade-off**: Serial execution uses only one core, so it's slower. Use it only when correctness requires it.

## Next steps

- [Parallelization in depth](../../conceptual/query-engine/parallelization.md) - Advanced control over parallelization, including barriers and thread pool configuration
- [How Deephaven tracks dependencies](../../conceptual/dag.md) - Understanding which operations can run in parallel
- [Table operations reference](../../reference/table-operations/) - Complete reference for all table operations
