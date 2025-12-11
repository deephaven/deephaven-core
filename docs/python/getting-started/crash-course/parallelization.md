---
title: Query Parallelization
sidebar_label: Parallelization
---

Deephaven automatically parallelizes queries, using multiple CPU cores to dramatically improve performance. Understanding how parallelization works helps you write efficient, high-performance queries.

When you run a query on a multi-core system, Deephaven splits the work across available CPU cores and processes data chunks simultaneously. A query that takes 10 seconds on a single core might complete in 2-3 seconds when parallelized across 4 cores.

This guide shows you how to write queries that leverage parallelization effectively.

## How Deephaven parallelizes queries

Deephaven parallelizes at two levels: across multiple table operations and within individual operations.

### Multiple operations run simultaneously

When you have independent operations, Deephaven processes them in parallel. Consider this query that performs three independent calculations:

```python test-set=parallel
from deephaven import time_table, agg

# Create a ticking data source
trades = time_table("PT1s").update(
    [
        "Symbol = `SYM` + (int)(i % 5)",
        "Price = 100 + randomGaussian(0, 10)",
        "Volume = randomInt(100, 10000)",
    ]
)

# These three operations run simultaneously
high_value = trades.where("Price * Volume > 500000")
by_symbol = trades.agg_by([agg.sum_("TotalVolume = Volume")], "Symbol")
recent = trades.tail(100)
```

When new data arrives, Deephaven processes all three operations at the same time on different CPU cores. The operations are independent, so they don't need to wait for each other.

### Individual operations split work across cores

Within a single operation, Deephaven splits the data into chunks and processes them in parallel:

```python test-set=parallel
from deephaven import empty_table

# Calculate prices for 1 million rows
large_table = empty_table(1_000_000).update(
    ["Price = i * 0.01", "Quantity = i % 1000", "Total = Price * Quantity"]
)
```

For this operation:

1. Deephaven splits the 1 million rows into chunks (typically 4,096 rows each).
2. Each CPU core processes different chunks simultaneously.
3. Results are combined into the final table.

On a 4-core system, this can be ~4x faster than processing all rows sequentially.

## Writing thread-safe queries

Deephaven parallelizes operations automatically when they are **thread-safe**. A thread-safe operation produces correct results regardless of which CPU core processes which rows or in what order.

### Thread-safe patterns

These patterns are always thread-safe and parallelize automatically:

**Column arithmetic** with [`update`](../../reference/table-operations/select/update.md):

```python test-set=safe
from deephaven import empty_table

source = empty_table(100).update(
    ["A = i * 2", "B = i + 10", "C = A * B", "D = sqrt(C)"]
)
```

**String operations** with [`update`](../../reference/table-operations/select/update.md):

```python test-set=safe
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

```python test-set=safe
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

```python test-set=safe
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

These operations are thread-safe because:

- They only use values from the current row.
- They don't modify shared state.
- They don't depend on processing order.
- They produce the same result regardless of which core processes them.

### Operations requiring serial execution

Some operations need rows to be processed in a specific order or access shared state. These require [`.with_serial()`](../../conceptual/query-engine/parallelization.md#using-with_serial-for-selectables) to force sequential, single-threaded execution:

```python test-set=serial order=null
from deephaven import empty_table
from deephaven.table import Selectable

# This counter needs sequential processing
counter = 0


def get_next_id():
    global counter
    counter += 1
    return counter


# Use .with_serial() for ordered execution
col = Selectable.parse("ID = get_next_id()").with_serial()
result = empty_table(100).update(col)
```

Without [`.with_serial()`](../../conceptual/query-engine/parallelization.md#using-with_serial-for-selectables), multiple cores might read and update `counter` simultaneously, producing incorrect results.

**When to use `.with_serial()`**:

- Sequential numbering or counters.
- Operations that depend on row order.
- Accessing non-thread-safe external resources.
- File I/O or logging operations.

**Performance trade-off**: Serial execution is slower because it uses only one core, but correctness comes first. Use serial execution when needed for correctness, and parallelization everywhere else for speed.

## Performance tips

### Prefer stateless operations

Stateless operations parallelize automatically and run faster:

```python test-set=perf
from deephaven import empty_table

source = empty_table(100).update(["Price = i * 10.0", "Quantity = i"])

# ✓ Stateless - parallelizes automatically
result = source.update("Value = Price * Quantity")

# ✗ Stateful - requires .with_serial()
counter = 0


def increment():
    global counter
    counter += 1
    return counter
```

### Use Deephaven's built-in operations

Built-in operations are optimized for parallel execution:

```python test-set=perf
from deephaven import agg, empty_table

source = empty_table(100).update(["Symbol = `ABC`", "Value = i"])

# ✓ Built-in aggregation - highly optimized
result = source.agg_by([agg.sum_("Total = Value")], "Symbol")

# ✗ Custom aggregation - harder to parallelize efficiently
```

### Choose the right table operation

Different operations have different performance characteristics:

- [`update`](../../reference/table-operations/select/update.md): Creates in-memory columns, best for frequently accessed data.
- [`update_view`](../../reference/table-operations/select/update-view.md): Computes on demand, best for large tables where only subsets are accessed.
- [`lazy_update`](../../reference/table-operations/select/lazy-update.md): Memoizes calculations, best when many rows share the same input values.

```python test-set=perf
from deephaven import empty_table

source = empty_table(1_000_000).update("Group = i % 100")

# update: Fast access, uses more memory
result1 = source.update("Squared = Group * Group")

# update_view: Uses less memory, computes on demand
result2 = source.update_view("Squared = Group * Group")

# lazy_update: Efficient for repeated values (100 unique Groups)
result3 = source.lazy_update("Squared = Group * Group")
```

### Leverage the Directed Acyclic Graph (DAG)

Structure queries so independent operations can run in parallel:

```python test-set=dag order=null
from deephaven import time_table, agg

market_data = time_table("PT1s").update(
    [
        "Symbol = `SYM` + (int)(i % 5)",
        "Price = 100 + randomGaussian(0, 10)",
        "Volume = randomInt(100, 10000)",
    ]
)

# These operations run simultaneously
summary = market_data.agg_by(
    [agg.avg("AvgPrice = Price"), agg.sum_("TotalVolume = Volume")], "Symbol"
)

high_volume = market_data.where("Volume > 5000")
recent = market_data.tail(1000)
```

All three derived tables (`summary`, `high_volume`, `recent`) update simultaneously when `market_data` receives new data.

## Next steps

- [Comprehensive parallelization guide](../../conceptual/query-engine/parallelization.md) - Deep dive into parallelization concepts, thread pools, and advanced control
- [Directed Acyclic Graph (DAG)](../../conceptual/dag.md) - Understanding how Deephaven tracks dependencies
- [Table operations reference](../../reference/table-operations/index.md) - Complete reference for all table operations
