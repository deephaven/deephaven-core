---
title: Query Parallelization
---

Modern computers have multiple processors (called "cores") that can work simultaneously. Deephaven automatically distributes work across these cores to make queries faster by having several cores work on different parts of a calculation at the same time. The actual speedup depends on the workload and scheduling overhead, not just the number of cores.

> [!TIP]
> **Most queries benefit from parallelization automatically.** You don't need to do anything special. This guide explains how parallelization works and covers the uncommon situations where you need to disable it.

## How parallelization works

Deephaven distributes work across cores in three ways:

1. **Across tables**: When multiple tables depend on the same live source, Deephaven's update graph can update them at the same time on different cores as new data arrives.
2. **Across rows**: When computing values for a single table, Deephaven divides the rows among cores so each core handles a portion.
3. **Across columns**: When you compute multiple columns in the same operation, Deephaven can calculate independent columns simultaneously.

### Across tables

When one table feeds into several downstream tables, Deephaven's update graph can update those downstream tables concurrently as new data arrives. In this example, `trades` feeds into three separate tables using [`where`](../../reference/table-operations/filter/where.md), [`agg_by`](../../reference/table-operations/group-and-aggregate/aggBy.md), and [`tail`](../../reference/table-operations/filter/tail.md):

```python test-set=parallel ticking-table order=null
from deephaven import time_table, agg

# Create a table with rows arriving at one-second intervals
trades = time_table("PT1s").update(
    [
        "Symbol = `SYM` + (int)(i % 5)",
        "Price = 100 + randomGaussian(0, 10)",
        "Volume = randomInt(100, 10000)",
    ]
)

# These three tables can update at the same time, on different cores, as new data arrives
high_value = trades.where("Price * Volume > 500000")
by_symbol = trades.agg_by([agg.sum_("TotalVolume = Volume")], "Symbol")
recent = trades.tail(100)
```

When new data arrives in `trades`, Deephaven's update graph makes `high_value`, `by_symbol`, and `recent` eligible to update concurrently — whether they actually run at the same time depends on the update graph's thread pool (sized to your CPU cores by default).

### Across rows

Within a single table, Deephaven splits the data into chunks and processes the chunks in parallel:

```python test-set=parallel order=large_table
from deephaven import empty_table

large_table = empty_table(100).update(
    ["Price = i * 0.01", "Quantity = i % 1000", "Total = Price * Quantity"]
)
```

> [!NOTE]
> This example uses 100 rows for clarity — well below the threshold where Deephaven would actually parallelize it. With 20 million rows and 4 cores, Deephaven would divide each column's computation into four chunks of roughly 5 million rows each, independently for `Price`, `Quantity`, and `Total`. All four cores would compute their chunks simultaneously, so the work would complete faster than if a single core processed all rows sequentially. This assumes the default operation-initialization thread pool, which uses one thread per core; a differently-sized pool changes the chunk count, and scheduling overhead means the speedup is rarely a perfectly linear 4x.

Deephaven only splits a single column's row-wise computation across cores once a table is large enough (at least a few million rows). Below that threshold, that column's own computation runs on a single core, though independent columns and other downstream tables can still run concurrently.

### Across columns

When you compute multiple columns in the same operation, Deephaven can also calculate independent columns at the same time:

```python test-set=parallel order=source
from deephaven import empty_table

source = empty_table(10).update(["A = i * 2", "B = i + 10"])
```

Since `A` and `B` don't depend on each other, Deephaven can compute them on different cores simultaneously.

## When it works

Parallelization produces correct results when each row can be computed independently. This means the formula for row 50 doesn't need to know anything about row 49 or row 51 — it only uses values from its own row.

These patterns are always safe to parallelize:

**Column arithmetic**:

```python test-set=safe order=source
from deephaven import empty_table

source = empty_table(100).update(
    ["A = i * 2", "B = i + 10", "C = A * B", "D = sqrt(C)"]
)
```

**String operations**:

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

**Conditional logic**:

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

All of these examples share the same property: each row's result depends only on values in that same row. It doesn't matter whether row 50 is computed before or after row 49, or whether they're computed on the same core or different cores — the results are identical either way.

## When it breaks

Parallelization produces incorrect results when a row's calculation depends on something outside that row. Two common cases:

- **Shared state**: The formula reads or modifies a variable that other rows also use. When multiple cores access the same variable simultaneously, they can overwrite each other's changes.
- **Row ordering**: The formula assumes rows are processed in a specific order (e.g., row 1 before row 2). With parallelization, row 2 might be processed before row 1, or both might be processed at the same time.

### Example: a broken counter

Consider a function that counts how many times it has been called:

```python syntax
counter = 0


def get_next_id() -> int:
    global counter
    counter += 1
    return counter


# INCORRECT: parallel execution corrupts the counter
result = empty_table(100).update("ID = get_next_id()")
```

The intent is for each row to get a unique ID: 1, 2, 3, and so on. On free-threaded Python builds with a table larger than the default `QueryTable.minimumParallelSelectRows` (about 4.2 million rows), Deephaven may parallelize Python-backed formulas, so multiple cores can call `get_next_id` at the same time. This doesn't throw an error — it silently produces wrong values like:

| ID |
| -- |
| 1  |
| 2  |
| 2  |
| 4  |
| 5  |
| 5  |
| 7  |

> [!NOTE]
> This example uses only 100 rows for clarity. With 100 rows, the formula is evaluated serially by default; the race and duplicate IDs shown above would appear only on a table above the default parallelization threshold (about 4.2 million rows) or with a lowered threshold.

Two cores might simultaneously read `counter = 5`, both add 1 to get 6, and both return 6. The result: duplicate IDs and skipped numbers.

## The fix: force sequential processing with `with_serial`

The [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) method tells Deephaven to process this formula serially: never running concurrently with itself, with rows evaluated one at a time in row-set order:

```python test-set=serial order=result
from deephaven import empty_table
from deephaven.table import Selectable

counter = 0


def get_next_id() -> int:
    global counter
    counter += 1
    return counter


# Force sequential processing for this formula
col = Selectable.parse("ID = get_next_id()").with_serial()
result = empty_table(100).update(col)
```

> [!NOTE]
> This example uses only 100 rows, well below the threshold where Deephaven would actually parallelize it, so it wouldn't show the race from the broken version above even without `with_serial`. Use `with_serial` any time your formula depends on shared state or row order, regardless of table size — parallelization isn't the only way execution order can vary, and `with_serial` is the only thing that guarantees rows are processed one at a time, in order.

**Trade-off**: Sequential processing forgoes the speedup of running rows concurrently across cores, so it's slower than parallel processing. Only use `with_serial` when your formula requires it for correctness.

## Key takeaways

- Deephaven runs formulas in parallel by default — this is fast but requires stateless code.
- Shared state or row-order dependencies cause silent errors with parallelization.
- Use `with_serial` to force sequential execution when your formula needs it.

Most queries just work. If your formulas use only column values and built-in functions, parallelization handles everything automatically — no extra code required.

For more depth — including barriers and other concurrency-control tools — see [query parallelization](../../conceptual/query-engine/parallelization.md).
