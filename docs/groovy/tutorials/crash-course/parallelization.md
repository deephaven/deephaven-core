---
title: Query Parallelization
---

Modern computers have multiple processors (called "cores") that can work simultaneously. Deephaven automatically distributes work across these cores to make queries faster. For example, if your computer has 4 cores and a calculation takes 8 seconds on a single core, Deephaven can complete it in roughly 2 seconds by having all 4 cores work on different parts at the same time.

> [!TIP]
> **Most queries benefit from parallelization automatically.** You don't need to do anything special. This guide explains how parallelization works and covers the uncommon situations where you need to disable it.

## How parallelization works

Deephaven distributes work across cores in two ways:

1. **Across tables**: When your query creates multiple tables, Deephaven computes them at the same time on different cores.
2. **Across rows**: When computing values for a single table, Deephaven divides the rows among cores so each core handles a portion.

### Across tables

When one table feeds into several downstream tables, Deephaven computes those downstream tables simultaneously. In this example, `trades` feeds into three separate tables:

```groovy test-set=parallel order=trades,highValue,bySymbol,recent
import static io.deephaven.api.agg.Aggregation.*

// Create a table that adds a new row every second
trades = timeTable("PT1s").update(
    "Symbol = `SYM` + (int)(i % 5)",
    "Price = 100 + randomGaussian(0, 10)",
    "Volume = randomInt(100, 10000)"
)

// These three tables are computed simultaneously on different cores
highValue = trades.where("Price * Volume > 500000")
bySymbol = trades.aggBy([AggSum("TotalVolume = Volume")], "Symbol")
recent = trades.tail(100)
```

When new data arrives in `trades`, Deephaven updates `highValue`, `bySymbol`, and `recent` at the same time, each on its own core.

### Across rows

When computing values within a single table, Deephaven divides the rows among available cores:

```groovy test-set=parallel order=largeTable
// Calculate values for 1 million rows
largeTable = emptyTable(1_000_000).update(
    "Price = i * 0.01",
    "Quantity = i % 1000",
    "Total = Price * Quantity"
)
```

With 1 million rows and 4 cores, Deephaven assigns roughly 250,000 rows to each core. All four cores compute their rows simultaneously, so the work completes about 4 times faster than if a single core processed all rows sequentially.

## When it works

Parallelization produces correct results when each row can be computed independently. This means the formula for row 50 doesn't need to know anything about row 49 or row 51 - it only uses values from its own row.

These patterns are always safe to parallelize:

**Column arithmetic**:

```groovy test-set=safe order=source
source = emptyTable(100).update(
    "A = i * 2",
    "B = i + 10",
    "C = A * B",
    "D = sqrt(C)"
)
```

**String operations**:

```groovy test-set=safe order=source
source = emptyTable(100).update(
    "FirstName = `User` + i",
    "LastName = `Name` + (i % 10)",
    "FullName = FirstName + ' ' + LastName"
)
```

**Conditional logic**:

```groovy test-set=safe order=source
source = emptyTable(100).update(
    "Value = i * 3.14",
    "Category = Value > 100 ? `High` : `Low`",
    "Tier = Value > 200 ? 1 : (Value > 100 ? 2 : 3)"
)
```

**Built-in functions**:

```groovy test-set=safe order=source,result
source = emptyTable(100).update("Timestamp = '2024-01-01T00:00:00 ET' + 'PT1m' * i")

result = source.update(
    "Hour = hourOfDay(Timestamp, 'ET', false)",
    "Day = dayOfMonth(Timestamp, 'ET')",
    "NextDay = Timestamp + 'P1D'"
)
```

All of these examples share the same property: each row's result depends only on values in that same row. It doesn't matter whether row 50 is computed before or after row 49, or whether they're computed on the same core or different cores - the results are identical either way.

## When it breaks

Parallelization produces incorrect results when a row's calculation depends on something outside that row. Two common cases:

- **Shared state**: The formula reads or modifies a variable that other rows also use. When multiple cores access the same variable simultaneously, they can overwrite each other's changes.
- **Row ordering**: The formula assumes rows are processed in a specific order (e.g., row 1 before row 2). With parallelization, row 2 might be processed before row 1, or both might be processed at the same time.

### Example: a broken counter

Consider a closure that counts how many times it has been called:

```groovy syntax
counter = 0

getNextId = {
    counter += 1
    return counter
}

// INCORRECT: parallel execution corrupts the counter
result = emptyTable(100).update("ID = getNextId()")
```

The intent is for each row to get a unique ID: 1, 2, 3, and so on. But with parallelization, multiple cores call `getNextId()` at the same time. Two cores might both read `counter = 5`, both add 1 to get 6, and both return 6. The result: duplicate IDs and skipped numbers.

### The fix: force sequential processing with `.withSerial()`

The [`.withSerial()`](../../conceptual/query-engine/parallelization.md#serialization) method tells Deephaven to process this formula on a single core, one row at a time, in order:

```groovy
import io.deephaven.api.Selectable
import io.deephaven.api.ColumnName
import io.deephaven.api.RawString
import java.util.concurrent.atomic.AtomicInteger

counter = new AtomicInteger(0)

// Force sequential processing for this formula
col = Selectable.of(ColumnName.of("ID"), RawString.of("counter.getAndIncrement()")).withSerial()
result = emptyTable(100).update([col])
```

**Trade-off**: Sequential processing uses only one core, so it's slower than parallel processing. Only use `.withSerial()` when your formula requires it for correctness.

## Next steps

- [Parallelization in depth](../../conceptual/query-engine/parallelization.md) - More techniques for controlling when formulas run in parallel vs. sequentially
- [How Deephaven tracks table relationships](../../conceptual/dag.md) - How Deephaven determines which tables can update simultaneously
- [Table operations reference](../../reference/table-operations/) - Complete reference for all table operations
