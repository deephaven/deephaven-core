---
title: "How Deephaven works: A mental model and patterns of use"
---

<div className="comment-title">

Understanding how to think about Deephaven

</div>

Deephaven works differently from SQL databases and from Java collections, and some of its behavior might surprise you. Code that looks straightforward can produce unexpected results. This guide explains _how to think_ about Deephaven so you can write effective queries and avoid common pitfalls.

This guide isn't a deep technical dive. For that, see [Deephaven's design](./deephaven-design.md). Instead, this guide builds the mental model you need to work productively with Deephaven from day one. The second half of the page puts that model to work: common query patterns, what you can build on the engine, and a quick reference.

<Svg src='../assets/conceptual/mental-model/mental-model-overview.svg' style={{height: 'auto', maxWidth: '1000px'}} />

_For hands-on learning, see the [Crash course](../getting-started/crash-course/overview.md)._

## Tables are recipes, not data

In traditional programming, a data structure is a container holding your data. When you filter or transform it, you get a new container with different data inside.

**Deephaven tables work differently.** When you call [`where`](../reference/table-operations/filter/where.md) or [`update`](../reference/table-operations/select/update.md), Deephaven doesn't copy the source data. Instead, it builds a new table that shares the parent's unchanged columns. The new table computes only what's new, such as the set of rows that pass a filter or a new computed column. It also keeps a live dependency on its parent, so it recomputes automatically when the source changes, without you rerunning any code.

```groovy ticking-table order=null
source = timeTable("PT1S").update("X = ii")
filtered = source.where("X > 2")
doubled = source.update("Y = X * 2")
```

`source` is a [live table](#static-vs-live-tables): its data changes while your program runs. It adds one row for each second that elapses. A slow or delayed [update cycle](./table-update-model.md) can add several of those rows at once.

`ii` is a built-in [special variable](../reference/query-language/variables/special-variables.md) that holds each row's position as a `long`, so `X` numbers the rows. The special variable `i` holds the same position as an `int`.

`filtered` holds the rows of `source` that currently satisfy `X > 2`, and `doubled` adds a `Y` column to every row of `source`. Both tables update automatically as new rows arrive in `source`.

**Why this matters:**

- You don't need to rerun your [filter](../reference/table-operations/filter/where.md) when data changes, because the filtered table updates itself.
- Multiple transformations can share the same source without duplicating data.
- Operations are typically much faster than copying entire datasets.

[`view`](../reference/table-operations/select/view.md) also keeps a dependency on its parent, but it doesn't compute values up front. It stores the formula and evaluates it when values are accessed, which saves memory for columns you rarely read. See [Use `view` for lightweight derived columns](#pattern-use-view-for-lightweight-derived-columns).

## Static vs. live tables

Deephaven tables come in two flavors:

- **Static tables**: Data that doesn't change. Usually loaded from files, created with [`emptyTable`](../reference/table-operations/create/emptyTable.md) or [`newTable`](../reference/table-operations/create/newTable.md), or [snapshots](../reference/table-operations/snapshot/snapshot.md) of live data.
- **Live tables**: Data that updates continuously. Deephaven also calls these refreshing tables. Created with [`timeTable`](../reference/table-operations/create/timeTable.md) or connected to [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md) and other real-time sources.

```groovy ticking-table order=null
// Static: this table always has 10 rows with values 0-9
staticTable = emptyTable(10).update("X = i")

// Live: this table grows by about one row per second
liveTable = timeTable("PT1S")  // See timeTable reference for duration syntax
```

**The key insight:** Most transformations on live tables produce live results. A table derived from a live table, like `filtered` in [Tables are recipes, not data](#tables-are-recipes-not-data), updates automatically. You don't need to poll for changes or rerun queries, because the engine handles propagation for you. [`snapshot`](../reference/table-operations/snapshot/snapshot.md) is a deliberate exception. It takes a live table and returns a static copy of it at a single point in time.

## Formulas run in the engine

When you write a formula string like `"Y = X * 2"`, that code runs inside Deephaven's Java-based engine, which is optimized for processing millions of rows efficiently.

```groovy order=result
result = emptyTable(1000).update("X = i", "Y = Math.sqrt(X * X + 1)")
```

The engine parses the string `"Y = Math.sqrt(X * X + 1)"` and compiles it to Java, so Groovy never evaluates the formula itself.

This has important implications:

- **Java methods are available**: Use `Math.sqrt`, `String` methods like `toUpperCase`, and other Java standard library methods.
- **Groovy variables are available**: Script variables assigned without `def` are available inside formulas through the [query scope](../how-to-guides/query-scope.md).
- **Groovy closures are callable**: Formulas can call [closures](../how-to-guides/groovy-closures.md), at a performance cost described in [Calling Groovy from formulas](#calling-groovy-from-formulas).
- **Engine-native formulas run fast**: Formulas that use only operators, Java methods, and Deephaven's built-in functions are engine-native. The engine compiles them to Java and reads their input columns in bulk chunks, so each row costs a compiled Java call rather than a closure call.

> [!TIP]
> Query strings use Java-style syntax:
>
> - Backticks (`` ` ``) enclose strings.
> - Single quotes enclose a single-character `char` literal, such as `'A'`, or a date-time literal, such as `'PT1S'` (a one-second duration) or `'2024-01-01T00:00:00Z'` (an instant).
> - Casts like `(int)` specify return types.
>
> API arguments are different. When you pass [`timeTable`](../reference/table-operations/create/timeTable.md) its duration as a string, write a plain string, as in `timeTable("PT1S")`. Only a formula needs the single-quoted literal, as in `'PT1S'`.

### Calling Groovy from formulas

You can call [Groovy closures](../how-to-guides/groovy-closures.md) from formulas:

```groovy order=result
myCalculation = { x -> x * x + 1 }

result = emptyTable(10).update("Y = (int)myCalculation(i)")
```

The engine calls a [closure](../how-to-guides/groovy-closures.md) once per row, which is slower than engine-native formulas. For performance-critical code, prefer engine-native formulas.

## Write stateless formulas

The engine doesn't promise to evaluate rows in order, or one at a time. This is the most common surprise for new users. Consider this code:

```groovy syntax
counter = 0

nextValue = {
    counter++
    return counter
}

// Don't rely on getting 1, 2, 3, 4, 5...
result = emptyTable(5).update("Value = (int)nextValue()")
```

You might expect `Value` to be `[1, 2, 3, 4, 5]`, but nothing guarantees it. Two things can break a counter like this one:

- **Parallel evaluation**: When an operation processes many rows at once, the engine can evaluate a formula's rows on multiple threads, so values can come out of order and differ between runs.
- **Live re-evaluation**: On a live table, the engine re-evaluates a formula only for the rows that change, so the counter's values no longer follow the row order.

On a small static table like this one, the values usually come back in order.

**The rule:** Formulas should be _stateless_. The result for row N should depend only on the input values for row N, not on what happened when processing other rows. Immutable variables, such as configuration values, are fine. Shared mutable state, such as a counter that every row updates, and logic that depends on the order rows are processed in are not.

### Replacing counters with row positions

If you need deterministic row numbering on a table whose rows never change, or only ever get added at the end, use [`ii`](../reference/query-language/variables/special-variables.md) (the row position) instead of a counter:

```groovy order=result
// Use ii (the row position) instead of a counter.
// Use ii rather than i for tables with more than 2 billion rows.
result = emptyTable(5).update("Value = ii + 1")
```

> [!NOTE]
> Live tables other than [append-only](./table-types.md#specialization-1-append-only) and [blink](./table-types.md#specialization-3-blink) tables reject [`ii` and `i`](../reference/query-language/variables/special-variables.md) by default, because inserting, removing, or moving rows can shift a row's position. To identify rows in such a table, use a column whose value for each row never changes, such as an ID column. See [special variables](../reference/query-language/variables/special-variables.md#refreshing-table-restrictions) for the full compatibility matrix.

### Replacing running totals with `updateBy`

A running total is also stateful, because each row's value depends on the rows before it. Instead of accumulating it in a Groovy variable, use an [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) cumulative operation such as [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md). The engine computes it in table order and keeps it correct as the table changes:

```groovy order=result
import io.deephaven.api.updateby.UpdateByOperation

result = emptyTable(5).update("X = i + 1").updateBy(UpdateByOperation.CumSum("RunningTotal = X"))
```

For more complex cases involving state, see [Controlling execution order](./query-engine/parallelization.md#controlling-execution-order), which explains how to make the engine evaluate a formula serially.

## Moving data between Groovy and Deephaven

Data lives in two places: Groovy variables and Deephaven tables. Understanding when data moves between them helps you write efficient code.

### From Groovy to Deephaven

When you create a table from Groovy data, the engine copies that data:

```groovy order=myTable
import io.deephaven.engine.util.TableTools

// Data is copied from Groovy into the engine
myTable = TableTools.newTable(
    TableTools.intCol("ID", 1, 2, 3),
    TableTools.stringCol("Name", "A", "B", "C")
)
```

### From Deephaven to Groovy

When you extract data back to Groovy, you can copy column values into an array. The array is a copy, not a live view, so later changes to the table don't affect it. The following example copies the `X` column into an array:

```groovy order=myTable
import io.deephaven.engine.table.vectors.ColumnVectors

myTable = emptyTable(5).update("X = i * 10")

// Copy column data into an array
xValues = ColumnVectors.ofInt(myTable, "X").copyToArray()
```

For live tables, this copy holds the data from one moment in time. The table keeps updating, but your copy doesn't change.

### Performance tips

Moving data between Groovy and Deephaven takes time. For large datasets, avoid extracting data in loops. Keep the work in the engine instead, as the [first pattern below](#pattern-prefer-engine-operations-over-loops) shows.

## Common patterns

### Pattern: Prefer engine operations over loops

You don't need a Groovy loop to compute per-group values. An aggregation such as [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) runs inside the engine over whole batches of rows. On a live table, it updates as rows arrive:

```groovy order=prices,avgBySymbol
import static io.deephaven.api.agg.Aggregation.AggAvg

// Instead of looping in Groovy to calculate averages...
prices = emptyTable(100).update(
    "Symbol = (i % 3 == 0) ? `AAPL` : ((i % 3 == 1) ? `GOOG` : `MSFT`)",
    "Price = randomDouble(100, 200)"
)

// Let the engine do it
avgBySymbol = prices.aggBy([AggAvg("AvgPrice = Price")], "Symbol")
```

See [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) for all aggregation options.

### Pattern: Use `view` for lightweight derived columns

Use [`view`](../reference/table-operations/select/view.md) when you want derived columns without storing them. Use [`update`](../reference/table-operations/select/update.md) when you need the results cached for repeated access or downstream operations.

```groovy order=source,derived
source = emptyTable(1000000).update("X = randomDouble(0, 100)")

// view computes on-demand, doesn't store the result — good for simple derivations
derived = source.view("X", "Doubled = X * 2", "Tripled = X * 3")

// update stores the result — better when you'll use it many times or it's expensive to compute
// stored = source.update("ExpensiveCalc = someComplexFunction(X)")
```

### Pattern: Compose queries step by step

Build complex analytics by chaining simple operations. Each step produces a table you can inspect, reuse, or build on:

```groovy ticking-table order=null
import static io.deephaven.api.agg.Aggregation.AggAvg
import static io.deephaven.api.agg.Aggregation.AggCount

// Start with raw data
raw = timeTable("PT1S").update(
    "Symbol = (ii % 2 == 0) ? `AAPL` : `GOOG`",
    "Price = randomDouble(100, 200)"
)

// Clean it
cleaned = raw.where("Price > 0")

// Enrich it
enriched = cleaned.update("PriceRounded = Math.round(Price)")

// Summarize it
summary = enriched.aggBy([AggAvg("AvgPrice = Price"), AggCount("Count")], "Symbol")

// All four tables update together when new data arrives
```

Each intermediate table (`cleaned`, `enriched`) is a first-class object you can display, [join](../how-to-guides/joins-exact-relational.md), or use as input to further operations.

### Pattern: Same code for batch and streaming

Write your logic once, and run the same table operations on files and on live streams:

```groovy syntax
import io.deephaven.parquet.table.ParquetTools
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggAvg

// This analysis logic...
def analyzeTrades(trades) {
    return trades.where("Quantity > 0")
        .aggBy([AggSum("TotalQty = Quantity"), AggAvg("AvgPrice = Price")], "Symbol")
}

// ...works on historical files
historical = ParquetTools.readTable("/data/trades.parquet")
historicalAnalysis = analyzeTrades(historical)

// ...and on live streams
// live = KafkaTools.consumeToTable(...)
// liveAnalysis = analyzeTrades(live)
```

You don't need separate codebases for batch and streaming data.

### Pattern: Partition large datasets

A [partitioned table](../how-to-guides/partitioned-tables.md) splits a table into subtables, one per key value, so you can process each one independently. Partitioned tables let you process subtables in parallel and retrieve a subtable by key directly, instead of re-running a [filter](../reference/table-operations/filter/where.md) for each key. [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) creates one, and [`transform`](../reference/table-operations/partitioned-tables/transform.md) applies a closure to every subtable:

```groovy ticking-table order=null
import io.deephaven.api.updateby.UpdateByOperation
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

trades = timeTable("PT0.1S").update(
    "Symbol = (ii % 3 == 0) ? `AAPL` : ((ii % 3 == 1) ? `GOOG` : `MSFT`)",
    "Price = randomDouble(100, 200)"
)

// Partition by symbol
bySymbol = trades.partitionBy("Symbol")

defaultCtx = ExecutionContext.getContext()

transformFunc = { t ->
    // Open the script's execution context so the closure's table operations
    // can use the query scope and compile formulas (see Execution context)
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t.updateBy(UpdateByOperation.RollingAvg(10, "AvgPrice = Price"))
    }
}

// transform applies transformFunc to every subtable (constituent), including ones added later
transformed = bySymbol.transform(transformFunc)
```

Here, `transformFunc` adds an `AvgPrice` column to each symbol's subtable: a rolling average of `Price` over that symbol's most recent 10 rows.

When you call [`transform`](../reference/table-operations/partitioned-tables/transform.md) from a script, the closure doesn't inherit the script's [execution context](./execution-context.md), which table operations need to use the query scope or compile formulas. So the closure opens the script's context before it calls [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md).

## What you can build

Deephaven is more than a table engine. It's a platform for building data applications.

### Data sources and sinks

Deephaven reads data from files, streams, and your own code, and writes results back out:

| Source           | How to use                                                                             |
| ---------------- | -------------------------------------------------------------------------------------- |
| **Parquet**      | [`ParquetTools.readTable`](../reference/data-import-export/Parquet/readTable.md)       |
| **CSV**          | [`CsvTools.readCsv`](../reference/data-import-export/CSV/readCsv.md)                   |
| **Kafka**        | [`KafkaTools.consumeToTable`](../reference/data-import-export/Kafka/consumeToTable.md) |
| **Manual entry** | [Input tables](../how-to-guides/input-tables.md) — edit cells in the UI                |
| **Programmatic** | [Table publisher](../how-to-guides/table-publisher.md) — push data from your code      |

| Sink        | How to use                                                                                 |
| ----------- | ------------------------------------------------------------------------------------------ |
| **Parquet** | [`ParquetTools.writeTable`](../reference/data-import-export/Parquet/writeTable.md)         |
| **CSV**     | [`CsvTools.writeCsv`](../reference/data-import-export/CSV/writeCsv.md)                     |
| **Kafka**   | [`KafkaTools.produceFromTable`](../reference/data-import-export/Kafka/produceFromTable.md) |

### Client-server architecture

Deephaven runs as a server. Multiple [clients](./client-server-model.md#client-apis) can connect simultaneously:

- **Web UI**: For interactive work in the built-in console and grids
- **Python client** ([`pydeephaven`](./client-server-model.md#client-apis)): For Python applications outside the server
- **JavaScript client**: For web applications
- **Java/C++/R/Go clients**: For integrations in those languages

Clients that subscribe to the same table see consistent, live updates. You can share tables between sessions with [shared tickets](./what-is-barrage.md#shared-tickets).

## Quick reference

| I want to...                        | Use this                                                                                                                                                    |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Check if a table updates            | [`isRefreshing`](../reference/table-operations/metadata/isRefreshing.md)                                                                                    |
| Take a static snapshot              | [`snapshot`](../reference/table-operations/snapshot/snapshot.md)                                                                                            |
| Number rows without a counter       | [`ii`](../reference/query-language/variables/special-variables.md)                                                                                          |
| Compute a running total             | [`updateBy`](../reference/table-operations/update-by-operations/updateBy.md) with [`CumSum`](../reference/table-operations/update-by-operations/cum-sum.md) |
| Add a column computed on demand     | [`view`](../reference/table-operations/select/view.md)                                                                                                      |
| Aggregate by group                  | [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)                                                                                       |
| Split a table into subtables by key | [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md)                                                                           |
| Connect remotely                    | [A client API](./client-server-model.md#client-apis)                                                                                                        |

| Engine capability          | What it means                                                   |
| -------------------------- | --------------------------------------------------------------- |
| **Incremental updates**    | Typically recompute only what changed, not entire datasets      |
| **Automatic propagation**  | Downstream tables update when sources change                    |
| **Parallel execution**     | Multiple threads process data simultaneously                    |
| **Shared data structures** | Derived tables share unchanged columns with their source tables |

## Related documentation

- [Quickstart](../getting-started/quickstart.md)
- [Crash course](../getting-started/crash-course/overview.md)
- [Table types](./table-types.md)
- [Deephaven's live DAG](./dag.md)
- [Create tables](../how-to-guides/new-and-empty-table.md)
- [Select, view, and update](../how-to-guides/use-select-view-update.md)
