---
title: "How Deephaven works: A mental model and patterns of use"
sidebar_label: Patterns of use
---

<div className="comment-title">

Understanding how to think about Deephaven

</div>

Deephaven works differently from tools like SQL or traditional Java data structures — and some of Deephaven's behavior might surprise you. Code that looks straightforward can produce unexpected results. This guide explains _how to think_ about Deephaven so you can write effective queries and avoid common pitfalls.

This isn't a deep technical dive — for that, see [Deephaven's design](./deephaven-design.md). Instead, this guide builds the mental model you need to work productively with Deephaven from day one.

![How Deephaven works: Tables are recipes, not data copies — updates flow automatically](../assets/conceptual/mental-model/mental-model-overview-static.png)

## Tables are recipes, not data

In traditional programming, a data structure is a container holding your data. When you filter or transform it, you get a new container with different data inside.

**Deephaven tables work differently.** A table is more like a _recipe_ — a description of how to compute results from source data. When you call [`where`](../reference/table-operations/filter/where.md) or [`update`](../reference/table-operations/select/update.md), you're not creating a copy with filtered data. You're creating a new recipe that says "take this input and apply this transformation."

```groovy order=source,filtered,doubled
source = emptyTable(5).update("X = i")
filtered = source.where("X > 2")
doubled = source.update("Y = X * 2")
```

Here, `filtered` contains rows where X > 2 (rows 3 and 4), computed immediately when you call `where`. But `filtered` also maintains a _dependency_ on `source` — if `source` changes (because it's connected to live data), `filtered` automatically recomputes and reflects those changes.

**Why this matters:**

- You don't need to re-run your filter when data changes — it happens automatically.
- Multiple transformations can share the same source without duplicating data.
- Operations are typically much faster than copying entire datasets.

When you call a table operation, Deephaven establishes the dependency and computes the initial result. The "recipe" remains active — if the source data changes, downstream tables update automatically without you re-running code. Operations like [`view`](../reference/table-operations/select/view.md) are an exception: they store the formula but defer evaluation until values are actually accessed, which saves memory for columns you rarely read.

## Formulas run in the engine

When you write a formula string like `"Y = X * 2"`, that code runs inside Deephaven's Java-based engine, which is optimized for processing millions of rows efficiently.

```groovy order=result
result = emptyTable(1000).update("X = i", "Y = Math.sqrt(X * X + 1)")
```

The engine parses and executes the string `"Y = Math.sqrt(X * X + 1)"`.

> [!TIP]
> Query strings use Java-style syntax: backticks (`` ` ``) for strings instead of quotes, `PT1S` for durations (1 second), and casts like `(int)` to specify return types.

This has important implications:

- **Java methods available**: Use `Math.sqrt()`, `String` methods like `.toUpperCase()`, and other Java standard library methods.
- **Groovy variables accessible**: Variables from your Groovy script are available inside formulas via the query scope.
- **Much faster**: The engine processes data in optimized batches, not one row at a time.

### Calling Groovy from formulas

You can call Groovy methods and closures from formulas:

```groovy syntax
myCalculation = { x -> x * x + 1 }

result = emptyTable(10).update("Y = (int)myCalculation(i)")
```

Closures assigned to top-level variables are available in query strings. For performance-critical code, prefer engine-native expressions when possible.

## Your code doesn't run row-by-row

This is the most common surprise for new users. Consider this code:

```groovy syntax
counter = 0

nextValue = {
    counter++
    return counter
}

// Don't rely on getting 1, 2, 3, 4, 5...
result = emptyTable(5).update("Value = (int)nextValue()")
```

You might expect `Value` to be `[1, 2, 3, 4, 5]`. But the engine can evaluate rows in _any order_, potentially in _parallel_ across multiple threads. You might get `[3, 1, 4, 2, 5]` or something else entirely — and results may differ between runs.

**The rule:** Formulas should be _stateless_ — the result for row N should depend only on the input values for row N, not on what happened when processing other rows. Immutable variables (like configuration values) are fine; mutable state and order-dependent logic are not.

### Replacing counters with row positions

If you need deterministic row numbering on **static or append-only tables**, use `ii` (the row position) instead of a counter:

```groovy order=result
// Use ii (the row number) instead of a counter
result = emptyTable(5).update("Value = ii + 1")
```

> [!NOTE]
> These variables work on static and certain streaming tables, but general refreshing tables reject them because positions and keys can shift. If you need row identity on a ticking table, use a stable key column instead. See [special variables](../reference/query-language/variables/special-variables.md) for the full compatibility matrix.

For more complex cases involving state, see [parallelization](./query-engine/parallelization.md) and the serial execution options. For details on [`ii` and other special variables](../reference/query-language/variables/special-variables.md), see the reference documentation.

## Static vs. live: understanding mutability

Deephaven tables come in two flavors:

- **Static tables**: Data that doesn't change. Loaded from files, created with [`emptyTable`](../reference/table-operations/create/emptyTable.md) or [`newTable`](../reference/table-operations/create/newTable.md), or snapshots of live data.
- **Refreshing (live) tables**: Data that updates continuously. Created with [`timeTable`](../reference/table-operations/create/timeTable.md), connected to streams, or other real-time sources.

```groovy order=staticTable,liveTable
// Static: this table will always have 10 rows with values 0-9
staticTable = emptyTable(10).update("X = i")

// Live: this table grows by one row every second
liveTable = timeTable("PT1S")  // See timeTable reference for duration syntax
```

**The key insight:** Transformations on live tables produce live results. If you filter a live table, the filtered result updates automatically. When you need to freeze the data at a point in time, use [`snapshot`](../reference/table-operations/snapshot/snapshot.md) to capture a static copy.

```groovy order=liveSource,recentOnly
liveSource = timeTable("PT1S").update("Value = randomInt(0, 100)")
recentOnly = liveSource.where("Value > 50")
// recentOnly continuously updates as new rows arrive and are filtered
```

You don't need to poll for changes or re-run queries — the engine handles propagation automatically.

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

When you extract data back to Groovy, you can copy column values into an array. This is a snapshot, not a live view — changes to the table won't affect the array:

```groovy syntax
import io.deephaven.engine.table.vectors.ColumnVectors

myTable = emptyTable(5).update("X = i * 10")

// Copy column data into an array
xValues = ColumnVectors.of(myTable, "X").copyToArray()
```

For live tables, consider using `snapshot()` to get a static copy at a specific moment in time.

### Performance tips

Moving data between Groovy and Deephaven takes time. For large datasets:

- Keep data in Deephaven tables and use engine operations (fast).
- Avoid repeatedly extracting data in loops (slow).
- Use snapshots strategically, not in tight loops.

## What you can build

Deephaven isn't just a table engine — it's a platform for building data applications.

### Data sources and sinks

| Source           | How to use                                                                             |
| ---------------- | -------------------------------------------------------------------------------------- |
| **Parquet**      | `ParquetTools.readTable("/path/to/file.parquet")`                                      |
| **CSV**          | `CsvTools.readCsv("/path/to/file.csv")`                                                |
| **Kafka**        | [`KafkaTools.consumeToTable`](../reference/data-import-export/Kafka/consumeToTable.md) |
| **Manual entry** | [Input tables](../how-to-guides/input-tables.md) — edit cells in the UI                |
| **Programmatic** | [Table Publisher](../how-to-guides/table-publisher.md) — push data from your code      |

| Destination        | How to use                                                                                 |
| ------------------ | ------------------------------------------------------------------------------------------ |
| **Parquet**        | `ParquetTools.writeTable(table, "/path/to/output.parquet")`                                |
| **Kafka**          | [`KafkaTools.produceFromTable`](../reference/data-import-export/Kafka/produceFromTable.md) |
| **Remote clients** | Connect via Python, Java, JavaScript, or C++ clients                                       |

### Client-server architecture

Deephaven runs as a server. Multiple clients can connect simultaneously:

- **Web UI**: Built-in interactive console and grids
- **Python client**: `from pydeephaven import Session`
- **JavaScript client**: For web applications
- **Java/C++ clients**: For high-performance integrations

Clients that subscribe to the same table see consistent, live updates. Tables can be shared between sessions using shared tickets.

## Common patterns

### Pattern: Prefer engine operations over loops

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

### Pattern: Use `ii` and `i` instead of counters

```groovy order=numbered
// i = row position as int (0, 1, 2...)
// ii = row position as long (use for tables with more than 2 billion rows)
numbered = emptyTable(10).update("RowNumber = ii", "Value = i * 2")
```

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

```groovy order=raw,cleaned,enriched,summary
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

Each intermediate table (`cleaned`, `enriched`) is a first-class object you can display, join, or use as input to further operations.

### Pattern: Same code for batch and streaming

Write your logic once — it works identically on files and live streams:

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
// live = kafkaConsumer(...)
// liveAnalysis = analyzeTrades(live)
```

No need for separate batch and streaming codebases.

### Pattern: Partition large datasets

Split data by key and process each partition efficiently:

```groovy order=trades
import io.deephaven.api.updateby.UpdateByOperation
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.util.SafeCloseable

trades = timeTable("PT0.1S").update(
    "Symbol = (ii % 3 == 0) ? `AAPL` : ((ii % 3 == 1) ? `GOOG` : `MSFT`)",
    "Price = randomDouble(100, 200)"
)

// Partition by symbol
bySymbol = trades.partitionBy("Symbol")

// Capture execution context - required for live partitioned tables
// since new constituents may arrive on update threads
defaultCtx = ExecutionContext.getContext()

transformFunc = { t ->
    try (SafeCloseable ignored = defaultCtx.open()) {
        return t.updateBy(UpdateByOperation.RollingAvg(10, "AvgPrice = Price"))
    }
}

// Apply operations to each partition
transformed = bySymbol.transform(transformFunc)
```

Partitioned tables let you parallelize processing, quickly retrieve subtables by key, and improve filter performance in loops. See [`partitionBy`](../reference/table-operations/group-and-aggregate/partitionBy.md) and [Partitioned tables](../how-to-guides/partitioned-tables.md) for details.

## Pitfalls to avoid

### Pitfall: Stateful functions in formulas

```groovy syntax
// DON'T: Rely on execution order
// cache = [:]
// def lookup(key) {
//     if (!cache.containsKey(key)) {
//         cache[key] = expensiveCalculation(key)
//     }
//     return cache[key]
// }
// result = source.update("Value = lookup(Key)")  // Race conditions!

// DO: Use engine-native operations or proper caching patterns
```

### Pitfall: Extracting data unnecessarily

```groovy syntax
// DON'T: Extract data for every operation
// data = myTable.getColumn("X").getDirect()
// filteredData = data.findAll { it > 10 }
// // ... then convert back to table

// DO: Use Deephaven operations directly
// result = myTable.where("X > 10")
```

## Quick reference

| I want to...               | Use this                                               |
| -------------------------- | ------------------------------------------------------ |
| Check if a table updates   | `table.isRefreshing()`                                 |
| Take a static snapshot     | `table.snapshot()`                                     |
| Edit data manually         | [Input tables](../how-to-guides/input-tables.md)       |
| Push data programmatically | [Table Publisher](../how-to-guides/table-publisher.md) |
| Process by groups          | `partitionBy`                                          |
| Connect remotely           | Python/Java/JS client                                  |

| Engine capability          | What it means                                    |
| -------------------------- | ------------------------------------------------ |
| **Incremental updates**    | Only recompute what changed, not entire datasets |
| **Automatic propagation**  | Downstream tables update when sources change     |
| **Parallel execution**     | Multiple threads process data simultaneously     |
| **Shared data structures** | Filtered views share memory with source tables   |

## Related documentation

- [Quickstart](../getting-started/quickstart.md) — Get Deephaven running
- [Crash course](../getting-started/crash-course/overview.md) — Hands-on tutorial
- [Table types](./table-types.md)
- [Deephaven's live DAG](./dag.md)
- [Create tables](../how-to-guides/new-and-empty-table.md)
- [Select, view, and update](../how-to-guides/use-select-view-update.md)
