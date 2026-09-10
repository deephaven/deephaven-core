---
title: Understanding the Table API
sidebar_label: The Table API
---

<div className="comment-title">

How table operations work in Deephaven

</div>

Deephaven's Table API lets you filter, transform, join, and aggregate data using a consistent set of operations. This guide explains the core concepts behind the API, helping you write more effective queries and avoid common pitfalls.

> [!NOTE]
> New to Deephaven? Start with [How Deephaven works: A mental model](./deephaven-mental-model.md) for the conceptual foundation this guide builds on. This page assumes you already know why tables don't copy data and why formulas run in the engine, and focuses on the API surface and tradeoffs you'll actually choose between.

For a quick-reference index of available operations and where to find full guides for each, see [Table operations overview](./table-operations-overview.md).

## Tables are immutable

Table operations never modify their source — the original is always safe to keep using. Most operations return a distinct new table; a no-op transformation may occasionally hand back the same object as an optimization, but either way the source is left untouched:

```groovy order=source,filtered
source = emptyTable(5).update("X = ii")
filtered = source.where("X > 2")

// source still has 5 rows
// filtered has 2 rows
```

This differs from some data libraries where operations modify data in place. In Deephaven, you build up results by chaining operations, and each step leaves its input untouched. That result usually isn't a full copy, though — see [Tables are recipes, not data](./deephaven-mental-model.md#tables-are-recipes-not-data) for how the engine shares unchanged columns instead of duplicating them.

**Why immutability matters:**

- **Debugging**: Intermediate tables remain available for inspection.
- **Reuse**: The same source can feed multiple downstream operations without interference.
- **Live updates**: The engine can safely propagate changes through the dependency graph.

## Formulas are strings

Column expressions are written as strings:

```groovy order=result
result = emptyTable(5).update(
    "X = ii",           // ii is the row number (as long)
    "Y = X * 2",        // reference another column
    "Z = Math.sqrt(X)"  // call a built-in function
)
```

These strings are parsed and executed by Deephaven's Java engine. This has several implications:

**Syntax is Java-like**:

- Use `Math.sqrt` for math functions.
- String literals use backticks: `` `hello` ``.
- Ternary expressions: `X > 0 ? X : -X`.

**You can call Groovy closures**, but there's a consideration:

```groovy order=source,result
doubleIt = { x -> x * 2 }

source = emptyTable(5).update("X = ii")
result = source.update("Y = (int)doubleIt(X)")
```

The engine calls your closure once per row — see [Calling Groovy from formulas](./deephaven-mental-model.md#calling-groovy-from-formulas) for the details. For performance-critical code, prefer built-in functions.

**[Query scope](../how-to-guides/query-scope.md) makes variables available:**

```groovy order=result
threshold = 10
multiplier = 2.5d

result = emptyTable(5).update(
    "X = ii",
    "Y = X > threshold ? X * multiplier : 0.0"
)
```

Variables defined in the script are automatically available in formula strings through Deephaven's query scope.

## What's available inside formulas

Inside a formula string, you have access to:

**Column values** — Reference by name:

```groovy syntax
"Total = Price * Quantity"
```

**Special variables**:

- `i` — Row position as `int` (0, 1, 2, ...)
- `ii` — Row position as `long` (for tables with more than 2 billion rows)
- `k` — Internal row key (use cautiously; not the same as row position)

`i`/`ii` are valid on static, append-only, or blink tables; `k` is valid on a slightly broader set — static, add-only (which includes append-only), or blink tables (see [Table types](./table-types.md) for what these mean). A general refreshing table rejects whichever of these it doesn't satisfy, because positions and keys can shift. See [special variables](../reference/query-language/variables/special-variables.md) for the full compatibility matrix.

**Built-in functions** — Math, string manipulation, time operations:

```groovy syntax
"Root = Math.sqrt(X)"
"Upper = Text.toUpperCase()"
"Hour = hourOfDay(Timestamp, timeZone(`America/New_York`), true)"
```

Query scope variables and your own Groovy closures are also available inside formulas — see [Formulas are strings](#formulas-are-strings) above for how those work and their tradeoffs.

## Operations build a dependency graph

When you chain operations, you create a [directed acyclic graph](./dag.md) (DAG) of table dependencies:

```groovy order=source,filtered,enriched,summary
import static io.deephaven.api.agg.Aggregation.AggSum

source = emptyTable(10).update("Category = ii % 3", "Value = ii * 10")

// Each operation creates a node in the graph
filtered = source.where("Value > 20")
enriched = filtered.update("Doubled = Value * 2")
summary = enriched.aggBy([AggSum("Total = Doubled")], "Category")
```

**For static tables**, this is just a convenient way to structure code.

**For live (refreshing) tables**, the graph becomes active:

- When source data changes, updates propagate automatically through all dependent tables.
- You don't re-run your code — the engine handles incremental updates.
- Each downstream table sees a consistent view of the data.

This is why Deephaven can efficiently process real-time data: it typically recomputes only what changed, not the entire result.

## Memory vs computation tradeoffs

The Table API offers several ways to add columns, each with different performance characteristics:

| Operation                                                           | Stores values | Recomputes on access | Best for                                         |
| ------------------------------------------------------------------- | ------------- | -------------------- | ------------------------------------------------ |
| `update`                                                            | Yes           | No                   | Expensive formulas, values accessed repeatedly   |
| `view`                                                              | No            | Yes                  | Simple formulas, memory-constrained environments |
| `select`                                                            | Yes           | No                   | Creating a new table with only specific columns  |
| [`updateView`](../reference/table-operations/select/update-view.md) | No            | Yes                  | Same as `view`, but keeping all original columns |
| [`lazyUpdate`](../reference/table-operations/select/lazy-update.md) | Cached        | When cache misses    | Few unique input values, expensive computation   |

**[`update`](../reference/table-operations/select/update.md)** computes values once and stores them:

```groovy syntax
// Good: complex calculation, accessed many times
result = source.update("Score = expensiveCalculation(A, B, C)")
```

**[`view`](../reference/table-operations/select/view.md)** computes on demand:

```groovy syntax
// Good: simple formula, saves memory
result = source.view("X", "Doubled = X * 2")
```

**[`select`](../reference/table-operations/select/select.md)** is like `update` but only includes specified columns:

```groovy syntax
// Drops all columns except those listed
result = source.select("Symbol", "Total = Price * Quantity")
```

For refreshing tables, this choice also affects update performance. `view` recomputes on every access, while `update` recomputes only when source data changes.

## Same API, different behavior

The same operations work on both static and live tables — see [Static vs. live: understanding mutability](./deephaven-mental-model.md#static-vs-live-understanding-mutability) for the underlying concept. In practice, this means:

```groovy order=null ticking-table
// Static table
staticTable = emptyTable(100).update("X = ii")
staticResult = staticTable.where("X > 50").update("Y = X * 2")

// Live table (updates every second)
liveTable = timeTable("PT1S").update("X = ii")
liveResult = liveTable.where("X > 50").update("Y = X * 2")
```

The code is identical. The difference:

- `staticResult` is computed once and never changes.
- `liveResult` automatically updates as new rows arrive in `liveTable`.

You can check whether a table is live with [`isRefreshing`](../reference/table-operations/metadata/isRefreshing.md):

```groovy syntax
println staticTable.isRefreshing()  // false
println liveTable.isRefreshing()    // true
```

## Related documentation

- [How Deephaven works: A mental model](./deephaven-mental-model.md) — The conceptual foundation this guide builds on
- [Table operations overview](./table-operations-overview.md) — Quick-reference index of available operations
- [Deephaven's design](./deephaven-design.md) — Architecture and update model
- [Table types](./table-types.md) — Static, streaming, blink, and ring tables
- [Javadoc: Table](https://deephaven.io/core/javadoc/io/deephaven/engine/table/Table.html) — Complete Table interface
- [Javadoc: TableOperations](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html) — Operation contracts
