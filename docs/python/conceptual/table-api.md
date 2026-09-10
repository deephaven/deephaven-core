---
title: Understanding the Table API
sidebar_label: The Table API
---

<div className="comment-title">

How table operations work in Deephaven

</div>

Deephaven's Table API lets you filter, transform, join, and aggregate data using a consistent set of operations. This guide explains the core concepts behind the API — understanding these will help you write more effective queries and avoid common pitfalls.

> [!NOTE]
> New to Deephaven? Start with [How Deephaven works: A mental model](./deephaven-mental-model.md) for the conceptual foundation this guide builds on. This page assumes you already know why tables don't copy data and why formulas run in the engine, and focuses on the API surface and tradeoffs you'll actually choose between.

For a quick-reference index of available operations and where to find full guides for each, see [Table operations overview](./table-operations-overview.md).

## Tables are immutable

Table operations never modify their source — the original is always safe to keep using. Most operations return a distinct new table; a no-op transformation may occasionally hand back the same object as an optimization, but either way the source is left untouched:

```python order=source,filtered
from deephaven import empty_table

source = empty_table(5).update("X = ii")
filtered = source.where("X > 2")

# source still has 5 rows
# filtered has 2 rows
```

This differs from libraries like pandas, where operations often modify data in place. In Deephaven, you build up results by chaining operations, and each step leaves its input untouched. That result usually isn't a full copy, though — see [Tables are recipes, not data](./deephaven-mental-model.md#tables-are-recipes-not-data) for how the engine shares unchanged columns instead of duplicating them.

**Why immutability matters:**

- **Debugging**: Intermediate tables remain available for inspection.
- **Reuse**: The same source can feed multiple downstream operations without interference.
- **Live updates**: The engine can safely propagate changes through the dependency graph.

## Formulas are strings

Column expressions are written as strings, not native Python code:

```python order=result
from deephaven import empty_table

result = empty_table(5).update(
    [
        "X = ii",  # ii is the row number (as long)
        "Y = X * 2",  # reference another column
        "Z = Math.sqrt(X)",  # call a built-in function
    ]
)
```

These strings are parsed and executed by Deephaven's Java engine, not Python. This has several implications:

**Syntax is Java-like**, not Python:

- Use `Math.sqrt`, not `math.sqrt`
- String literals use backticks: `` `hello` ``, not `"hello"`
- Ternary expressions: `X > 0 ? X : -X`

**You can call Python functions**, but there's a cost:

```python order=source,result
from deephaven import empty_table


def double_it(x):
    return x * 2


source = empty_table(5).update("X = ii")
result = source.update("Y = (int)double_it(X)")
```

Crossing from Java to Python adds overhead — see [Formulas run in the engine, not in Python](./deephaven-mental-model.md#formulas-run-in-the-engine-not-in-python) for when the engine can batch that crossing per chunk versus falling back to once per row. Either way, for performance-critical code, prefer built-in functions or a Java function.

**[Query scope](../how-to-guides/query-scope.md) makes variables available:**

```python order=result
from deephaven import empty_table

threshold = 10
multiplier = 2.5

result = empty_table(5).update(["X = ii", "Y = X > threshold ? X * multiplier : 0"])
```

Python variables in the local or global scope are automatically available in formula strings through Deephaven's query scope.

## Operations build a dependency graph

When you chain operations, you create a [directed acyclic graph](./dag.md) (DAG) of table dependencies:

```python order=source,filtered,enriched,summary
from deephaven import empty_table, agg

source = empty_table(10).update(["Category = ii % 3", "Value = ii * 10"])

# Each operation creates a node in the graph
filtered = source.where("Value > 20")
enriched = filtered.update("Doubled = Value * 2")
summary = enriched.agg_by([agg.sum_("Total = Doubled")], by=["Category"])
```

**For static tables**, this is just a convenient way to structure code.

**For live (refreshing) tables**, the graph becomes active:

- When source data changes, updates propagate automatically through all dependent tables.
- You don't re-run your code — the engine handles incremental updates.
- Each downstream table sees a consistent view of the data.

This is why Deephaven can efficiently process real-time data: it typically recomputes only what changed, not the entire result.

## Memory vs computation tradeoffs

The Table API offers several ways to add columns, each with different performance characteristics:

| Operation                                                            | Stores values | Recomputes on access | Best for                                         |
| -------------------------------------------------------------------- | ------------- | -------------------- | ------------------------------------------------ |
| `update`                                                             | Yes           | No                   | Expensive formulas, values accessed repeatedly   |
| `view`                                                               | No            | Yes                  | Simple formulas, memory-constrained environments |
| `select`                                                             | Yes           | No                   | Creating a new table with only specific columns  |
| [`update_view`](../reference/table-operations/select/update-view.md) | No            | Yes                  | Same as `view`, but keeping all original columns |
| [`lazy_update`](../reference/table-operations/select/lazy-update.md) | Cached        | When cache misses    | Few unique input values, expensive computation   |

**[`update`](../reference/table-operations/select/update.md)** computes values once and stores them:

```python syntax
# Good: complex calculation, accessed many times
result = source.update("Score = expensiveCalculation(A, B, C)")
```

**[`view`](../reference/table-operations/select/view.md)** computes on demand:

```python syntax
# Good: simple formula, saves memory
result = source.view(["X", "Doubled = X * 2"])
```

**[`select`](../reference/table-operations/select/select.md)** is like `update` but only includes specified columns:

```python syntax
# Drops all columns except those listed
result = source.select(["Symbol", "Total = Price * Quantity"])
```

For refreshing tables, this choice also affects update performance. `view` recomputes on every access, while `update` recomputes only when source data changes.

## What's available inside formulas

Inside a formula string, you have access to:

**Column values** — Reference by name:

```python syntax
"Total = Price * Quantity"
```

**Special variables**:

- `i` — Row position as `int` (0, 1, 2, ...)
- `ii` — Row position as `long` (for tables with more than 2 billion rows)
- `k` — Internal row key (use cautiously; not the same as row position)

`i`/`ii` are valid on static, append-only, or blink tables; `k` is valid on a slightly broader set — static, add-only (which includes append-only), or blink tables. A general refreshing table rejects whichever of these it doesn't satisfy, because positions and keys can shift. See [special variables](../reference/query-language/variables/special-variables.md) for the full compatibility matrix.

**Built-in functions** — Math, string manipulation, time operations:

```python syntax
"Root = Math.sqrt(X)"

"Upper = myString.toUpperCase()"
"Hour = hourOfDay(Timestamp, timeZone(`America/New_York`))"
```

Query scope variables and your own Python functions are also available inside formulas — see [Formulas are strings](#formulas-are-strings) above for how those work and their tradeoffs.

## Same API, different behavior

The same operations work on both static and live tables — see [Static vs. live: understanding mutability](./deephaven-mental-model.md#static-vs-live-understanding-mutability) for the underlying concept. In practice, this means:

```python order=static_result,live_result ticking-table
from deephaven import empty_table, time_table

# Static table
static_table = empty_table(100).update("X = ii")
static_result = static_table.where("X > 50").update("Y = X * 2")

# Live table (updates every second)
live_table = time_table("PT1S").update("X = ii")
live_result = live_table.where("X > 50").update("Y = X * 2")
```

The code is identical. The difference:

- `static_result` is computed once and never changes.
- `live_result` automatically updates as new rows arrive in `live_table`.

You can check whether a table is live with [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md):

```python syntax
print(static_table.is_refreshing)  # False
print(live_table.is_refreshing)  # True
```

## Related documentation

- [How Deephaven works: A mental model](./deephaven-mental-model.md) — The conceptual foundation this guide builds on
- [Table operations overview](./table-operations-overview.md) — Quick-reference index of available operations
- [Deephaven's design](./deephaven-design.md) — Architecture and update model
- [Table types](./table-types.md) — Static, streaming, blink, and ring tables
- [Pydoc: Table](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) — Complete Python API reference
- [Javadoc: TableOperations](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html) — Operation contracts
