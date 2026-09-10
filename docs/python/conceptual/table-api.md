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

For a practical tour of available operations, see [Table operations overview](./table-operations-overview.md).

## Tables are immutable

Every table operation returns a **new** table. The original is never modified:

```python order=source,filtered
from deephaven import empty_table

source = empty_table(5).update("X = ii")
filtered = source.where("X > 2")

# source still has 5 rows
# filtered has 2 rows
```

This differs from libraries like pandas, where operations often modify data in place. In Deephaven, you build up results by chaining operations, with each step producing a new table. "New table" doesn't mean a full copy, though — see [Tables are recipes, not data](./deephaven-mental-model.md#tables-are-recipes-not-data) for how the engine shares unchanged columns instead of duplicating them.

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

**Query scope makes variables available:**

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

| Operation     | Stores values | Recomputes on access | Best for                                         |
| ------------- | ------------- | -------------------- | ------------------------------------------------ |
| `update`      | Yes           | No                   | Expensive formulas, values accessed repeatedly   |
| `view`        | No            | Yes                  | Simple formulas, memory-constrained environments |
| `select`      | Yes           | No                   | Creating a new table with only specific columns  |
| `lazy_update` | Cached        | When cache misses    | Few unique input values, expensive computation   |

**`update`** computes values once and stores them:

```python syntax
# Good: complex calculation, accessed many times
result = source.update("Score = expensiveCalculation(A, B, C)")
```

**`view`** computes on demand:

```python syntax
# Good: simple formula, saves memory
result = source.view(["X", "Doubled = X * 2"])
```

**`select`** is like `update` but only includes specified columns:

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

`i`/`ii` and `k` are only valid on static, append-only, or blink tables — a general refreshing table rejects them because positions and keys can shift. See [special variables](../reference/query-language/variables/special-variables.md) for the full compatibility matrix.

**Query scope** — Python variables from local/global scope:

```python syntax
threshold = 100
"Filtered = Value > threshold"
```

**Built-in functions** — Math, string manipulation, time operations:

```python syntax
"Root = Math.sqrt(X)"

"Upper = myString.toUpperCase()"
"Hour = hourOfDay(Timestamp, timeZone(`America/New_York`))"
```

**Your own functions** — With a performance cost for crossing to Python:

```python syntax
def score(a, b):
    return a * 0.7 + b * 0.3


"Score = (double)score(MetricA, MetricB)"
```

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

You can check whether a table is live with `is_refreshing`:

```python syntax
print(static_table.is_refreshing)  # False
print(live_table.is_refreshing)  # True
```

## Related documentation

- [How Deephaven works: A mental model](./deephaven-mental-model.md) — The conceptual foundation this guide builds on
- [Table operations overview](./table-operations-overview.md) — Practical guide to available operations
- [Deephaven's design](./deephaven-design.md) — Architecture and update model
- [Table types](./table-types.md) — Static, streaming, blink, and ring tables
- [Pydoc: Table](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) — Complete Python API reference
- [Javadoc: TableOperations](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html) — Operation contracts
