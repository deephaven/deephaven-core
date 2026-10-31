---
title: Understanding the Table API
---

<div className="comment-title">

How table operations work in Deephaven

</div>

Deephaven's Table API lets you [filter](../how-to-guides/use-filters.md), [transform](../how-to-guides/use-select-view-update.md), [aggregate](./aggregations.md), and join data through one consistent set of operations. The same operations work whether the data is static or updating in real time. Joins come in two families: [exact and relational joins](../how-to-guides/joins-exact-relational.md) and [time-series and range joins](../how-to-guides/joins-timeseries-range.md).

This guide explains the core concepts behind the API, helping you write more effective queries and choose between the overlapping ways to add columns.

> [!NOTE]
> New to Deephaven? Start with [How Deephaven works: A mental model](./deephaven-mental-model.md) for the conceptual foundation this guide builds on. This page assumes you already know why tables don't copy data and why formulas run in the engine. It focuses on the API itself and the tradeoffs you'll actually choose between.

For a quick-reference index of available operations and where to find full guides for each, see [Table operations overview](./table-operations-overview.md).

## Tables are immutable

Table operations never modify their source, so the original is always safe to keep using. A [live table's](./deephaven-mental-model.md#static-vs-live-tables) rows still change as new data arrives, but no operation changes the table you pass to it.

Most operations return a distinct new table. As an optimization, a no-op transformation can return the same object. Either way, the operation doesn't change the source:

```python order=source,filtered
from deephaven import empty_table

source = empty_table(5).update("X = ii")  # ii is the row number
filtered = source.where("X > 2")

# source still has 5 rows
# filtered has 2 rows
```

This differs from libraries like pandas, where assigning to a column (for example, `df["x"] = ...`) modifies the DataFrame in place. In Deephaven, you build up results by chaining operations, and each step leaves its input untouched.

Each step's output usually isn't a full copy, though. [Tables are recipes, not data](./deephaven-mental-model.md#tables-are-recipes-not-data) explains how the engine shares unchanged columns instead of duplicating them.

**Why immutability matters:**

- **Debugging**: Intermediate tables remain available for inspection.
- **Reuse**: The same source can feed multiple downstream operations without interference.
- **Live updates**: When source data changes, the engine can safely propagate those changes through the [dependency graph](./dag.md) of tables built from it.

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

Each table depends on the one before it. If `source` were live, `filtered`, `enriched`, and `summary` would all update whenever it changed.

As [Static vs. live](./deephaven-mental-model.md#static-vs-live-tables) in the mental model guide shows, the same operations work on both static and [live tables](./deephaven-mental-model.md#static-vs-live-tables). Here is what that means for the graph:

```python test-set=1 ticking-table order=null
from deephaven import empty_table, time_table

# Static table
static_table = empty_table(100).update("X = ii")
static_result = static_table.where("X > 50").update("Y = X * 2")

# Live table (updates every second)
live_table = time_table("PT1S").update("X = ii")
live_result = live_table.where("X > 50").update("Y = X * 2")
```

The code is identical for static and [live tables](./deephaven-mental-model.md#static-vs-live-tables), but the graph behaves differently. The engine computes `static_result` once, and it never changes. For a [static table](./deephaven-mental-model.md#static-vs-live-tables), the graph is simply a convenient way to structure your code.

For `live_result`, the graph stays active. When `live_table` gets new rows:

- Updates propagate automatically through every dependent table.
- You don't re-run your code. The engine handles the incremental updates.
- Each downstream table sees a consistent view of the data.

Deephaven processes real-time data efficiently because it typically recomputes only what changed, not the entire result.

To check which kind you have, use [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md):

```python test-set=1 order=:log
print(static_table.is_refreshing)  # False
print(live_table.is_refreshing)  # True
```

## Formulas are strings

You write column expressions, called [formulas](../how-to-guides/formulas.md), as strings, not native Python code:

```python order=result
from deephaven import empty_table

result = empty_table(5).update(
    [
        "X = ii",  # ii is the row number (as long)
        "Y = X * 2",  # reference another column
        "Z = Math.sqrt(X)",  # call a Java method
    ]
)
```

Deephaven's Java engine, not Python, parses and executes these strings, so the syntax is Java-like:

- Call [Java methods](../how-to-guides/java-classes.md) such as `Math.sqrt`, not Python's `math.sqrt`.
- Use backticks for [string literals](../how-to-guides/string-char-literals.md): `` `hello` ``. Java double quotes also work, but you must escape them inside a double-quoted Python string.
- Use the [ternary operator](../how-to-guides/ternary-if-how-to.md) for conditionals: `X > 0 ? X : -X`.

[What's available inside formulas](#whats-available-inside-formulas) covers the other things a formula can use, including Deephaven's [built-in functions](../how-to-guides/built-in-functions.md).

## What's available inside formulas

Inside a [formula string](../how-to-guides/formulas.md), you have access to:

**Column values**, referenced by name:

```python syntax
"Total = Price * Quantity"
```

**Special variables**:

- [`i`](../reference/query-language/variables/special-variables.md) — Row position as `int` (0, 1, 2, ...)
- [`ii`](../reference/query-language/variables/special-variables.md) — Row position as `long` (for tables with more than 2 billion rows)
- [`k`](../reference/query-language/variables/special-variables.md) — Internal row key, which isn't necessarily the same as row position

[Live tables](./deephaven-mental-model.md#static-vs-live-tables) restrict where you can use these variables. See [Special variables on live tables](#special-variables-on-live-tables).

**[Built-in functions](../how-to-guides/built-in-functions.md)**, such as math, parsing, and time functions:

```python syntax
"Magnitude = abs(X)"

"Number = parseInt(Text)"
"Hour = hourOfDay(Timestamp, timeZone(`America/New_York`), true)"
```

**[Java methods](../how-to-guides/java-classes.md)** on column values and Java classes such as `Math`:

```python syntax
"Upper = Text.toUpperCase()"
```

**[Your own Python functions](../how-to-guides/python-functions.md)**, at a performance cost:

```python order=source,result
from deephaven import empty_table


def double_it(x: int) -> int:
    return x * 2


source = empty_table(5).update("X = ii")
result = source.update("Y = double_it(X)")
```

Each call from Java into Python adds overhead. [Calling Python from formulas](./deephaven-mental-model.md#calling-python-from-formulas) explains when the engine batches those calls and when it makes one call per row. Either way, for performance-critical code, prefer [built-in functions](../how-to-guides/built-in-functions.md).

**[Query scope](../how-to-guides/query-scope.md) variables**:

```python order=result
from deephaven import empty_table

threshold = 10
multiplier = 2.5

result = empty_table(5).update(["X = ii", "Y = X > threshold ? X * multiplier : 0.0"])
```

Python variables in the local or global scope are automatically available in [formula strings](../how-to-guides/formulas.md) through Deephaven's [query scope](../how-to-guides/query-scope.md).

### Special variables on live tables

On a [static table](./deephaven-mental-model.md#static-vs-live-tables), formulas in [`update`](../reference/table-operations/select/update.md), [`select`](../reference/table-operations/select/select.md), [`view`](../reference/table-operations/select/view.md), and [`where`](../reference/table-operations/filter/where.md) can use [`i`](../reference/query-language/variables/special-variables.md), [`ii`](../reference/query-language/variables/special-variables.md), and [`k`](../reference/query-language/variables/special-variables.md) without restriction.

On a [live table](./deephaven-mental-model.md#static-vs-live-tables), row positions and keys can shift. By default, the engine rejects a formula that uses these variables unless the table is one of these types:

- [`i`](../reference/query-language/variables/special-variables.md) and [`ii`](../reference/query-language/variables/special-variables.md): [append-only](./table-types.md#specialization-1-append-only) and [blink](./table-types.md#specialization-3-blink) tables.
- [`k`](../reference/query-language/variables/special-variables.md): [add-only](./table-types.md#specialization-2-add-only) tables (including [append-only](./table-types.md#specialization-1-append-only) tables) and [blink](./table-types.md#specialization-3-blink) tables.

A [time table](../reference/table-operations/create/timeTable.md) is [append-only](./table-types.md#specialization-1-append-only), which is why `live_table` in the [dependency graph example](#operations-build-a-dependency-graph) can use [`ii`](../reference/query-language/variables/special-variables.md).

Some operations, such as [formula aggregations](../reference/table-operations/group-and-aggregate/AggFormula.md), reject these variables on every table, static or live.

See [special variables](../reference/query-language/variables/special-variables.md#refreshing-table-restrictions) for the error message and array-access rules, and [Table types](./table-types.md) for what each table type is.

## Choose how to add columns: memory vs. computation

The Table API offers several ways to add columns, each with different performance characteristics:

| Operation                                                            | Stores values | Recomputes on access | Keeps other columns | Best for                                         |
| -------------------------------------------------------------------- | ------------- | -------------------- | ------------------- | ------------------------------------------------ |
| [`update`](../reference/table-operations/select/update.md)           | Yes           | No                   | Yes                 | Expensive formulas, values accessed repeatedly   |
| [`select`](../reference/table-operations/select/select.md)           | Yes           | No                   | No                  | Expensive formulas, values accessed repeatedly   |
| [`view`](../reference/table-operations/select/view.md)               | No            | Yes                  | No                  | Simple formulas, memory-constrained environments |
| [`update_view`](../reference/table-operations/select/update-view.md) | No            | Yes                  | Yes                 | Simple formulas, memory-constrained environments |
| [`lazy_update`](../reference/table-operations/select/lazy-update.md) | Cached        | When cache misses    | Yes                 | Few unique input values, expensive computation   |

- **[`update`](../reference/table-operations/select/update.md)** computes values up front and stores them in memory.
- **[`select`](../reference/table-operations/select/select.md)** is like [`update`](../reference/table-operations/select/update.md) but only includes the columns you list.
- **[`view`](../reference/table-operations/select/view.md)** computes values on demand instead of storing them, and only includes the columns you list.
- **[`update_view`](../reference/table-operations/select/update-view.md)** is like [`view`](../reference/table-operations/select/view.md) but keeps all existing columns.

The following example adds the same `Total` column with [`update`](../reference/table-operations/select/update.md), [`select`](../reference/table-operations/select/select.md), and [`view`](../reference/table-operations/select/view.md). The `Total` values are identical in all three results. The difference is which columns each result keeps: `result_update` keeps `Price` and `Quantity`, while `result_select` and `result_view` keep only `Symbol` and `Total`.

```python order=source,result_update,result_select,result_view
from deephaven import empty_table

source = empty_table(5).update(
    [
        "Symbol = ii % 2 == 0 ? `AAPL` : `MSFT`",
        "Price = 100.0 + ii",
        "Quantity = (int)(10 * (ii + 1))",
    ]
)

# update stores the new column and keeps every existing column
result_update = source.update("Total = Price * Quantity")

# select stores values but keeps only the columns listed
result_select = source.select(["Symbol", "Total = Price * Quantity"])

# view keeps only the columns listed and computes Total each time it's read
result_view = source.view(["Symbol", "Total = Price * Quantity"])
```

**[`lazy_update`](../reference/table-operations/select/lazy-update.md)** computes a value the first time it sees a given set of inputs and caches the result. A later row with the same inputs reuses the cached value, so [`lazy_update`](../reference/table-operations/select/lazy-update.md) pays off only when the inputs have few distinct values.

If the formula is only a call to a [Python function](../how-to-guides/python-functions.md) that the engine [auto-vectorizes](./deephaven-mental-model.md#calling-python-from-formulas), [`lazy_update`](../reference/table-operations/select/lazy-update.md) doesn't cache it. The engine evaluates that call in batches instead of row by row.

For [live tables](./deephaven-mental-model.md#static-vs-live-tables), choosing between stored and on-demand operations also decides when the work happens:

- [`update`](../reference/table-operations/select/update.md) and [`select`](../reference/table-operations/select/select.md) recompute and store values for changed rows on every [update cycle](./dag.md), even if nothing reads them.
- [`view`](../reference/table-operations/select/view.md) and [`update_view`](../reference/table-operations/select/update-view.md) don't evaluate their formulas when the source changes. They compute values only when something reads them.
- [`lazy_update`](../reference/table-operations/select/lazy-update.md) also waits until a value is read, then caches it.

## Related documentation

- [How Deephaven works: A mental model](./deephaven-mental-model.md)
- [Table operations overview](./table-operations-overview.md)
- [Deephaven's design](./deephaven-design.md)
- [Deephaven's live DAG](./dag.md)
- [Table types](./table-types.md)
- [Aggregations in Deephaven](./aggregations.md)
- [Formulas](../how-to-guides/formulas.md)
- [Query scope](../how-to-guides/query-scope.md)
- [Built-in query language functions](../how-to-guides/built-in-functions.md)
- [Python functions in query strings](../how-to-guides/python-functions.md)
- [Java objects and classes in query strings](../how-to-guides/java-classes.md)
- [String and char literals in query strings](../how-to-guides/string-char-literals.md)
- [Use the ternary conditional operator in query strings](../how-to-guides/ternary-if-how-to.md)
- [Filter table data](../how-to-guides/use-filters.md)
- [Select and create columns](../how-to-guides/use-select-view-update.md)
- [Exact and relational joins](../how-to-guides/joins-exact-relational.md)
- [Time-series and range joins](../how-to-guides/joins-timeseries-range.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`update`](../reference/table-operations/select/update.md)
- [`view`](../reference/table-operations/select/view.md)
- [`select`](../reference/table-operations/select/select.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`lazy_update`](../reference/table-operations/select/lazy-update.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`sum_`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`formula`](../reference/table-operations/group-and-aggregate/AggFormula.md)
- [`is_refreshing`](../reference/table-operations/metadata/is_refreshing.md)
- [Special variables](../reference/query-language/variables/special-variables.md)
- [Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
