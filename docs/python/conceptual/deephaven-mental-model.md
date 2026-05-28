---
title: "How Deephaven works: A mental model and patterns of use"
sidebar_label: Patterns of use
---

<div className="comment-title">

Understanding how to think about Deephaven if you're coming from pandas or SQL

</div>

If you've used pandas, polars, or SQL, some of Deephaven's behavior might surprise you. Code that looks straightforward can produce unexpected results. This guide explains _how to think_ about Deephaven so you can write effective queries and avoid common pitfalls.

This isn't a deep technical dive — for that, see [Deephaven's design](./deephaven-design.md). Instead, this guide builds the mental model you need to work productively with Deephaven from day one.

## Tables are recipes, not data

In pandas, a DataFrame is a container holding your data. When you filter or transform it, you get a new container with different data inside.

**Deephaven tables work differently.** A table is more like a _recipe_ — a description of how to compute results from source data. When you call `where` or `update`, you're not creating a copy with filtered data. You're creating a new recipe that says "take this input and apply this transformation."

```python order=source,filtered,doubled
from deephaven import empty_table

source = empty_table(5).update("X = i")
filtered = source.where("X > 2")
doubled = source.update("Y = X * 2")
```

Here, `filtered` doesn't contain rows 3 and 4 — it contains the _instruction_ "show rows from `source` where X > 2." If `source` changes (because it's connected to live data), `filtered` automatically reflects those changes.

**Why this matters:**

- You don't need to re-run your filter when data changes — it happens automatically
- Multiple transformations can share the same source without duplicating data
- Operations are typically much faster than copying entire datasets

## Formulas run in the engine, not in Python

When you write a formula string like `"Y = X * 2"`, that code doesn't run in Python. It runs inside Deephaven's Java-based engine, which is optimized for processing millions of rows efficiently.

```python order=result
from deephaven import empty_table

result = empty_table(1000).update(["X = i", "Y = Math.sqrt(X * X + 1)"])
```

The string `"Y = Math.sqrt(X * X + 1)"` is parsed and executed by the engine, not by Python's interpreter. This has important implications:

- **Java methods, not Python functions**: Use `Math.sqrt()`, not `math.sqrt()`. Use `String` methods like `.toUpperCase()`, not Python string methods.
- **No Python state by default**: Variables from your Python script aren't automatically available inside formulas.
- **Much faster**: The engine processes data in optimized batches, not one row at a time.

### Calling Python from formulas

You _can_ call Python functions from formulas, but understand that this crosses a boundary:

```python order=result
from deephaven import empty_table


def my_calculation(x):
    return x**2 + 1


result = empty_table(10).update("Y = (int)my_calculation(i)")
```

When you reference a Python function in a formula, the engine calls back into Python for each value. This works, but it's slower than pure-engine formulas. For performance-critical code, prefer engine-native expressions.

## Your code doesn't run row-by-row

This is the most common surprise for new users. Consider this code:

```python syntax
counter = 0


def next_value():
    global counter
    counter += 1
    return counter


# This won't produce 1, 2, 3, 4, 5...
result = empty_table(5).update("Value = (int)next_value()")
```

You might expect `Value` to be `[1, 2, 3, 4, 5]`. But the engine can evaluate rows in _any order_, potentially in _parallel_ across multiple threads. You might get `[3, 1, 4, 2, 5]` or something else entirely — and results may differ between runs.

**The rule:** Formulas should be _stateless_. The result for row N should depend only on the input values for row N, not on what happened when processing other rows.

### When you need sequential processing

If you genuinely need sequential processing, Deephaven provides mechanisms for this:

```python order=result
from deephaven import empty_table

# Use ii (the row number) instead of a counter
result = empty_table(5).update("Value = ii + 1")
```

For more complex cases involving state, see [parallelization](./query-engine/parallelization.md) and the serial execution options.

## Static vs. live: understanding mutability

Deephaven tables come in two flavors:

- **Static tables**: Data that doesn't change. Loaded from files, created from Python objects, or snapshots of live data.
- **Refreshing (live) tables**: Data that updates continuously. Connected to streams, sensors, or other real-time sources.

```python order=static_table,live_table
from deephaven import empty_table, time_table

# Static: this table will always have 10 rows with values 0-9
static_table = empty_table(10).update("X = i")

# Live: this table grows by one row every second
live_table = time_table("PT1S")
```

**The key insight:** Transformations on live tables produce live results. If you filter a live table, the filtered result also updates automatically.

```python order=live_source,recent_only
from deephaven import time_table

live_source = time_table("PT1S").update("Value = randomInt(0, 100)")
recent_only = live_source.where("Value > 50")
# recent_only continuously updates as new rows arrive and are filtered
```

You don't need to poll for changes or re-run queries — the engine handles propagation automatically.

## The Python-Deephaven boundary

Data lives in two worlds: Python objects and Deephaven tables. Understanding when and how data crosses this boundary helps you write efficient code.

### From Python to Deephaven

When you create a table from Python data, that data is copied into the engine:

```python order=my_table
from deephaven import new_table
from deephaven.column import int_col, string_col

# Data is copied from Python into the engine
my_table = new_table([int_col("ID", [1, 2, 3]), string_col("Name", ["A", "B", "C"])])
```

### From Deephaven to Python

When you extract data back to Python, you're taking a _snapshot_:

```python order=my_table,snapshot
from deephaven import empty_table
from deephaven.numpy import to_numpy

my_table = empty_table(5).update("X = i * 10")

# Get a snapshot as numpy arrays
snapshot = to_numpy(my_table)
# snapshot is now a dict with column arrays - a Python copy, not connected to the table
```

For live tables, this snapshot represents the data at one moment in time. The table may continue updating, but your snapshot won't.

### Performance implications

Crossing the boundary has overhead. For large datasets:

- Keep data in Deephaven tables and use engine operations (fast)
- Avoid repeatedly converting between pandas and Deephaven (slow)
- Use snapshots strategically, not in tight loops

## Common patterns

### Pattern: Prefer engine operations over Python loops

```python order=prices,avg_by_symbol
from deephaven import empty_table
from deephaven import agg

# Instead of looping in Python to calculate averages...
prices = empty_table(100).update(
    [
        "Symbol = (i % 3 == 0) ? `AAPL` : ((i % 3 == 1) ? `GOOG` : `MSFT`)",
        "Price = randomDouble(100, 200)",
    ]
)

# Let the engine do it
avg_by_symbol = prices.agg_by([agg.avg("AvgPrice = Price")], by=["Symbol"])
```

### Pattern: Use `ii` and `i` instead of counters

```python order=numbered
from deephaven import empty_table

# ii is the row number (position), i is the row key
numbered = empty_table(10).update(["RowNumber = ii", "Value = i * 2"])
```

### Pattern: Use `view` for lightweight derived columns

```python order=source,derived
from deephaven import empty_table

source = empty_table(1000000).update("X = randomDouble(0, 100)")

# view computes on-demand, doesn't store the result
derived = source.view(["X", "Doubled = X * 2", "Tripled = X * 3"])
```

## Common pitfalls

### Pitfall: Treating tables like DataFrames

```python syntax
# DON'T: Try to modify a table in place
# my_table["NewCol"] = values  # This doesn't work

# DO: Create a new table with the added column
# result = my_table.update("NewCol = ...")
```

### Pitfall: Stateful functions in formulas

```python syntax
# DON'T: Rely on execution order
# cache = {}
# def lookup(key):
#     if key not in cache:
#         cache[key] = expensive_calculation(key)
#     return cache[key]
# result = source.update("Value = lookup(Key)")  # Race conditions!

# DO: Use engine-native operations or proper caching patterns
```

### Pitfall: Converting unnecessarily

```python syntax
# DON'T: Convert to pandas for every operation
# df = my_table.to_pandas()
# df = df[df["X"] > 10]
# result = deephaven.pandas.to_table(df)

# DO: Use Deephaven operations directly
# result = my_table.where("X > 10")
```

## Capabilities at a glance

Now that you understand the mental model, here's what the engine can do:

| Capability                     | What it means                                    |
| ------------------------------ | ------------------------------------------------ |
| **Incremental updates**        | Only recompute what changed, not entire datasets |
| **Automatic propagation**      | Downstream tables update when sources change     |
| **Column-oriented processing** | Optimized for analytics operations on columns    |
| **Parallel execution**         | Multiple threads process data simultaneously     |
| **Shared data structures**     | Filtered views share memory with source tables   |
| **Cross-language support**     | Same engine powers Python, Java, and web clients |

## Next steps

- **[Table types](./table-types.md)**: Understand static, refreshing, and blink tables
- **[DAG concept](./dag.md)**: How the engine tracks dependencies between tables
- **[Deephaven's design](./deephaven-design.md)**: Technical deep-dive into the architecture
- **[Parallelization](./query-engine/parallelization.md)**: Controlling concurrent execution

## Related documentation

- [Table types](./table-types.md)
- [Deephaven's design](./deephaven-design.md)
- [Deephaven's live DAG](./dag.md)
- [Table update model](./table-update-model.md)
- [Parallelizing queries](./query-engine/parallelization.md)
