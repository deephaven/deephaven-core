---
title: "How Deephaven works: A mental model and patterns of use"
sidebar_label: Patterns of use
---

<div className="comment-title">

Understanding how to think about Deephaven

</div>

Deephaven works differently from tools like pandas, polars, or SQL — and even if you haven't used those, some of Deephaven's behavior might surprise you. Code that looks straightforward can produce unexpected results. This guide explains _how to think_ about Deephaven so you can write effective queries and avoid common pitfalls.

This isn't a deep technical dive — for that, see [Deephaven's design](./deephaven-design.md). Instead, this guide builds the mental model you need to work productively with Deephaven from day one.

![How Deephaven works: Tables are recipes, not data copies — updates flow automatically](../assets/conceptual/mental-model/mental-model-overview-static.png)

## Tables are recipes, not data

In pandas, a DataFrame is a container holding your data. When you filter or transform it, you get a new container with different data inside.

**Deephaven tables work differently.** A table is more like a _recipe_ — a description of how to compute results from source data. When you call [`where`](../reference/table-operations/filter/where.md) or [`update`](../reference/table-operations/select/update.md), you're not creating a copy with filtered data. You're creating a new recipe that says "take this input and apply this transformation."

```python order=source,filtered,doubled
from deephaven import empty_table

source = empty_table(5).update("X = i")
filtered = source.where("X > 2")
doubled = source.update("Y = X * 2")
```

Here, `filtered` contains rows where X > 2 (rows 3 and 4), computed immediately when you call `where`. But `filtered` also maintains a _dependency_ on `source` — if `source` changes (because it's connected to live data), `filtered` automatically recomputes and reflects those changes.

**Why this matters:**

- You don't need to re-run your filter when data changes — it happens automatically.
- Multiple transformations can share the same source without duplicating data.
- Operations are typically much faster than copying entire datasets.

When you call a table operation, Deephaven establishes the dependency and computes the initial result. The "recipe" remains active — if the source data changes, downstream tables update automatically without you re-running code. Operations like [`view`](../reference/table-operations/select/view.md) are an exception: they store the formula but defer evaluation until values are actually accessed, which saves memory for columns you rarely read.

## Formulas run in the engine, not in Python

When you write a formula string like `"Y = X * 2"`, that code doesn't run in Python. It runs inside Deephaven's Java-based engine, which is optimized for processing millions of rows efficiently.

```python order=result
from deephaven import empty_table

result = empty_table(1000).update(["X = i", "Y = Math.sqrt(X * X + 1)"])
```

The engine parses and executes the string `"Y = Math.sqrt(X * X + 1)"`, not Python's interpreter.

> [!TIP]
> Query strings use Java-style syntax: backticks (`` ` ``) for strings instead of quotes, `PT1S` for durations (1 second), and casts like `(int)` to specify return types.

This has important implications:

- **Java methods, not Python functions**: Use `Math.sqrt()`, not `math.sqrt()`. Use `String` methods like `.toUpperCase()`, not Python string methods.
- **Python variables are available**: Local and global variables from your script are automatically resolved through [query scope](../how-to-guides/query-scope.md), but you can also call Python functions (with a performance cost).
- **Much faster**: The engine processes data in optimized batches, not one row at a time.

### Calling Python from formulas

You _can_ call Python functions from formulas, but understand that this crosses a boundary:

```python order=result
from deephaven import empty_table


def my_calculation(x):
    return x**2 + 1


result = empty_table(10).update("Y = (int)my_calculation(i)")
```

When you reference a Python function in a formula, the engine must cross into Python to evaluate it. The boundary crossing is batched (once per chunk of rows), but a scalar function is still called once per row within that chunk. This is slower than pure-engine formulas. For performance-critical code, prefer engine-native expressions.

## Your code doesn't run row-by-row

This is the most common surprise for new users. Consider this code:

```python syntax
counter = 0


def next_value():
    global counter
    counter += 1
    return counter


# Don't rely on getting 1, 2, 3, 4, 5...
result = empty_table(5).update("Value = (int)next_value()")
```

You might expect `Value` to be `[1, 2, 3, 4, 5]`. But the engine can evaluate rows in _any order_, potentially in _parallel_ across multiple threads. You might get `[3, 1, 4, 2, 5]` or something else entirely — and results may differ between runs.

**The rule:** Formulas should be _stateless_ — the result for row N should depend only on the input values for row N, not on what happened when processing other rows. Immutable Python variables (like configuration values) are fine; mutable state and order-dependent logic are not.

### Replacing counters with row positions

If you need deterministic row numbering on **static or append-only tables**, use `ii` (the row position) instead of a counter:

```python order=result
from deephaven import empty_table

# Use ii (the row number) instead of a counter
result = empty_table(5).update("Value = ii + 1")
```

> [!NOTE]
> These variables work on static and certain streaming tables, but general refreshing tables reject them because positions and keys can shift. If you need row identity on a ticking table, use a stable key column instead. See [special variables](../reference/query-language/variables/special-variables.md) for the full compatibility matrix.

For more complex cases involving state, see [parallelization](./query-engine/parallelization.md) and the serial execution options. For details on [`ii` and other special variables](../reference/query-language/variables/special-variables.md), see the reference documentation.

## Static vs. live: understanding mutability

Deephaven tables come in two flavors:

- **Static tables**: Data that doesn't change. Loaded from files, created with [`empty_table`](../reference/table-operations/create/emptyTable.md) or [`new_table`](../reference/table-operations/create/newTable.md), or snapshots of live data.
- **Refreshing (live) tables**: Data that updates continuously. Created with [`time_table`](../reference/table-operations/create/timeTable.md), connected to streams, or other real-time sources.

```python order=static_table,live_table
from deephaven import empty_table, time_table

# Static: this table will always have 10 rows with values 0-9
static_table = empty_table(10).update("X = i")

# Live: this table grows by one row every second
live_table = time_table("PT1S")  # See time_table reference for duration syntax
```

**The key insight:** Transformations on live tables produce live results. If you filter a live table, the filtered result updates automatically. When you need to freeze the data at a point in time, use [`snapshot`](../reference/table-operations/snapshot/snapshot.md) to capture a static copy.

```python order=live_source,recent_only
from deephaven import time_table

live_source = time_table("PT1S").update("Value = randomInt(0, 100)")
recent_only = live_source.where("Value > 50")
# recent_only continuously updates as new rows arrive and are filtered
```

You don't need to poll for changes or re-run queries — the engine handles propagation automatically.

## Moving data between Python and Deephaven

Data lives in two places: Python variables and Deephaven tables. Understanding when data moves between them helps you write efficient code.

### From Python to Deephaven

When you create a table from Python data, the engine copies that data:

```python order=my_table
from deephaven import new_table
from deephaven.column import int_col, string_col

# Data is copied from Python into the engine
my_table = new_table([int_col("ID", [1, 2, 3]), string_col("Name", ["A", "B", "C"])])
```

### From Deephaven to Python

When you extract data back to Python, you're taking a _snapshot_:

```python order=my_table
from deephaven import empty_table
from deephaven.numpy import to_numpy

my_table = empty_table(5).update("X = i * 10")

# Get a snapshot as a numpy array
snapshot = to_numpy(my_table, cols=["X"])
# snapshot is a numpy ndarray - a Python copy, not connected to the table
```

For live tables, this snapshot represents the data at one moment in time. The table may continue updating, but your snapshot won't.

### Performance tips

Moving data between Python and Deephaven takes time. For large datasets:

- Keep data in Deephaven tables and use engine operations (fast).
- Avoid repeatedly converting between pandas and Deephaven (slow).
- Use snapshots strategically, not in tight loops.

## What you can build

Deephaven isn't just a table engine — it's a platform for building data applications.

### Interactive UIs

Create live dashboards entirely in Python with `deephaven.ui`:

```python syntax
from deephaven import time_table, ui

source = time_table("PT1S").update("Value = randomInt(0, 100)")


@ui.component
def my_dashboard():
    # use_state creates a variable (threshold) that the UI can change
    threshold, set_threshold = ui.use_state(50)
    # use_memo creates a filtered table that updates when threshold changes
    filtered = ui.use_memo(lambda: source.where(f"Value > {threshold}"), [threshold])

    return ui.flex(
        ui.slider(value=threshold, on_change=set_threshold, min_value=0, max_value=100),
        ui.table(filtered),
        direction="column",
    )


dashboard = my_dashboard()
```

The UI updates automatically as data changes and as users interact with controls. See [deephaven.ui](../how-to-guides/deephaven-ui.md) for a full introduction.

### Data sources and sinks

| Source           | How to use                                                                        |
| ---------------- | --------------------------------------------------------------------------------- |
| **Parquet**      | `read("/path/to/file.parquet")`                                                   |
| **CSV**          | `deephaven.csv.read("/path/to/file.csv")`                                         |
| **Kafka**        | [`consume`](../reference/data-import-export/Kafka/consume.md)                     |
| **Manual entry** | [Input tables](../how-to-guides/input-tables.md) — edit cells in the UI           |
| **Programmatic** | [Table Publisher](../how-to-guides/table-publisher.md) — push data from your code |

| Destination        | How to use                                                    |
| ------------------ | ------------------------------------------------------------- |
| **Parquet**        | `write(table, "/path/to/output.parquet")`                     |
| **Kafka**          | [`produce`](../reference/data-import-export/Kafka/produce.md) |
| **Python**         | `to_pandas(table)` or `to_numpy(table)`                       |
| **Remote clients** | Connect via Python, Java, JavaScript, or C++ clients          |

### Client-server architecture

Deephaven runs as a server. Multiple clients can connect simultaneously:

- **Web UI**: Built-in interactive console and grids
- **Python client**: `from pydeephaven import Session`
- **JavaScript client**: For web applications
- **Java/C++ clients**: For high-performance integrations

Clients that subscribe to the same table see consistent, live updates. Tables can be shared between sessions using [shared tickets](../reference/client-api/session/publish-table.md).

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

See [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) for all aggregation options.

### Pattern: Use `ii` and `i` instead of counters

```python order=numbered
from deephaven import empty_table

# i = row position as int (0, 1, 2...)
# ii = row position as long (use for tables with more than 2 billion rows)
numbered = empty_table(10).update(["RowNumber = ii", "Value = i * 2"])
```

### Pattern: Use `view` for lightweight derived columns

Use [`view`](../reference/table-operations/select/view.md) when you want derived columns without storing them. Use [`update`](../reference/table-operations/select/update.md) when you need the results cached for repeated access or downstream operations.

```python order=source,derived
from deephaven import empty_table

source = empty_table(1000000).update("X = randomDouble(0, 100)")

# view computes on-demand, doesn't store the result — good for simple derivations
derived = source.view(["X", "Doubled = X * 2", "Tripled = X * 3"])

# update stores the result — better when you'll use it many times or it's expensive to compute
# stored = source.update("ExpensiveCalc = some_complex_function(X)")
```

### Pattern: Compose queries step by step

Build complex analytics by chaining simple operations. Each step produces a table you can inspect, reuse, or build on:

```python order=raw,cleaned,enriched,summary
from deephaven import time_table, agg

# Start with raw data
raw = time_table("PT1S").update(
    ["Symbol = (ii % 2 == 0) ? `AAPL` : `GOOG`", "Price = randomDouble(100, 200)"]
)

# Clean it
cleaned = raw.where("Price > 0")

# Enrich it
enriched = cleaned.update("PriceRounded = Math.round(Price)")

# Summarize it
summary = enriched.agg_by(
    [agg.avg("AvgPrice = Price"), agg.count_("Count")], by=["Symbol"]
)

# All four tables update together when new data arrives
```

Each intermediate table (`cleaned`, `enriched`) is a first-class object you can display, join, or use as input to further operations.

### Pattern: Same code for batch and streaming

Write your logic once — it works identically on files and live streams:

```python syntax
from deephaven.parquet import read
from deephaven import agg


# This analysis logic...
def analyze_trades(trades):
    return trades.where("Quantity > 0").agg_by(
        [agg.sum_("TotalQty = Quantity"), agg.avg("AvgPrice = Price")], by=["Symbol"]
    )


# ...works on historical files
historical = read("/data/trades.parquet")
historical_analysis = analyze_trades(historical)

# ...and on live streams
# live = kafka_consumer(...)
# live_analysis = analyze_trades(live)
```

No need for separate batch and streaming codebases.

### Pattern: Partition large datasets

Split data by key and process each partition efficiently:

```python order=trades
from deephaven import time_table
from deephaven import updateby as uby
from deephaven.execution_context import get_exec_ctx

trades = time_table("PT0.1S").update(
    [
        "Symbol = (ii % 3 == 0) ? `AAPL` : ((ii % 3 == 1) ? `GOOG` : `MSFT`)",
        "Price = randomDouble(100, 200)",
    ]
)

# Partition by symbol
by_symbol = trades.partition_by("Symbol")

# Capture execution context - required for live partitioned tables
# since new constituents may arrive on update threads
ctx = get_exec_ctx()


def transform_func(t):
    with ctx:
        return t.update_by(
            ops=uby.rolling_avg_tick(cols=["AvgPrice = Price"], rev_ticks=10)
        )


# Apply operations to each partition
transformed = by_symbol.transform(transform_func)
```

Partitioned tables let you parallelize processing, quickly retrieve subtables by key, and improve filter performance in loops. See [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) and [Partitioned tables](../how-to-guides/partitioned-tables.md) for details.

## Pitfalls to avoid

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
# from deephaven.pandas import to_pandas, to_table
# df = to_pandas(my_table)
# df = df[df["X"] > 10]
# result = to_table(df)

# DO: Use Deephaven operations directly
# result = my_table.where("X > 10")
```

## Quick reference

| I want to...               | Use this                                               |
| -------------------------- | ------------------------------------------------------ |
| Check if a table updates   | `table.is_refreshing`                                  |
| Take a static snapshot     | `table.snapshot()`                                     |
| Edit data manually         | [Input tables](../how-to-guides/input-tables.md)       |
| Push data programmatically | [Table Publisher](../how-to-guides/table-publisher.md) |
| Process by groups          | `partition_by`                                         |
| Build a dashboard          | `deephaven.ui`                                         |
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
