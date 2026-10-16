---
title: "How Deephaven works: A mental model and patterns of use"
---

<div className="comment-title">

Understanding how to think about Deephaven

</div>

Deephaven works differently from tools like pandas, Polars, or SQL. Even if you haven't used those tools, some of Deephaven's behavior might surprise you. Code that looks straightforward can produce unexpected results. This guide explains _how to think_ about Deephaven so you can write effective queries and avoid common pitfalls.

This guide isn't a deep technical dive. For that, see [Deephaven's design](./deephaven-design.md). Instead, this guide builds the mental model you need to work productively with Deephaven from day one. The second half of the page puts that model to work: common query patterns, what you can build on the engine, and a quick reference.

<Svg src='../assets/conceptual/mental-model/mental-model-overview.svg' style={{height: 'auto', maxWidth: '1000px'}} />

_For hands-on learning, see the [Crash course](../getting-started/crash-course/overview.md)._

## Tables are recipes, not data

In pandas, a DataFrame is a container holding your data. When you filter or transform it, you get a new container with different data inside.

**Deephaven tables work differently.** When you call [`where`](../reference/table-operations/filter/where.md) or [`update`](../reference/table-operations/select/update.md), Deephaven doesn't copy the source data. Instead, it builds a new table that shares the parent's unchanged columns. The new table computes only what's new, such as the set of rows that pass a filter or a new computed column. It also keeps a live dependency on its parent, so it recomputes automatically when the source changes, without you rerunning any code.

```python ticking-table order=null
from deephaven import time_table

source = time_table("PT1S").update("X = ii")
filtered = source.where("X > 2")
doubled = source.update("Y = X * 2")
```

`source` is a [live table](#static-vs-live-tables): its data changes while your program runs. It adds one row for each second that elapses. A slow or delayed [update cycle](./table-update-model.md) can add several of those rows at once.

`ii` is a built-in [special variable](../reference/query-language/variables/special-variables.md) that holds each row's position as a `long`, so `X` numbers the rows. The special variable `i` holds the same position as an `int`.

`filtered` holds the rows of `source` that currently satisfy `X > 2`, and `doubled` adds a `Y` column to every row of `source`. Both tables update automatically as new rows arrive in `source`, without you rerunning any code.

**Why this matters:**

- You don't need to rerun your [filter](../reference/table-operations/filter/where.md) when data changes, because the filtered table updates itself.
- Multiple transformations can share the same source without duplicating data.
- Operations are typically much faster than copying entire datasets.

Because a table is a recipe rather than a container, you can't modify it in place the way you would a DataFrame. Instead, create a new table with the added column:

```python syntax
# DON'T: Try to modify a table in place
# my_table["NewColumn"] = values  # This doesn't work

# DO: Create a new table with the added column
# result = my_table.update("NewColumn = ...")
```

[`view`](../reference/table-operations/select/view.md) also keeps a dependency on its parent, but it doesn't compute values up front. It stores the formula and evaluates it when values are accessed, which saves memory for columns you rarely read. See [Use `view` for lightweight derived columns](#pattern-use-view-for-lightweight-derived-columns).

## Static vs. live tables

Deephaven tables come in two flavors:

- **Static tables**: Data that doesn't change. Usually loaded from files, created with [`empty_table`](../reference/table-operations/create/emptyTable.md) or [`new_table`](../reference/table-operations/create/newTable.md), or [snapshots](../reference/table-operations/snapshot/snapshot.md) of live data.
- **Live tables**: Data that updates continuously. Deephaven also calls these refreshing tables. Created with [`time_table`](../reference/table-operations/create/timeTable.md) or connected to [Kafka streams](../how-to-guides/data-import-export/kafka-stream.md) and other real-time sources.

```python ticking-table order=null
from deephaven import empty_table, time_table

# Static: this table always has 10 rows with values 0-9
static_table = empty_table(10).update("X = i")

# Live: this table grows by about one row per second
live_table = time_table("PT1S")  # See time_table reference for duration syntax
```

**The key insight:** Most transformations on live tables produce live results. A table derived from a live table, like `filtered` in [Tables are recipes, not data](#tables-are-recipes-not-data), updates automatically. You don't need to poll for changes or rerun queries, because the engine handles propagation for you. [`snapshot`](../reference/table-operations/snapshot/snapshot.md) is a deliberate exception. It takes a live table and returns a static copy of it at a single point in time.

## Formulas run in the engine, not in Python

When you write a formula string like `"Y = X * 2"`, that code doesn't run in Python. It runs inside Deephaven's Java-based engine, which is optimized for processing millions of rows efficiently.

```python order=result
from deephaven import empty_table

result = empty_table(1000).update(["X = i", "Y = Math.sqrt(X * X + 1)"])
```

Deephaven's engine parses and executes the string `"Y = Math.sqrt(X * X + 1)"`. Python's interpreter never sees it.

This has important implications:

- **Java methods are available**: Use `Math.sqrt` rather than `math.sqrt`, and `String` methods like `toUpperCase` rather than Python string methods.
- **Python variables are available**: The engine resolves local and global variables from your script through the [query scope](../how-to-guides/query-scope.md).
- **Python functions are callable**: Formulas can call Python functions, at a performance cost described in [Calling Python from formulas](#calling-python-from-formulas).
- **Engine-native formulas run fast**: Formulas that use only operators, Java methods, and Deephaven's built-in functions are engine-native. The engine compiles them to Java and reads their input columns in bulk chunks, so each row costs a compiled Java call rather than a call into Python.

> [!TIP]
> Query strings use Java-style syntax:
>
> - Backticks (`` ` ``) enclose strings.
> - Single quotes enclose a single-character `char` literal, such as `'A'`, or a date-time literal, such as `'PT1S'` (a one-second duration) or `'2024-01-01T00:00:00Z'` (an instant).
> - Casts like `(int)` specify return types.
>
> API arguments are different. When you pass [`time_table`](../reference/table-operations/create/timeTable.md) its duration as a string, write a plain Python string, as in `time_table("PT1S")`. Only a formula needs the single-quoted literal, as in `'PT1S'`.

### Calling Python from formulas

You _can_ call Python functions from formulas, but understand that this crosses a boundary:

```python order=result
from deephaven import empty_table


def my_calculation(x: int) -> int:
    return x**2 + 1


result = empty_table(10).update("Y = my_calculation(i)")
```

When you reference a Python function in a formula, the engine must [cross into Python](./python-java-boundary.md) to evaluate it. Some calls are eligible for _auto-vectorization_, which means the engine passes a whole batch of rows to one Python call.

A call is eligible when:

- It is a bare call to a function by name, such as `f(X)`, not a method on an object, such as `obj.f(X)`. It isn't wrapped in a cast or used inside a larger expression.
- Its arguments are columns, variables, or constants.
- The function has a fixed parameter list (no `*args`) that matches the number of arguments in the call.

[Type hints](../how-to-guides/python-functions.md#type-hints) don't affect eligibility, but they set the result column's type. Without a return type hint, the column holds Python objects.

The call in this example is eligible. A call that doesn't meet these conditions falls back to one Python call per row. Both kinds of call are slower than engine-native formulas, so prefer engine-native formulas for performance-critical code.

## Write stateless formulas

The engine doesn't promise to evaluate rows in order, or one at a time. This is the most common surprise for new users. Consider this code:

```python syntax
from deephaven import empty_table

counter = 0


def next_value():
    global counter
    counter += 1
    return counter


# Don't rely on getting 1, 2, 3, 4, 5...
result = empty_table(5).update("Value = (int)next_value()")
```

You might expect `Value` to be `[1, 2, 3, 4, 5]`, but nothing guarantees it. Two things can break a counter like this one:

- **Parallel evaluation**: When an operation processes many rows at once, the engine can evaluate a formula's rows on multiple threads, so values can come out of order and differ between runs.
- **Live re-evaluation**: On a live table, the engine re-evaluates a formula only for the rows that change, so the counter's values no longer follow the row order.

On a small static table like this one, the values usually come back in order. Also, a formula that calls a Python function, like this one, runs in parallel only on a [free-threaded Python build](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where).

**The rule:** Formulas should be _stateless_. The result for row N should depend only on the input values for row N, not on what happened when processing other rows. Immutable Python variables, such as configuration values, are fine. Shared mutable state, such as a counter that every row updates, and logic that depends on the order rows are processed in are not.

### Replacing counters with row positions

If you need deterministic row numbering on a table whose rows never change, or only ever get added at the end, use [`ii`](../reference/query-language/variables/special-variables.md) (the row position) instead of a counter:

```python order=result
from deephaven import empty_table

# Use ii (the row position) instead of a counter.
# Use ii rather than i for tables with more than 2 billion rows.
result = empty_table(5).update("Value = ii + 1")
```

> [!NOTE]
> Live tables other than [append-only](./table-types.md#specialization-1-append-only) and [blink](./table-types.md#specialization-3-blink) tables reject [`ii` and `i`](../reference/query-language/variables/special-variables.md) by default, because inserting, removing, or moving rows can shift a row's position. To identify rows in such a table, use a column whose value for each row never changes, such as an ID column. See [special variables](../reference/query-language/variables/special-variables.md#refreshing-table-restrictions) for the full compatibility matrix.

### Replacing running totals with `update_by`

A running total is also stateful, because each row's value depends on the rows before it. Instead of accumulating it in a Python variable, use an [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) cumulative operation such as [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md). The engine computes it in table order and keeps it correct as the table changes:

```python order=result
from deephaven import empty_table
from deephaven import updateby as uby

result = empty_table(5).update("X = i + 1").update_by(uby.cum_sum("RunningTotal = X"))
```

For more complex cases involving state, see [Controlling concurrency](./query-engine/parallelization.md#controlling-concurrency-for-select-update-and-where), which explains how to make the engine evaluate a formula serially.

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

When you extract data back to Python, you get a copy that isn't connected to the table:

```python order=my_table
from deephaven import empty_table
from deephaven.numpy import to_numpy

my_table = empty_table(5).update("X = i * 10")

# Copy the X column into a NumPy array with shape (5, 1)
# to_numpy returns a 2D array with one column per requested column
x_values = to_numpy(my_table, cols=["X"])
# x_values is a NumPy ndarray: a Python copy that isn't connected to the table
```

For live tables, this copy holds the data from one moment in time. The table keeps updating, but your copy doesn't change.

### Performance tips

Moving data between Python and Deephaven takes time. For large datasets, avoid copying data out with [`to_numpy`](../reference/numpy/to-numpy.md) or [`to_pandas`](../reference/pandas/to-pandas.md) and back again in a loop. Keep the work in the engine instead, as the [first pattern below](#pattern-prefer-engine-operations-over-python-loops) shows.

## Common patterns

### Pattern: Prefer engine operations over Python loops

You don't need a Python loop to compute per-group values. An aggregation such as [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md) runs inside the engine over whole batches of rows. On a live table, it updates as rows arrive:

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

```python ticking-table order=null
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

Each intermediate table (`cleaned`, `enriched`) is a first-class object you can display, [join](../how-to-guides/joins-exact-relational.md), or use as input to further operations.

### Pattern: Same code for batch and streaming

Write your logic once, and run the same table operations on files and on live streams:

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
# live = kc.consume(...)  # from deephaven.stream.kafka import consumer as kc
# live_analysis = analyze_trades(live)
```

You don't need separate codebases for batch and streaming data.

### Pattern: Partition large datasets

A [partitioned table](../how-to-guides/partitioned-tables.md) splits a table into subtables, one per key value, so you can process each one independently. Partitioned tables let you process subtables in parallel and retrieve a subtable by key directly, instead of re-running a [filter](../reference/table-operations/filter/where.md) for each key. [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md) creates one, and [`transform`](../reference/table-operations/partitioned-tables/transform.md) applies a function to every subtable:

```python ticking-table order=null
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

ctx = get_exec_ctx()


def transform_func(t):
    # Open the script's execution context so the function's table operations
    # can use the query scope and compile formulas (see Execution context)
    with ctx:
        return t.update_by(
            ops=uby.rolling_avg_tick(cols=["AvgPrice = Price"], rev_ticks=10)
        )


# transform applies transform_func to every subtable (constituent), including ones added later
transformed = by_symbol.transform(transform_func)
```

Here, `transform_func` adds an `AvgPrice` column to each symbol's subtable: a rolling average of `Price` over that symbol's most recent 10 rows.

When you call [`transform`](../reference/table-operations/partitioned-tables/transform.md) from a script, the function doesn't inherit the script's [execution context](./execution-context.md), which table operations need to use the query scope or compile formulas. So the function opens the script's context before it calls [`update_by`](../reference/table-operations/update-by-operations/updateBy.md).

## What you can build

Deephaven is more than a table engine. It's a platform for building data applications.

### Interactive UIs

Create live dashboards entirely in Python with [`deephaven.ui`](../how-to-guides/deephaven-ui.md):

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

The UI updates automatically as data changes and as users interact with controls. See the [`deephaven.ui` guide](../how-to-guides/deephaven-ui.md) for a full introduction.

### Data sources and sinks

Deephaven reads data from files, streams, and your own code, and writes results back out:

| Source           | How to use                                                                                    |
| ---------------- | --------------------------------------------------------------------------------------------- |
| **Parquet**      | [`deephaven.parquet.read`](../reference/data-import-export/Parquet/readTable.md)              |
| **CSV**          | [`deephaven.csv.read`](../reference/data-import-export/CSV/readCsv.md)                        |
| **Kafka**        | [`deephaven.stream.kafka.consumer.consume`](../reference/data-import-export/Kafka/consume.md) |
| **Manual entry** | [Input tables](../how-to-guides/input-tables.md) — edit cells in the UI                       |
| **Programmatic** | [Table publisher](../how-to-guides/table-publisher.md) — push data from your code             |

| Sink        | How to use                                                                                      |
| ----------- | ----------------------------------------------------------------------------------------------- |
| **Parquet** | [`deephaven.parquet.write`](../reference/data-import-export/Parquet/writeTable.md)              |
| **CSV**     | [`deephaven.csv.write`](../reference/data-import-export/CSV/writeCsv.md)                        |
| **Kafka**   | [`deephaven.stream.kafka.producer.produce`](../reference/data-import-export/Kafka/produce.md)   |
| **Python**  | [`to_pandas`](../reference/pandas/to-pandas.md) or [`to_numpy`](../reference/numpy/to-numpy.md) |

### Client-server architecture

Deephaven runs as a server. Multiple [clients](./client-server-model.md#client-apis) can connect simultaneously:

- **Web UI**: For interactive work in the built-in console and grids
- **Python client** ([`pydeephaven`](../getting-started/pyclient-quickstart.md)): For Python applications outside the server
- **JavaScript client**: For web applications
- **Java/C++/R/Go clients**: For integrations in those languages

Clients that subscribe to the same table see consistent, live updates. You can share tables between sessions with [shared tickets](./what-is-barrage.md#shared-tickets).

## Quick reference

| I want to...                        | Use this                                                                                                                                                      |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Check if a table updates            | [`table.is_refreshing`](../reference/table-operations/metadata/is_refreshing.md)                                                                              |
| Take a static snapshot              | [`snapshot`](../reference/table-operations/snapshot/snapshot.md)                                                                                              |
| Number rows without a counter       | [`ii`](../reference/query-language/variables/special-variables.md)                                                                                            |
| Compute a running total             | [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) with [`cum_sum`](../reference/table-operations/update-by-operations/cum-sum.md) |
| Add a column computed on demand     | [`view`](../reference/table-operations/select/view.md)                                                                                                        |
| Aggregate by group                  | [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)                                                                                        |
| Split a table into subtables by key | [`partition_by`](../reference/table-operations/group-and-aggregate/partitionBy.md)                                                                            |
| Build a dashboard                   | [`deephaven.ui`](../how-to-guides/deephaven-ui.md)                                                                                                            |
| Connect remotely                    | [A client API](./client-server-model.md#client-apis)                                                                                                          |

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
