---
title: Table iterators
---

This guide shows you how to iterate over table data in Python queries with Deephaven's built-in table iteration methods.

> [!NOTE]
> These methods are for **extracting** data from Deephaven to Python, not for **transforming** table data. To transform table data, use table operations such as [`update`](../reference/table-operations/select/update.md) to add columns, [`where`](../reference/table-operations/filter/where.md) to filter rows, or [`update_by`](../reference/table-operations/update-by-operations/updateBy.md) for rolling and cumulative calculations. See [Recipes, not loops](../getting-started/crash-course/vectorization-vs-loops.md) for why table operations outperform Python loops.

## Iteration methods

Deephaven offers the following table methods to iterate over table data:

- [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md)
- [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md)
- [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md)
- [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md)

These methods all return generators that yield Python data structures containing table data. Generators are efficient because they minimize copies of data and only load data into memory when needed. You can iterate over a generator only once. After that, it's exhausted.

The methods differ in two ways:

- [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md) and [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md) are row-based: they yield one row at a time. [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md) and [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md) are chunk-based: they yield a chunk of rows at a time as NumPy arrays.
- The `_dict` methods yield dictionaries keyed by column name, and the `_tuple` methods yield named tuples. [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md) is slightly slower than [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md), but because you always access values by name, your code doesn't depend on column order. See [Column order and unpacking](#column-order-and-unpacking).

Row-based and chunk-based methods both copy data into Python in chunks of rows. The row-based methods then build a Python dictionary or tuple for every row, while the chunk-based methods build one per chunk. Chunk-based methods do less per-row Python work, but your code must work with arrays instead of single values.

## One row at a time

[`iter_dict`](../reference/data-import-export/iterate/iter-dict.md) and [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md) return generators that yield one row at a time. `iter_dict` yields a dictionary with column names as keys. `iter_tuple` yields a named tuple with column names as attributes.

The following example iterates over a table one row at a time and prints its values:

```python order=:log,source
from deephaven import empty_table

source = empty_table(10).update(["X = randomInt(0, 10)", "Y = randomBool()"])

for row in source.iter_dict():
    x = row["X"]
    y = row["Y"]
    print(f"X: {x}\tY: {y}")

for row in source.iter_tuple():
    x = row.X
    y = row.Y
    print(f"X: {x}\tY: {y}")

for x, y in source.iter_tuple():
    print(f"X: {x}\tY: {y}")
```

The third loop unpacks each row into `x` and `y` by position, so it depends on the table's column order. See [Column order and unpacking](#column-order-and-unpacking) before you use it in a query whose columns can change.

The row-based methods also let you choose a chunk size with the `chunk_size` argument. This chunk size is the number of rows Deephaven copies from the table into Python at a time. The default chunk size is 2048 rows. The loop still receives one row at a time.

## One chunk of rows at a time

[`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md) and [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md) return generators that yield a chunk of rows at a time. `iter_chunk_dict` yields a dictionary with column names as keys. `iter_chunk_tuple` yields a named tuple with column names as attributes. Each value in the dictionary or named tuple is a NumPy array that holds that column's values for the chunk.

Set the number of rows in each chunk with the `chunk_size` argument. The default is 2048 rows.

The following example iterates over a table one chunk of rows at a time and prints its values. The table has 10 rows and the chunk size is 5, so each loop body runs twice, once per chunk:

```python order=:log,source
from deephaven import empty_table

source = empty_table(10).update(["X = randomInt(0, 10)", "Y = randomBool()"])

for chunk in source.iter_chunk_dict(chunk_size=5):
    x = chunk["X"]
    y = chunk["Y"]
    print(f"X: {x}\tY: {y}")

for chunk in source.iter_chunk_tuple(chunk_size=5):
    x = chunk.X
    y = chunk.Y
    print(f"X: {x}\tY: {y}")

for x, y in source.iter_chunk_tuple(chunk_size=5):
    print(f"X: {x}\tY: {y}")
```

## Null values

Null values in primitive columns arrive as Deephaven's [null constants](../reference/query-language/types/nulls.md), not as `None` or `NaN`. For example, a null `int` value arrives as `-2147483648`, the value of `NULL_INT`. This applies to both the row-based and chunk-based methods.

Null values in object columns arrive differently in the two kinds of method. The row-based methods return `None`. The chunk-based methods convert each column to a NumPy array, so a null `String` arrives as the string `"None"`, a null `Instant` arrives as `NaT`, and a null `Boolean` arrives as `True`. If a `String` or `Boolean` column can contain nulls, use a row-based method, or filter out the nulls with [`where`](../reference/table-operations/filter/where.md) before you iterate.

The following example iterates over a table that has a null in its second row, first one row at a time and then one chunk at a time:

```python order=:log,source
from deephaven import empty_table

source = empty_table(3).update(["I = i == 1 ? NULL_INT : i", "S = i == 1 ? null : `a`"])

for row in source.iter_dict():
    print(row)

for chunk in source.iter_chunk_dict():
    print(chunk)
```

## Iterate over specific columns

[`iter_dict`](../reference/data-import-export/iterate/iter-dict.md), [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md), [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md), and [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md) each take an optional column name or list of column names as their first argument. Pass it to iterate over only those columns.

The values always arrive in the table's column order, whatever order you list the columns in. [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md) and [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md) key each value by column name, so the list order doesn't matter for them. With [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md) and [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md), list the columns in the same order as the table. [Column order and unpacking](#column-order-and-unpacking) explains what goes wrong otherwise.

The following example only iterates over the `X` and `Z` columns in the `source` table:

```python order=:log,source
from deephaven import empty_table

source = empty_table(10).update(
    ["X = randomInt(0, 10)", "Y = randomBool()", "Z = randomDouble(0, 100)"]
)

for row in source.iter_dict(["X", "Z"]):
    x = row["X"]
    z = row["Z"]
    print(f"X: {x}\tZ: {z}")

for row in source.iter_tuple(["X", "Z"]):
    x = row.X
    z = row.Z
    print(f"X: {x}\tZ: {z}")
```

## Column order and unpacking

Unpacking values in the `for` statement, as in `for x, y in source.iter_tuple()`, assigns them to variables in the table's column order. The tuple methods also depend on column order when you pass them a column list. Column order causes problems in these cases:

- If a query reorders the columns, unpacking silently assigns values to the wrong variables.
- If a query adds or removes columns, unpacking fails with a `ValueError`.
- If you pass a column list to [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md) or [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md) in a different order from the table, the tuple fields are named in your list's order, but the values arrive in the table's order. Accessing values by name then reads the wrong columns.

There are two ways to guard against these problems:

- Access values by name inside the loop. Use [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md) or [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md), or use [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md) or [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md) without a column list.
- Use [`view`](../reference/table-operations/select/view.md) to put the columns in the order you unpack them before you iterate.

In the following example, `reordered` has the same columns as `source`, but in the order `Y`, `X`. Accessing values by name gives the right values, positional unpacking assigns the `Y` values to `x`, and calling [`view`](../reference/table-operations/select/view.md) first restores the correct order:

```python order=:log,source,reordered
from deephaven import empty_table

source = empty_table(5).update(["X = i", "Y = ii * 10"])
reordered = source.view(["Y", "X"])

# Access by name: x and y get the right values regardless of column order
for row in reordered.iter_tuple():
    x = row.X
    y = row.Y
    print(f"By name - X: {x}\tY: {y}")

# Positional unpacking follows the table's column order, so x gets the Y values
for x, y in reordered.iter_tuple():
    print(f"Positional - X: {x}\tY: {y}")

# view fixes the column order, so positional unpacking is correct again
for x, y in reordered.view(["X", "Y"]).iter_tuple():
    print(f"After view - X: {x}\tY: {y}")
```

## Iterate over refreshing tables

The [update graph](../conceptual/dag.md#update-graph-ug-cycles) processes table updates in cycles. When you iterate over a [refreshing table](../conceptual/table-types.md), Deephaven can lock the update graph for the whole iteration. The lock stops the table from updating, so all of the values come from one consistent view of the table. Iteration runs under an update graph lock in the following cases:

- Code you run in the console or in a script already holds the update graph's [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock), so no table can update until the code finishes.
- On a thread you start yourself, the iterator acquires the update graph's [shared lock](../conceptual/query-engine/engine-locking.md#shared-lock) and holds it until the generator is exhausted, closed, or destroyed. It does this only when [`auto_locking`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.auto_locking) is enabled, which it is by default, and the thread isn't already marked as safe to run table operations without a lock.
- Code on your own thread already holds a lock it acquired explicitly, for example with [`update_graph.shared_lock`](/core/pydoc/code/deephaven.update_graph.html#deephaven.update_graph.shared_lock). The iterator uses that lock instead of acquiring another one.

Code that runs as part of an update cycle, such as a [table listener](./table-listeners-python.md) callback, doesn't acquire a lock, because the update graph is already processing that cycle.

While the update graph is locked, it can't start a new update cycle. That stops every refreshing table from updating, including tables you aren't iterating over. Keep loop bodies short so the update graph can start its next cycle sooner.

> [!IMPORTANT]
> A generator releases the lock it acquired and the resources it holds when it is exhausted, closed, or destroyed. A `for` loop that runs to completion, such as `for row in table.iter_dict():`, does this automatically. Close the generator yourself when you:
>
> - Store it in a variable and leave the loop early with `break`.
> - Consume it without a `for` loop, for example by calling `next` on it.
>
> To close it, call its `close` method, or release the last reference to it with `del` or by setting it to `None`.

The following example reads only the first row, so it closes the generator itself:

```python order=:log,source
from deephaven import empty_table

source = empty_table(100).update("X = i")

rows = source.iter_dict()
first_row = next(rows)
print(first_row)

# The generator isn't exhausted, so close it to release its resources
rows.close()
```

## Related documentation

- [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md)
- [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md)
- [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md)
- [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md)
- [Multithreading: Synchronization, locks, and snapshots](../conceptual/query-engine/engine-locking.md)
