---
title: Table iterators
sidebar_label: Table iterators
---

This guide will show you how to iterate over table data in Python queries. Deephaven offers several built-in methods on tables to efficiently iterate over table data via native Python objects. These methods return generators, which are efficient for iterating over large data sets, as they minimize copies of data and only load data into memory when needed. Additionally, these methods handle locking to ensure that all data from an iteration is from a consistent table snapshot.

> [!NOTE]
> These methods are for **extracting** data from Deephaven to Python, not for **transforming** table data. To create or modify columns, use operations like [`update`](../reference/table-operations/select/update.md), [`where`](../reference/table-operations/filter/where.md), or [`update_by`](../reference/table-operations/update-by-operations/updateBy.md). See [Vectorization](../getting-started/crash-course/vectorization-vs-loops.md) for more details.

## Native methods

Deephaven offers the following table methods to iterate over table data:

- [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md)
- [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md)
- [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md)
- [`iter_tuple_dict`](../reference/data-import-export/iterate/iter-chunk-tuple.md)

These methods all return generators that yield Python data structures containing table data. Generators are efficient because they minimize copies of data and only load data into memory when needed. A generator can only iterate over a data structure once before it's exhausted.

## One row at a time

[`iter_dict`](../reference/data-import-export/iterate/iter-dict.md) and [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md) return a generator that yields one row at a time. The former yields a dictionary with column names as keys, while the latter yields a named tuple with column names as attributes.

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

## One chunk of rows at a time

[`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md) and [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md) return a generator that yields a chunk of rows at a time. Chunk size is defined in the function call. The former yields a dictionary with column names as keys, while the latter yields a named tuple with column names as attributes.

The following example iterates over a table one chunk of rows at a time and prints its values:

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

If the chunk size is not specified, the default is 2048 rows. The following example does not specify a chunk size. The table it iterates over is only 6 rows long, so there is only one chunk.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(["X = i", "Y = randomBool()", "Z = ii"])

for x, z in source.iter_chunk_tuple(["X", "Z"]):
    print(f"X: {x}\tZ: {z}")
```

## Omit columns

All four available methods allow you to only iterate over certain columns in a table. The following example only iterates over the `X` and `Z` columns in the `source` table:

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

for x, z in source.iter_tuple(["X", "Z"]):
    print(f"X: {x}\tZ: {z}")

for chunk in source.iter_chunk_dict(["X", "Z"], chunk_size=5):
    x = chunk["X"]
    z = chunk["Z"]
    print(f"X: {x}\tZ: {z}")

for chunk in source.iter_chunk_tuple(["X", "Z"], chunk_size=5):
    x = chunk.X
    z = chunk.Z
    print(f"X: {x}\tZ: {z}")

for x, z in source.iter_chunk_tuple(["X", "Z"], chunk_size=5):
    print(f"X: {x}\tZ: {z}")
```

## Schema ordering

Table iterators can be tolerant of schema ordering changes by unpacking values inside of the loop, as such:

```python order=:log,source
from deephaven import empty_table

source = empty_table(5).update(["X = i", "Y = ii"])

for row in source.iter_tuple():
    x = row.X
    y = row.Y
    print(x, y)
```

However, unpacking the values in the for statement itself is not tolerant of schema ordering changes:

```python order=:log,source
from deephaven import empty_table

source = empty_table(5).update(["X = i", "Y = ii"])

for x, y in source.iter_tuple():
    print(x, y)
```

There are two ways to ensure iteration is tolerant of schema ordering changes:

- Use [`view`](../reference/table-operations/select/view.md) to limit the table to the desired columns before iteration.
- Specify columns in the iteration call, as shown in the previous examples.

## Performance considerations

Both the row-based and chunk-based methods are efficient when iterating over table data. Consider the following when choosing between available methods:

- Dicts are slightly slower than tuples, but they provide more flexibility.
- Both chunked and nonchunked methods copy data from a table into Python in a chunked way, so they are both efficient. The performance difference between the two is minimal.
- All of these methods automatically handle locking so that the iterations happen over a consistent view of the table.
- The row-based methods also allow you to choose a chunk size. This chunk size is the number of rows copied from the Deephaven table into Python at a time. The default chunk size is 2048 rows.
- The chunk-based iterators are slightly more performant than row-based, but require more complex code.

## Related documentation

- [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md)
- [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md)
- [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md)
- [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md)
