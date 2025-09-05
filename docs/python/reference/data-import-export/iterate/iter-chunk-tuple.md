---
title: iter_chunk_tuple
---

`iter_chunk_tuple` returns a generator that reads one chunk of rows at a time into a named tuple. The named tuple contains fields with their names being column names and values being NumPy arrays of columnar data.

If locking is not explicitly specified, this method will automatically lock to ensure that all data from an iteration is from a consistent table snapshot.

## Syntax

```python syntax
iter_chunk_tuple(cols: Optional[Union[str, Sequence[str]]] = None, tuple_name: str = "Deephaven", chunk_size: int = 2048) -> Generator[Tuple[np.ndarray, ...], None, None]
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[Str]]" Optional>

The columns to read. If not given, all columns are read. The default is `None`, which reads all columns.

</Param>
<Param name="tuple_name" type="str" Optional>

The name of the named tuple. The default is `Deephaven`.

</Param>
<Param name="chunk_size" type="int" Optional>

The number of rows to read at a time. The default is 2048.

</Param>
</ParamTable>

## Returns

A generator that yields a named tuple for each row in the table.

## Examples

The following example iterates over a table in chunks of two rows of a time. No columns are given, so all columns are read.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(["X = randomInt(0, 10)", "Y = randomDouble(100, 200)"])

for chunk in source.iter_chunk_tuple(chunk_size=2):
    x = chunk.X
    y = chunk.Y
    print(f"Column X: {x}")
    print(f"Column Y: {y}")
```

The following example iterates over two of three columns in a table. The chunk size is not given, so it defaults to 2048 rows. The `source` table has only 6 rows, so there's only one chunk.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(
    ["X = randomInt(0, 10)", "Y = randomDouble(100, 200)", "Z = randomBool()"]
)

for chunk in source.iter_chunk_tuple(cols=["X", "Z"]):
    x = chunk.X
    z = chunk.Z
    print(f"Column X: {x}")
    print(f"Column Z: {z}")
```

The following example iterates over two of three columns a table, unpacking the rows in the for loop itself. Because the column are specified in the function call, it is tolerant of schema ordering changes.

```python
from deephaven import empty_table

source = empty_table(6).update(
    ["X = randomInt(0, 10)", "Y = randomDouble(100, 200)", "Z = randomBool()"]
)

for y, z in source.iter_chunk_tuple(cols=["Y", "Z"]):
    print(f"Column Y: {y}")
    print(f"Column Z: {z}")
```

The following example iterates over a table in chunks of two rows at a time. The named tuple is named `my_tuple`.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(["X = randomInt(0, 10)", "Y = randomDouble(100, 200)"])

for chunk in source.iter_chunk_tuple(chunk_size=2, tuple_name="my_tuple"):
    print(type(chunk).__name__)
    x = chunk.X
    y = chunk.Y
    print(f"Column X: {x}")
    print(f"Column Y: {y}")
```

## Related documentation

<!-- TODO: Link to API documentation when it's available. -->

- [`iter_chunk_dict`](./iter-chunk-dict.md)
- [`iter_dict`](./iter-dict.md)
- [`iter_tuple`](./iter-tuple.md)
