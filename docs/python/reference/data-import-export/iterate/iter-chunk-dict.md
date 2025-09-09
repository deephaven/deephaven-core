---
title: iter_chunk_dict
---

`iter_chunk_dict` returns a generator that reads a chunk of rows at a time from a table into a dictionary. The dictionary is a map of column names to NumPy arrays of values.

If locking is not explicitly specified, this method will automatically lock to ensure that all data from an iteration is from a consistent table snapshot.

## Syntax

```python syntax
iter_chunk_dict(cols: Optional[Union[str, Sequence[str]]] = None, chunk_size: int = 2048) -> Generator[Tuple[Any, ...], None, None]
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[Str]]" Optional>

The columns to read. If not given, all columns are read. The default is `None`, which means all columns.

</Param>
<Param name="chunk_size" type="int" Optional>

The number of rows to read at a time. If not given, the default is 2048.

</Param>
</ParamTable>

## Returns

A generator that yields a dictionary of column names to NumPy arrays.

## Examples

The following example iterates over all columns in a table in chunks of two rows at a time and prints the rows.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(["X = randomInt(0, 10)", "Y = randomDouble(100, 200)"])

for chunk in source.iter_chunk_dict(chunk_size=2):
    x = chunk["X"]
    y = chunk["Y"]
    print(f"Column X: {x}")
    print(f"Column Y: {y}")
```

The following example iterates over only two of three columns in a table using the default chunk size. Since the table is only eight rows long, the generator contains only one chunk.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(
    ["X = randomInt(0, 10)", "Y = randomDouble(100, 200)", "Z = randomBool()"]
)

for chunk in source.iter_chunk_dict(cols=["X", "Z"]):
    x = chunk["X"]
    z = chunk["Z"]
    print(f"Column X: {x}")
    print(f"Column Z: {z}")
```

## Related documentation

<!-- TODO: Add API doc links when they're available -->

- [`iter_chunk_tuple`](./iter-chunk-tuple.md)
- [`iter_dict`](./iter-dict.md)
- [`iter_tuple`](./iter-tuple.md)
