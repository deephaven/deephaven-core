---
title: iter_dict
---

`iter_dict` returns a generator that iterates over one row at a time from a table into a dictionary. The dictionary maps column names to scalar values of the corresponding column data types.

If locking is not explicitly specified, this method will automatically lock to ensure that all data from an iteration is from a consistent table snapshot.

## Syntax

```python syntax
iter_dict(cols: Optional[Union[str, Sequence[str]]] = None, chunk_size: int = 2048) -> Generator[Dict[str, Any], None, None]
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]" Optional>

The columns to read. If not given, all columns are read. The default is `None`, which reads all columns.

</Param>
<Param name="chunk_size" type="int" Optional>

The number of rows internally read at a time by Deephaven. The default is 2048.

</Param>
</ParamTable>

## Returns

A generator that yields a dictionary of column names and scalar column values.

## Examples

The following example iterates over a table and prints each value in each row:

```python
from deephaven import empty_table

source = empty_table(6).update(
    ["X = randomInt(0, 10)", "Y = randomDouble(100, 200)", "Z = randomBool()"]
)

for chunk in source.iter_dict():
    x = chunk["X"]
    y = chunk["Y"]
    z = chunk["Z"]
    print(f"X: {x}\tY: {y}\tZ: {z}")
```

The following example iterates over a table and prints only the `X` and `Z` columns:

```python
from deephaven import empty_table

source = empty_table(6).update(
    ["X = randomInt(0, 10)", "Y = randomDouble(100, 200)", "Z = randomBool()"]
)

for chunk in source.iter_dict(cols=["X", "Z"]):
    x = chunk["X"]
    z = chunk["Z"]
    print(f"X: {x}\tZ: {z}")
```

## Related documentation

<!-- TODO: Link to API documentation when it's available. -->

- [`iter_chunk_dict`](./iter-chunk-dict.md)
- [`iter_chunk_tuple`](./iter-chunk-tuple.md)
- [`iter_tuple`](./iter-tuple.md)
