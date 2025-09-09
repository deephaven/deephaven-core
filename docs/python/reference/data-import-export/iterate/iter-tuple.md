---
title: iter_tuple
---

`iter_tuple` returns a generator that reads one row at a time from a table into a named tuple. The named tuple contains fields with column names as their names and values from columns.

If locking is not explicitly specified, this method will automatically lock to ensure that all data from an iteration is from a consistent table snapshot.

## Syntax

```python syntax
iter_tuple(cols: Optional[Union[str, Sequence[str]]] = None, tuple_name: str = "Deephaven", chunk_size: int = 2048)
```

## Parameters

<ParamTable>
<Param name="cols" type="Union[str, Sequence[str]]" Optional>

The columns to read. If not given, all columns are read. The default is `None`, which reads all columns.

</Param>
<Param name="tuple_name" type="str" Optional>

The name of the named tuple. The default is "Deephaven".

</Param>
<Param name="chunk_size" type="int" Optional>

The number of rows to read at a time internally by the engine. The default is 2048.

</Param>
</ParamTable>

## Examples

The following example iterates over a table and prints the values of each row.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(["X = randomInt(0, 10)", "Y = randomDouble(100, 200)"])

for chunk in source.iter_tuple():
    x = chunk.X
    y = chunk.Y
    print(f"X: {x}\tY: {y}")
```

The following example iterates over only two of three columns from a table.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(
    ["X = randomInt(0, 10)", "Y = randomDouble(100, 200)", "Z = randomBool()"]
)

for chunk in source.iter_tuple(cols=["X", "Z"]):
    x = chunk.X
    z = chunk.Z
    print(f"X: {x}\tZ: {z}")
```

The following example iterates over two of three columns in a table. The values are unpacked in the for loop itself. Because columns are specified in the function call, the code is tolerant of schema ordering changes.

```python order=:log,source
from deephaven import empty_table

source = empty_table(10).update(
    ["X = randomInt(0, 10)", "Y = randomBool()", "Z = randomDouble(0, 100)"]
)

for y, z in source.iter_tuple(["Y", "Z"]):
    print(f"Y: {y}\tZ: {z}")
```

The following example iterates over a table and renames the named tuple to `my_tuple`.

```python order=:log,source
from deephaven import empty_table

source = empty_table(6).update(["X = randomInt(0, 10)", "Y = randomDouble(100, 200)"])

for chunk in source.iter_tuple(tuple_name="my_tuple"):
    print(type(chunk).__name__)
    x = chunk.X
    y = chunk.Y
    print(f"X: {x}\tY: {y}")
```

## Related documentation

<!-- TODO: Link to API documentation when it's available. -->

- [`iter_chunk_dict`](./iter-chunk-dict.md)
- [`iter_chunk_tuple`](./iter-chunk-tuple.md)
- [`iter_dict`](./iter-dict.md)
