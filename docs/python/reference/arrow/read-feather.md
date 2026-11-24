---
title: read_feather
---

`read_feather` reads a Feather file into a Deephaven table.

## Syntax

```python syntax
read_feather(path: str) -> Table
```

## Parameters

<ParamTable>
<Param name="path" type="str">

The file path.

</Param>
</ParamTable>

## Returns

A Deephaven table.

`read_feather` reads fewer column types than [`to_table`](./to-table.md). It cannot read columns of many types, including null, `date32`, list, large list, fixed-size list, map, struct, union, duration, interval, large string, or large binary columns, or time-of-day columns other than `time32("ms")`. It also cannot read encoded columns correctly: it reads a dictionary-encoded column as its integer indices, and it raises an error for a run-end-encoded column. [`to_arrow`](./to-arrow.md#encoded-columns) can produce encoded columns. To read a file with any of these columns, read it with PyArrow's [`read_table`](https://arrow.apache.org/docs/python/generated/pyarrow.feather.read_table.html) and convert the result with [`to_table`](./to-table.md), which accepts many of them. See [Supported column types](./to-table.md#supported-column-types).

## Example

The following example reads a Feather file into a Deephaven table:

```python order=source
from deephaven import arrow as dharrow

source = dharrow.read_feather("/data/examples/Iris/feather/Iris.feather")
```

## Related documentation

- [`to_arrow`](./to-arrow.md)
- [`to_table`](./to-table.md)
- [Pydoc](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.read_feather)
