---
title: to_arrow
---

`to_arrow` converts a Deephaven table to a PyArrow table.

## Syntax

```python syntax
to_arrow(table: Table, cols: Sequence[str] | None = None) -> pyarrow.Table
```

## Parameters

<ParamTable>
<Param name="table" type="Table">

A Deephaven table.

</Param>
<Param name="cols" type="Sequence[str]" optional>

The names of the columns to convert, in the order they appear in the result. Default is `None`, which means all columns. An empty sequence also converts all columns.

</Param>
</ParamTable>

## Returns

A PyArrow table. If `table` is [refreshing](../table-operations/metadata/is_refreshing.md), the result is a snapshot of its data at the time of the call and does not update. Starting in version 43.0, Deephaven encodes some columns of the result by default. See [Encoded columns](#encoded-columns).

`to_arrow` raises a `DHError` if the conversion fails, for example if `cols` names a column that `table` does not have.

## Encoded columns

Some columns of the result can be [dictionary-encoded](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout) or [run-end-encoded](https://arrow.apache.org/docs/format/Columnar.html#run-end-encoded-layout). Starting in version 43.0, Deephaven picks these encodings by default, and `to_arrow` has no parameter to control them. The server properties `BarrageUtil.ree.autoDetectEnabled` and `BarrageUtil.dictionary.autoDetectEnabled` turn this automatic choice on or off for the whole server.

A [Barrage schema annotation](../../how-to-guides/data-import-export/barrage-schema.md) is an Arrow schema stored in a table's `BarrageSchema` attribute. To choose the encodings for one table, attach an annotation to `table` before you call `to_arrow`. When `table` has an annotation, Deephaven uses only the encodings the annotation specifies and does not pick encodings for its other columns. The annotation applies even when the server properties above are off. Building an annotation in Python uses Java Arrow types through `jpy`; see [Barrage schema annotation](../../how-to-guides/data-import-export/barrage-schema.md#working-examples) for the steps.

When you pass `cols`, `to_arrow` selects the columns with [`view`](../table-operations/select/view.md), which drops any schema annotation on `table`, so the result uses Deephaven's own encoding choices. To keep an annotation, select the columns with `view` first, attach the annotation to the resulting table, and then call `to_arrow` on that table without `cols`.

Downstream readers handle encoded columns differently: [`to_table`](./to-table.md) accepts them, but if you write the result to a Feather file, [`read_feather`](./read-feather.md) cannot read them correctly. To decode them before writing, see [Deephaven and PyArrow](../../how-to-guides/pyarrow.md#convert-between-deephaven-and-pyarrow-tables).

## Examples

The following example converts a Deephaven table to a PyArrow table and prints its schema:

```python order=source,:log
from deephaven import arrow as dharrow
from deephaven import empty_table

source = empty_table(10).update(["X = i", "Y = sin(X)", "Z = cos(X)"])

pa_source = dharrow.to_arrow(table=source)
print(pa_source.schema)
```

The following example converts only the `Z` and `X` columns of a Deephaven table to a PyArrow table. The printed schema lists `Z` first because that is the order given in `cols`:

```python order=source,:log
from deephaven import arrow as dharrow
from deephaven import empty_table

source = empty_table(10).update(["X = i", "Y = sin(X)", "Z = cos(X)"])

pa_source = dharrow.to_arrow(table=source, cols=["Z", "X"])
print(pa_source.schema)
```

The following example converts a table whose `Sym` column holds only two distinct strings. With the default server properties, the printed schema shows `Sym` as a dictionary-encoded column and `X` as a plain integer column:

```python order=source,:log
from deephaven import arrow as dharrow
from deephaven import empty_table

source = empty_table(100).update(["Sym = i % 3 == 0 ? `A` : `B`", "X = i"])

pa_source = dharrow.to_arrow(table=source)
print(pa_source.schema)
```

## Related documentation

- [Barrage schema annotation](../../how-to-guides/data-import-export/barrage-schema.md)
- [Deephaven and PyArrow](../../how-to-guides/pyarrow.md)
- [`is_refreshing`](../table-operations/metadata/is_refreshing.md)
- [`read_feather`](./read-feather.md)
- [`to_table`](./to-table.md)
- [`view`](../table-operations/select/view.md)
- [Pydoc](/core/pydoc/code/deephaven.arrow.html#deephaven.arrow.to_arrow)
