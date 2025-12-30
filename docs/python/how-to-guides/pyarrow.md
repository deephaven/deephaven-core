---
title: Deephaven and PyArrow
sidebar_label: PyArrow
---

This guide covers the intersection of Deephaven and [PyArrow](https://arrow.apache.org/docs/python/index.html). PyArrow is a Python library for [Apache Arrow](https://arrow.apache.org/), which is a columnar memory format similar to Deephaven's table format. Deephaven's [Arrow integration](./deephaven-python-package.md#arrow) provides the ability to do two things:

- Convert between Deephaven and Arrow tables.
- Read Arrow feather files into Deephaven tables.

## `deephaven.arrow`

> [!NOTE]
> Converting between Deephaven and PyArrow tables copies all the objects into memory. Be cautious when converting large datasets.

The [`deephaven.arrow`](/core/pydoc/code/deephaven.arrow.html#module-deephaven.arrow) submodule provides only three functions:

- [`read_feather`](../reference/arrow/read-feather.md)
- [`to_arrow`](../reference/arrow/to-arrow.md)
- [`to_table`](../reference/arrow/to-table.md)

Read a feather file into a Deephaven table as `iris`:

```python test-set=1 order=iris
from deephaven import arrow as dharrow

iris = dharrow.read_feather("/data/examples/Iris/feather/Iris.feather")
```

Then, convert `iris` to a PyArrow table. Once with all columns, then with only the `Class` column.

```python test-set=1 order = pa_iris,pa_class_only
pa_iris = dharrow.to_arrow(iris)
pa_class_only = dharrow.to_arrow(iris, cols=["Class"])
```

Finally, convert `pa_iris` back to a Deephaven table. The first copies all columns; the second copies all but the `Class` column.

```python test-set=1 order=iris_from_pa,iris_no_class
iris_from_pa = dharrow.to_table(pa_iris)
iris_no_class = dharrow.to_table(
    pa_iris, cols=["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"]
)
```

## Related documentation

- [`read_feather`](../reference/arrow/read-feather.md)
- [`to_arrow`](../reference/arrow/to-arrow.md)
- [`to_table`](../reference/arrow/to-table.md)
