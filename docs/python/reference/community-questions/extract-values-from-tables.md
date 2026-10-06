---
title: How do I extract data from a Deephaven table?
---

_How can I get data out of a Deephaven table and into Python objects?_

Use the [`deephaven.numpy`](../../how-to-guides/use-numpy.md) or [`deephaven.pandas`](../../how-to-guides/use-pandas.md) module. [`to_numpy`](../numpy/to-numpy.md) copies table data into a NumPy array, and [`to_pandas`](../pandas/to-pandas.md) copies it into a pandas DataFrame. Both take an optional `cols` argument to copy only the columns you need.

```python order=:log
from deephaven.numpy import to_numpy
from deephaven.pandas import to_pandas
from deephaven import empty_table

table = empty_table(10).update(["X = ii", "Y = sqrt(X)"])

x_np = to_numpy(table, cols=["X"]).flatten()
print(x_np)

x_pd = to_pandas(table, cols=["X"])
print(x_pd)
```

> [!NOTE]
> Both functions copy the data into memory. For large tables, use table operations such as [`where`](../table-operations/filter/where.md) or [`view`](../table-operations/select/view.md) first so that only the data you need is copied.

For other ways to get data out of a table, see:

- [Extract table values](../../how-to-guides/extract-table-value.md): read individual values, iterate over a column with a `for` loop, and access values by row key.
- [Table iterators](../../how-to-guides/iterate-table-data.md): loop over rows as dictionaries or tuples, one row or one chunk at a time.
- [`deephaven.learn`](../../how-to-guides/use-deephaven-learn.md): gather table data into Python objects for calculations and scatter the results back into new table columns.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
