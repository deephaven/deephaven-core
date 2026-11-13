---
title: Is it possible to convert one or more columns of a Deephaven table to JSON?
---

Yes. Convert the column(s) to a NumPy array with [`deephaven.numpy.to_numpy`](/core/pydoc/code/deephaven.numpy.html#deephaven.numpy.to_numpy), then serialize the array to JSON with Python's [`json`](https://docs.python.org/3/library/json.html#json.JSONEncoder) module. `to_numpy` converts one column, or several columns of the same type, into a NumPy array. For columns of mixed types, use [`deephaven.pandas.to_pandas`](../pandas/to-pandas.md) and the DataFrame's `to_json` method instead.

```python order=source,df
import json
from deephaven import empty_table
from deephaven.numpy import to_numpy
from deephaven.pandas import to_pandas

source = empty_table(5).update(["X = i", "Y = i * 1.1", "Label = `item_` + i"])

# Single column or same-type columns to JSON via NumPy
arr = to_numpy(source, cols=["X"])
json_str = json.dumps(arr.tolist())
print(json_str)

# Mixed-type columns to JSON via pandas
df = to_pandas(source)
json_str_mixed = df.to_json(orient="records")
print(json_str_mixed)
```

For more on working with NumPy arrays in Deephaven, see [How to use NumPy](../../how-to-guides/use-numpy.md).

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
