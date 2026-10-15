---
title: Powerful Python Integrations
---

Deephaven integrates with popular Python libraries. This chapter covers some highlights of Deephaven's Python interoperability: converting tables to and from pandas DataFrames and NumPy arrays, running AI models on real-time data with a [table listener](../../how-to-guides/table-listeners-python.md), and where [`deephaven.learn`](../../how-to-guides/use-deephaven-learn.md) fits in.

## pandas

The [`deephaven.pandas`](../../how-to-guides/use-pandas.md) module is Deephaven's gateway to [pandas](https://pandas.pydata.org/) interoperability. The previous chapter, [Recipes, not loops!](./vectorization-vs-loops.md), used both of its functions: [`to_pandas`](../../reference/pandas/to-pandas.md) converts a Deephaven table into a [pandas DataFrame](https://pandas.pydata.org/docs/reference/frame.html), and [`to_table`](../../reference/pandas/to-table.md) converts a pandas DataFrame into a Deephaven table.

```python test-set=1 order=t_df,t
from deephaven import pandas as dhpd
from deephaven import empty_table

t = empty_table(10).update(
    [
        "Timestamp = '2015-01-01T00:00:00 ET' + 'PT1m'.multipliedBy(ii)",
        "Group = randomInt(1, 4)",
        "GroupMean = Group == 1 ? -10.0 : Group == 2 ? 0.0 : Group == 3 ? 10.0 : NULL_DOUBLE",
        "GroupStd = Group == 1 ? 2.5 : Group == 2 ? 0.5 : Group == 3 ? 1.0 : NULL_DOUBLE",
        "X = randomGaussian(GroupMean, GroupStd)",
    ]
)

t_df = dhpd.to_pandas(t)
```

[`to_pandas`](../../reference/pandas/to-pandas.md) converts each Deephaven column to a pandas dtype. By default, it uses pandas nullable dtypes, so Deephaven nulls become pandas missing values such as `pd.NA`.

```python test-set=1 order=:log
print(t_df.dtypes)
```

You can apply pandas operations to the DataFrame and convert the result back to a Deephaven table.

```python test-set=1 order=t_df_group_avg,t_group_avg
t_df_group_avg = t_df.groupby("Group", as_index=False).mean()
t_group_avg = dhpd.to_table(t_df_group_avg)
```

This example shows how flexible the pandas integration is. For this particular job, Deephaven Query Language (DQL) table operations are more efficient: `t.drop_columns("Timestamp").avg_by("Group")`. Unlike the pandas `mean` method, [`avg_by`](../../reference/table-operations/group-and-aggregate/avgBy.md) can't average timestamp columns. It raises an error when the table has one, so the DQL version drops `Timestamp` first.

pandas DataFrames are static. Converting a ticking Deephaven table to a pandas DataFrame takes a snapshot of the table at that moment.

## NumPy

[NumPy](https://numpy.org/) is one of Python's most popular packages. It implements arrays and array operations. Deephaven's [`deephaven.numpy`](../../how-to-guides/use-numpy.md) module converts in both directions: [`to_numpy`](../../reference/numpy/to-numpy.md) converts a Deephaven table to a NumPy array, and [`to_table`](../../reference/numpy/to-table.md) converts a NumPy array to a Deephaven table.

The following example converts the table's `X` and `Z` columns to a NumPy array, transposes the array with NumPy so the table's 10 rows become 10 columns, and converts the result back to a table. [`to_table`](../../reference/numpy/to-table.md) needs one name for each column, so the example passes a list of 10 column names.

```python test-set=2 order=t,t_transposed,:log
from deephaven import numpy as dhnp
from deephaven import empty_table

t = empty_table(10).update(["X = ii", "Y = X + 2", "Z = sin(X + Y)"])

t_np = dhnp.to_numpy(t.view(["X = (double)X", "Z"]))
print(t_np)

t_np_transposed = t_np.transpose()
print(t_np_transposed)

col_names = [
    "Col1",
    "Col2",
    "Col3",
    "Col4",
    "Col5",
    "Col6",
    "Col7",
    "Col8",
    "Col9",
    "Col10",
]
t_transposed = dhnp.to_table(t_np_transposed, col_names)
```

The example casts the `X` column from `long` to `double` because [`to_numpy`](../../reference/numpy/to-numpy.md) requires all columns to have the same data type, while a Deephaven table can have a different type in each column. For example, the following conversion fails.

```python test-set=2 should-fail
t = empty_table(10).update(["X = ii", "Y = sin(X)", "Z = Y > 0 ? 1 : 0"])

# This fails because to_numpy requires all columns to have the same data type
t_np = dhnp.to_numpy(t)
```

Like pandas DataFrames, NumPy arrays are static. Converting a ticking Deephaven table with [`to_numpy`](../../reference/numpy/to-numpy.md) takes a snapshot of the table at that moment.

## Listener-based AI

Deephaven can evaluate machine learning models on real-time data in several ways. One is to call the model from a table operation such as [`update`](../../reference/table-operations/select/update.md), as shown in the [AI/ML workflows guide](../../how-to-guides/ml-ai-no-learn.md#table-operations). This chapter covers two others. A [table listener](../../how-to-guides/table-listeners-python.md) works on any ticking table. [`deephaven.learn`](#deephavenlearn), covered in the next section, supports only some kinds of tables.

A listener-based AI workflow has four steps:

1. A [table listener](../../how-to-guides/table-listeners-python.md) receives each batch of changes to a source table.
2. The listener passes the new data to an AI model.
3. The listener publishes the model's results through a [`table_publisher`](../../reference/table-operations/create/TablePublisher.md) to a [blink table](../../conceptual/table-types.md#specialization-3-blink). A blink table holds only the rows added in the current update cycle.
4. [`blink_to_append_only`](../../reference/table-operations/create/blink-to-append-only.md) turns that blink table into a complete history of the model's predictions.

Here's an example using a synthetic ticking dataset and a simple "model".

```python ticking-table order=null
from deephaven import new_table, time_table
from deephaven.stream import blink_to_append_only
from deephaven.table_listener import listen, TableUpdate
from deephaven.stream.table_publisher import table_publisher
import deephaven.column as dhcol
import deephaven.dtypes as dtypes

# define model parameters
P0, P1, NOISE = 13, 1.5, 15


# AI model could be anything from a simple scikit-learn linear model to a massive PyTorch network
def model(x):
    return P0 + P1 * x


# use table_publisher to create a blink prediction table and a TablePublisher that adds rows to it
preds_blink, preds_publish = table_publisher(
    "Predictions",
    {
        "Timestamp": dtypes.Instant,
        "X": dtypes.double,
        "Y": dtypes.double,
        "PredictedY": dtypes.double,
    },
)


# listener function: run the model on new rows and publish the predictions
def on_update(update: TableUpdate, is_replay: bool):
    # only need Timestamp, X, and Y values
    adds = update.added(["Timestamp", "X", "Y"])

    # if there are new rows, compute and publish predictions
    if adds:
        # evaluate the model on this set of inputs
        outputs = model(adds["X"])

        # create a new table with the inputs and outputs
        output_table = new_table(
            [
                dhcol.datetime_col("Timestamp", adds["Timestamp"]),
                dhcol.double_col("X", adds["X"]),
                dhcol.double_col("Y", adds["Y"]),
                dhcol.double_col("PredictedY", outputs),
            ]
        )

        # publish the new table
        preds_publish.add(output_table)


# generate ticking data for demonstrating real-time inference
source = time_table("PT0.5s").update(
    [
        "X = 100 * random()",
        "Y = P0 + P1 * X + randomGaussian(0.0, NOISE)",
    ]
)

# listen to ticking source and publish outputs
handle = listen(source, on_update)

# convert preds_blink to a full-history table
predictions = blink_to_append_only(preds_blink)
```

![`source` and `predictions` tables](../../assets/tutorials/crash-course/ai-listener.gif)

The example handles only added rows (`update.added`). To also handle modified rows, and to learn more about this workflow, see the [AI/ML workflows guide](../../how-to-guides/ml-ai-no-learn.md#table-listener-and-publisher).

## `deephaven.learn`

Aimed at AI/ML practitioners who want to go further, [`deephaven.learn`](../../how-to-guides/use-deephaven-learn.md) provides a general-purpose framework for efficient data interchange between Deephaven tables and Python objects such as NumPy arrays and PyTorch tensors. `deephaven.learn` uses a gather-compute-scatter model:

- **Gather**: copy data from table columns into a Python object, such as a NumPy array.
- **Compute**: run a calculation, such as a model, on that object.
- **Scatter**: write the calculation's results back into a table.

[`deephaven.learn`](../../how-to-guides/use-deephaven-learn.md) works with these tables:

- Any static table.
- Ticking [add-only](../../conceptual/table-types.md#specialization-2-add-only) tables, including [append-only](../../conceptual/table-types.md#specialization-1-append-only) tables.
- Ticking [blink](../../conceptual/table-types.md#specialization-3-blink) tables.

For other ticking tables, use the [table listener](../../how-to-guides/table-listeners-python.md) approach from [Listener-based AI](#listener-based-ai), or one of the other workflows in the [AI/ML workflows guide](../../how-to-guides/ml-ai-no-learn.md).

Next, try the [`deephaven.learn` guide](../../how-to-guides/use-deephaven-learn.md), or see [Use scikit-learn in Deephaven](../../how-to-guides/use-scikit-learn.md#classify-the-iris-dataset-with-deephaven-tables) for a complete example that trains a k-nearest neighbors model to classify Iris flowers.
