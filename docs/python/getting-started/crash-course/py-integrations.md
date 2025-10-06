---
title: Powerful Python Integrations
sidebar_label: Python Integrations
---

Deephaven empowers Python developers by providing efficient integrations with popular Python libraries. This section covers some highlights of Deephaven's Python interoperability as well as the inherent limitations of static Python data structures.

## Pandas

The [`deephaven.pandas`](../../how-to-guides/use-pandas.md) module is the gateway to [Pandas](https://pandas.pydata.org/) interoperability. The module itself is simple, containing only two functions: [`to_pandas`](/core/pydoc/code/deephaven.pandas.html#deephaven.pandas.to_pandas), which converts a Deephaven table into a [Pandas DataFrame](https://pandas.pydata.org/docs/reference/frame.html), and [`to_table`](/core/pydoc/code/deephaven.pandas.html#deephaven.pandas.to_table), which converts a [Pandas DataFrame](https://pandas.pydata.org/docs/reference/frame.html) into a Deephaven table.

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

The appropriate column types are automatically inferred.

```python test-set=1
print(t_df.dtypes)
```

Pandas operations can be applied to the DataFrame and the result can be converted back to a Deephaven table.

```python test-set=1 order=t_df_group_avg,t_group_avg
t_df_group_avg = t_df.groupby("Group").mean()
t_group_avg = dhpd.to_table(t_df_group_avg)
```

Using DQL to do this particular job would be more efficient, but this example demonstrates the flexibility of the Pandas integration.

Note that Pandas DataFrames are inherently static. Converting a ticking Deephaven table to a Pandas DataFrame snapshots the table at that moment in time.

## NumPy

[NumPy](https://numpy.org/) is one of Python's most popular packages. It implements arrays and array operations. Deephaven's [`deephaven.numpy`](../../how-to-guides/use-numpy.md) package, like the Pandas package, contains only two functions, [`to_numpy`](/core/pydoc/code/deephaven.numpy.html#deephaven.numpy.to_numpy) and [`to_table`](/core/pydoc/code/deephaven.numpy.html#deephaven.numpy.to_numpy), for converting to and from NumPy arrays and Deephaven tables, respectively.

```python test-set=2 order=t,t_transposed
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

Note: in this example, the `X` column is cast from a `long` to a `double`. NumPy arrays can only contain a single data type, while Deephaven tables can contain one data type per column. To create a NumPy array, all columns must be the same type. For example, the following conversion will fail.

```python test-set=2  should-fail
t = empty_table(10).update(["X = ii", "Y = sin(X)", "Z = Y > 0 ? 1 : 0"])

# This will fail - a NumPy array can't have more than one data type!
t_np = dhnp.to_numpy(t)
```

## Listener-based AI

Deephaven makes real-time AI inference easy. Evaluate complex machine learning models on real-time data streams with Deephaven's [table listeners](../../how-to-guides/table-listeners-python.md) and [table publishers](../../how-to-guides/table-publisher.md).

Listener-based AI workflows use a [table listener](../../how-to-guides/table-listeners-python.md) to keep track of a table's changes. Data from these changes is then used as inputs for an AI model. Then, the results are published to an AI results [blink table](../../conceptual/table-types.md#specialization-3-blink) using a [`table_publisher`](../../how-to-guides/table-publisher.md). Finally, these blink table results are combined into a complete AI inference history using [`blink_to_append_only`](/core/pydoc/code/deephaven.stream.html#deephaven.stream.blink_to_append_only).

Here's an example using a synthetic ticking dataset and a simple "model".

```python ticking-table order=null
from deephaven import new_table, time_table
from deephaven.stream import blink_to_append_only
from deephaven.table_listener import listen, TableUpdate
from deephaven.stream.table_publisher import table_publisher
import deephaven.column as dhcol
import deephaven.dtypes as dtypes

# define model parameters
P0, P1, Noise = 13, 1.5, 15


# AI model could be anything from a simple scikit-learn linear model to a massive PyTorch network
def model(x):
    return P0 + P1 * x


# use table_publisher to create a blink prediction table plus a publishing function
preds_blink, preds_publish = table_publisher(
    "Predictions",
    {
        "Timestamp": dtypes.Instant,
        "X": dtypes.double,
        "Y": dtypes.double,
        "PredictedY": dtypes.double,
    },
)


# function to listen to table updates and pass new data to compute_and_publish
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
    else:
        return


# generate ticking data for demonstrating real-time inference
source = time_table("PT0.5s").update(
    [
        "X = 100 * random()",
        "Y = P0 + P1*X + randomGaussian(0.0, Noise)",
    ]
)

# listen to ticking source and publish outputs
handle = listen(source, on_update)

# convert preds_blink to a full-history table
predictions = blink_to_append_only(preds_blink)
```

![`source` and `predictions` tables](../../assets/tutorials/crash-course/ai-listener.gif)

To learn more about this workflow, check out the [AI/ML workflows user guide](../../how-to-guides/ml-ai-no-learn.md).

## The `deephaven.learn` library

> **_NOTE:_** `deephaven.learn` only works on append-only tables. See the [previous section](#listener-based-ai) for another approach that works with all table types.

Aimed at ML/AI practitioners, [`deephaven.learn`](../../how-to-guides/use-deephaven-learn.md) provides a general-purpose framework for efficient data interchange between Deephaven tables and Python objects such as NumPy arrays, Torch tensors, and more. [`deephaven.learn`](../../how-to-guides/use-deephaven-learn.md) is fundamentally geared towards machine learning applications, as it enables models to be applied to real-time data.

[`deephaven.learn`](../../how-to-guides/use-deephaven-learn.md) utilizes a gather-compute-scatter framework:

- Data is gathered from a table into a Python object.
- A computation is done on that object, possibly producing outputs.
- The results of the computation are scattered back into the table.

A simple and effective example of machine learning in Deephaven is Iris flower classification. It's an effective introductory problem in machine learning. The objective is to build a model that accurately predicts the class of Iris flower based on four measurements. This code will do so with a K-nearest neighbors model.

```python test-set=3
from deephaven import read_csv
import numpy as np

iris = read_csv("/data/examples/Iris/csv/iris.csv")

classes = {}
num_classes = 0


def get_class_number(c) -> np.intc:
    global classes, num_classes
    if c not in classes:
        classes[c] = num_classes
        num_classes += 1
    return classes[c]


iris = iris.update(formulas=["Class = get_class_number(Class)"])
```

### Gather

To gather table data into Python, use `deephaven.learn.gather.table_to_numpy_2d`. The following code block calls it in two functions: one for doubles, the other for ints. These will both be used shortly.

```python test-set=3
from deephaven.learn import gather


# "gather" functions for double and 32-bit int data types
def table_to_numpy_double(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, np_type=np.double)


def table_to_numpy_int(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, np_type=np.intc)
```

### Compute

Machine learning applies models to data. Models typically produce no outputs when training, but do produce outputs when applied after training is complete. The following code block defines two functions. The first trains a K-nearest neighbors model on data, and the second applies the trained model. They will be used shortly.

```python test-set=3
import os

os.system("pip install -U scikit-learn")

from sklearn.neighbors import KNeighborsClassifier

model = None


def fit_knn(x_train, y_train):
    global model
    model = KNeighborsClassifier(n_neighbors=3)
    model.fit(x_train, y_train.squeeze())


def use_knn(features):
    if features.ndim == 1:
        features = np.expand_dims(features, 0)
    predictions = np.zeros(len(features))
    for i in range(0, len(features)):
        predictions[i] = model.predict([features[i]]).squeeze()
    return predictions
```

### Scatter

In this case, scattering Python data back into a table is simple. Since the model produces scalar outputs, a scatter function only needs to return the output at the correct index.

```python test-set=3
def numpy_to_table(data, idx):
    return int(data[idx])
```

### Put it all together

The `deephaven.learn.learn` function is the final piece of the puzzle. It takes the table, model function, and gather/scatter functions to both train the model as well as apply it to the table data.

First, train the model. Because this is a supervised learning model, two inputs are required: the features and the class labels. The training phase produces no outputs, so none are given.

```python test-set=3
from deephaven import learn

learn.learn(
    table=iris,
    model_func=fit_knn,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_numpy_double,
        ),
        learn.Input("Class", table_to_numpy_int),
    ],
    outputs=None,
    batch_size=150,
)
```

With the trained model in hand, apply it to the table data. This time, an output is produced.

```python test-set=3
iris_knn_classified = learn.learn(
    table=iris,
    model_func=use_knn,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_numpy_double,
        )
    ],
    outputs=[learn.Output("ClassifiedClass", numpy_to_table, "int")],
    batch_size=150,
)
```

For more information, see the [deephaven.learn guide](../../how-to-guides/use-deephaven-learn.md).
