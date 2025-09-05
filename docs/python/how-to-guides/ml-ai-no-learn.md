---
title: AI/ML workflows
sidebar_label: Workflows
---

This guide explores some different AI/ML workflows in Deephaven. For training, it discusses using a [table iterator](./iterate-table-data.md). For testing and application, it covers using table operations, as well as a combination of a [table listener](./table-listeners-python.md) and [table publisher](./table-publisher.md). The workflows presented can be applied to any kind of table, including tables that are not append-only. The alternative to these workflows, [`deephaven.learn`](./use-deephaven-learn.md), only works on append-only tables.

## Train a model

### Table iterator

Training data in AI/ML applications is almost always historical and thus inherently static. Training a model on historical data can be done with a table iterator. Four table iterators can be used:

- [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md)
- [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md)
- [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md)
- [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md)

Model training is typically done on batch data, so either chunked iterator is usually the best choice. The choice between the dict-based or tuple-based iterators is a matter of personal preference.

Consider the following code, which trains a simple linear regression model on a table of noisy static data:

```python test-set=1 order=:log
from deephaven import empty_table
import numpy as np


# This function "trains" (fits) a cubic polynomial to the data
def fit_poly(x, y):
    return np.poly1d(np.polyfit(x, y, 3))


# Our training table
source = empty_table(200).update(
    [
        "X = 0.1 * i",
        "Y = 0.1 * pow(X, 3) + 0.3 * pow(X, 2) - 0.7 * X - 11 + randomDouble(-1, 1)",
    ]
)

# Pull the data out via a table iterator.
# Split the table into two chunks, fitting a polynomial to each chunk
coeffs = np.zeros((2, 4))
for idx, chunk in enumerate(source.iter_chunk_dict(chunk_size=100)):
    x = chunk["X"]
    y = chunk["Y"]
    p = fit_poly(x, y)
    coeffs[idx, :] = np.array(p)

# Our "model" will be the average of the two polynomial fits
coeffs = np.mean(coeffs, axis=0)

print(
    f"{coeffs[0]:.3f} * x ^ 3 + {coeffs[1]:.3f} * x ^ 2 + {coeffs[2]:.3f} * x + {coeffs[3]:.3f}"
)
```

## Real-time inference

### Table operations

One way to apply a model to a table in real time is to use one of the following table operations:

- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`lazy_update`](../reference/table-operations/select/lazy-update.md)

These operations create new columns from existing columns and thus can be used to create a new column using a model. Carefully consider which method to use, as they have different performance tradeoffs. For more information, see [choose the right selection method](./use-select-view-update.md#choose-the-right-column-selection-method).

The following code block uses [`update`](../reference/table-operations/select/update.md) to immediately perform calculations and store the results in memory.

```python test-set=1 ticking-table order=null
from deephaven.plot.figure import Figure
from deephaven import time_table


# A function that calculates the value from the fitted polynomial model
def apply_fitted_poly(x) -> float:
    return coeffs[0] * x**3 + coeffs[1] * x**2 + coeffs[2] * x + coeffs[3]


# Create a ticking data table
source_live = time_table("PT0.2s").update(
    [
        "X = -10 + 0.1 * i",
        "Y = 0.1 * pow(X, 3) + 0.3 * pow(X, 2) - 0.7 * X - 11 + randomDouble(-1, 1)",
    ]
)

# Apply the fitted polynomial to the live data
result_live = source_live.update("YFitted = apply_fitted_poly(X)")

# Plot the result
fig = (
    Figure()
    .plot_xy(series_name="Noisy data", t=result_live, x="Timestamp", y="Y")
    .plot_xy(
        series_name="Polynomial fitted data", t=result_live, x="Timestamp", y="YFitted"
    )
    .show()
)
```

![The above XY series plot](../assets/how-to/polyfit-live.gif)

### Table listener and publisher

A combination of a [table listener](./table-listeners-python.md) and [table publisher](./table-publisher.md) can also evaluate AI/ML models in real time. The listener can listen to added, modified, and/or removed rows during each update cycle. Those rows can then be passed to the model, and the results can be published to a new table via the publisher.

![A diagram showing how table data passes through the AI model](../assets/how-to/listener-publisher-ai.png)

In this workflow, the [table listener](./table-listeners-python.md) is made to listen to both rows that have been added and modified in the current update cycle. It then applies the polynomial model to the data and publishes the results to a new table called `preds`. Since the [table publisher](./table-publisher.md) only produces a [blink table](../conceptual/table-types.md#specialization-3-blink) of current model outputs, the [blink table](../conceptual/table-types.md#specialization-3-blink) is converted to an append-only table via [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md) to store the full history of the model's outputs.

```python test-set=1 ticking-table order=null
from deephaven.stream.table_publisher import table_publisher
from deephaven.table_listener import listen, TableUpdate
from deephaven.column import datetime_col, double_col
from deephaven import time_table, dtypes, new_table
from deephaven.stream import blink_to_append_only
from deephaven.plot.figure import Figure

import numpy as np
import random

# Create a ticking table of data
source_live = time_table("PT0.2s").update(
    [
        "X = -10 + 0.1 * i",
        "Y = 0.1 * pow(X, 3) + 0.3 * pow(X, 2) - 0.7 * X - 11 + randomDouble(-1, 1)",
    ]
)

# Create the output table and publisher
preds, preds_publisher = table_publisher(
    "AIOutput",
    {
        "Timestamp": dtypes.Instant,
        "YFitted": dtypes.double,
    },
)

# Keep the full prediction history
preds_history = blink_to_append_only(preds)


# A function to calculate and publish the output from the model
def compute_and_publish(timestamps, features, preds_publisher):
    outputs = (
        coeffs[0] * features**3
        + coeffs[1] * features**2
        + coeffs[2] * features
        + coeffs[3]
    )
    preds_publisher.add(
        new_table(
            [
                datetime_col("Timestamp", timestamps),
                double_col("YFitted", outputs),
            ]
        )
    )


# Table listener function
def on_update(update: TableUpdate, is_replay: bool) -> None:
    ts_col = "Timestamp"
    data_col = "X"
    adds = update.added([ts_col, data_col])
    modifies = update.modified([ts_col, data_col])

    if adds and modifies:
        data = np.hstack([adds[data_col], modifies[data_col]])
        timestamps = np.hstack([adds[ts_col], modifies[ts_col]])
    elif adds:
        data = adds[data_col]
        timestamps = adds[ts_col]
    elif modifies:
        data = modifies[data_col]
        timestamps = modifies[ts_col]
    else:
        return

    compute_and_publish(timestamps, data, preds_publisher)


# Listen to the ticking table and apply the model to added and modified rows
handle = listen(source_live, on_update, do_replay=True)

result_live = source_live.natural_join(preds_history, "Timestamp")

# Plot the result
fig = (
    Figure()
    .plot_xy(series_name="Noisy data", t=result_live, x="Timestamp", y="Y")
    .plot_xy(
        series_name="Polynomial fitted data", t=result_live, x="Timestamp", y="YFitted"
    )
    .show()
)
```

![The above live plot](../assets/how-to/polyfit-live-publisher.gif)

#### Utilize a thread pool

To see a consistent data snapshot, an update graph (UG) lock is acquired before the listener function is evaluated for an update. Slow listener evaluations can cause the entire update graph calculation to fall behind. To avoid this, slow calculations can be offloaded to other threads. The following code block uses a thread pool in Python's [concurrency](https://docs.python.org/3/library/concurrent.futures.html) library to illustrate the concept:

```python test-set=1 ticking-table order=null
# This will manage the thread pool
from concurrent import futures

from deephaven.stream.table_publisher import table_publisher
from deephaven.table_listener import listen, TableUpdate
from deephaven.column import datetime_col, double_col
from deephaven import time_table, dtypes, new_table
from deephaven.stream import blink_to_append_only
from deephaven.plot.figure import Figure

import numpy as np
import random

# This ThreadPoolExecutor will multi-thread work as the listener receives row data
executor = futures.ThreadPoolExecutor(max_workers=4)

# Create a ticking table of data
source_live = time_table("PT0.2s").update(
    [
        "X = -10 + 0.1 * i",
        "Y = 0.1 * pow(X, 3) + 0.3 * pow(X, 2) - 0.7 * X - 11 + randomDouble(-1, 1)",
    ]
)

# Create the output table and publisher
preds, preds_publisher = table_publisher(
    "AIOutput",
    {
        "Timestamp": dtypes.Instant,
        "YFitted": dtypes.double,
    },
)

# Keep data history, also make the latest rows show up at the top
preds_history = blink_to_append_only(preds).reverse()


# A function to calculate and publish the output from our model
def compute_and_publish(ts, features):
    outputs = (
        coeffs[0] * features**3
        + coeffs[1] * features**2
        + coeffs[2] * features
        + coeffs[3]
    )
    tout = new_table(
        [
            datetime_col("Timestamp", ts),
            double_col("YFitted", outputs),
        ]
    )
    preds_publisher.add(tout)


# Table listener function
def on_update(update: TableUpdate, is_replay: bool) -> None:
    ts_col = "Timestamp"
    data_col = "X"
    adds = update.added([ts_col, data_col])
    modifies = update.modified([ts_col, data_col])

    if adds and modifies:
        data = np.hstack([adds[data_col], modifies[data_col]])
        timestamps = np.hstack([adds[ts_col], modifies[ts_col]])
    elif adds:
        data = adds[data_col]
        timestamps = adds[ts_col]
    elif modifies:
        data = modifies[data_col]
        timestamps = modifies[ts_col]
    else:
        return

    executor.submit(compute_and_publish, timestamps, data)


# Listen to the ticking table and apply the model to added and modified rows
handle = listen(source_live, on_update, do_replay=True)

result_live = source_live.natural_join(preds_history, "Timestamp")

# Plot the result
fig = (
    Figure()
    .plot_xy(series_name="Noisy data", t=result_live, x="Timestamp", y="Y")
    .plot_xy(
        series_name="Polynomial fitted data", t=result_live, x="Timestamp", y="YFitted"
    )
    .show()
)
```

![The above ticking XY plot](../assets/how-to/polyfit-live-publisher.gif)

### Workflow trade-offs

So, you have a trained AI/ML model and aren't sure which workflow to use. Here are some things to consider:

- A single [table operation](#table-operations) is the simplest way to make predictions with a model. It's done in a single line of code.
  The [listener + publisher](#table-listener-and-publisher) is a more complex workflow, but it offers more flexibility. For instance, it can handle multi-row inputs and outputs and take advantage of vectorization for performance.
- The [listener + publisher](#table-listener-and-publisher) can handle delayed calculations and can [utilize a thread pool](#workflow-trade-offs) to manage workloads. The [table operations](#table-operations) will hold the UG lock until the work is finished.

## Related documentation

- [`deephaven.learn`](./use-deephaven-learn.md)
- [PyTorch in Deephaven](./use-pytorch.md)
- [SciKit-Learn in Deephaven](./use-scikit-learn.md)
- [TensorFlow in Deephaven](./use-tensorflow.md)
