---
title: AI/ML workflows
---

This guide explores several AI/ML workflows in Deephaven. It shows how to train a model on historical data with a [table iterator](./iterate-table-data.md), then apply it to live data in real time. It covers two ways to apply the model:

- A [table operation](../conceptual/table-operations-overview.md), which works on any table.
- A [table listener](./table-listeners-python.md) and [table publisher](./table-publisher.md), which work on any [ticking table](../conceptual/table-types.md), including ones that modify or remove rows.

[`deephaven.learn`](./use-deephaven-learn.md) is an alternative to these workflows. It works on static tables and on [add-only](../conceptual/table-types.md#specialization-2-add-only) or [blink](../conceptual/table-types.md#specialization-3-blink) ticking tables. [Append-only](../conceptual/table-types.md#specialization-1-append-only) tables are add-only, so they qualify too.

## Train a model

Training data in AI/ML applications is almost always historical and thus inherently static. You can train a model on historical data with a [table iterator](./iterate-table-data.md). Deephaven provides four [table iterators](./iterate-table-data.md):

- [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md)
- [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md)
- [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md)
- [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md)

Model training typically uses batch data, so a chunked iterator is usually the best choice. Use [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md) or [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md). The choice between the dict-based and tuple-based iterators is a matter of personal preference.

Consider the following code, which fits a cubic polynomial model to a table of noisy static data:

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

# Our "model" will be the average of the two polynomial fits.
# Averaging only illustrates per-chunk work. To train on all of the data,
# combine the chunks and fit once.
coeffs = np.mean(coeffs, axis=0)

print(
    f"{coeffs[0]:.3f} * x ^ 3 + {coeffs[1]:.3f} * x ^ 2 + {coeffs[2]:.3f} * x + {coeffs[3]:.3f}"
)
```

The printed polynomial is the model. Its coefficients approximate the 0.1, 0.3, -0.7, and -11 used to generate the data. They don't match exactly, because of the noise and because the model averages two separate fits.

## Real-time inference

You can apply a trained model to a [ticking table](../conceptual/table-types.md) in two ways:

- Add a column with a [table operation](../conceptual/table-operations-overview.md).
- Process each update with a [table listener](./table-listeners-python.md), and publish the results with a [table publisher](./table-publisher.md).

[Table operations](../conceptual/table-operations-overview.md) are simpler, while the [listener](./table-listeners-python.md) and [publisher](./table-publisher.md) give more control. [Workflow trade-offs](#workflow-trade-offs) compares them.

The examples in this section apply the model fitted in [Train a model](#train-a-model), whose coefficients are stored in `coeffs`. Run that example first.

### Table operations

One way to apply a model to a table in real time is to use one of the following [table operations](../conceptual/table-operations-overview.md):

- [`select`](../reference/table-operations/select/select.md)
- [`view`](../reference/table-operations/select/view.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`lazy_update`](../reference/table-operations/select/lazy-update.md)

These operations create new columns from existing columns, so you can use them to add a column of model outputs. They differ in when they compute results and whether they store them in memory. For more information, see [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method).

The following code block uses [`update`](../reference/table-operations/select/update.md) to apply the model. [`update`](../reference/table-operations/select/update.md) performs the calculations immediately and stores the results in memory.

```python test-set=1 ticking-table order=null
import deephaven.plot.express as dx
from deephaven import time_table


# A function that calculates the value from the fitted polynomial model
def apply_fitted_poly(x) -> float:
    return coeffs[0] * x**3 + coeffs[1] * x**2 + coeffs[2] * x + coeffs[3]


# Create a ticking data table
source_live = time_table("PT0.2s").update(
    [
        "X = 0.1 * (i % 200)",
        "Y = 0.1 * pow(X, 3) + 0.3 * pow(X, 2) - 0.7 * X - 11 + randomDouble(-1, 1)",
    ]
)

# Apply the fitted polynomial to the live data
result_live = source_live.update("YFitted = apply_fitted_poly(X)")

# Plot the noisy data and the fitted values
fig = dx.line(result_live, x="Timestamp", y=["Y", "YFitted"])
```

![A live line plot of the noisy `Y` values and the model's `YFitted` values over time](../assets/how-to/polyfit-live.gif)

### Table listener and publisher

A combination of a [table listener](./table-listeners-python.md) and [table publisher](./table-publisher.md) can also evaluate AI/ML models in real time. The [listener](./table-listeners-python.md) runs in each [update cycle](../conceptual/table-update-model.md) in which the source table changes. It receives the rows that were added, modified, and removed, and passes the ones you choose to the model. The [publisher](./table-publisher.md) writes the model's outputs to a new table.

![A diagram showing a table listener passing added, modified, and removed rows from a source table to an AI model, and a table publisher writing the model's outputs to a new table](../assets/how-to/listener-publisher-ai.png)

The diagram shows all three kinds of row change reaching the model. This guide's example passes only the added and modified rows to it.

The example below works in five steps:

- The [table listener](./table-listeners-python.md) applies the polynomial model to the rows added or modified in each [update cycle](../conceptual/table-update-model.md).
- The [table publisher](./table-publisher.md) writes the outputs to `preds`, a [blink table](../conceptual/table-types.md#specialization-3-blink) that holds only the current cycle's outputs.
- The [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md) call converts `preds` to an [append-only](../conceptual/table-types.md#specialization-1-append-only) table that keeps the full prediction history.
- The [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) call keeps only the latest prediction for each `Timestamp`.
- The [`natural_join`](../reference/table-operations/join/natural-join.md) call joins those latest predictions back to the source table so the plot can show them next to the input data.

```python test-set=1 ticking-table order=null
from deephaven.stream.table_publisher import table_publisher
from deephaven.table_listener import listen, TableUpdate
from deephaven.column import datetime_col, double_col
from deephaven import time_table, dtypes, new_table
from deephaven.stream import blink_to_append_only
import deephaven.plot.express as dx

import numpy as np

# Create a ticking table of data
source_live = time_table("PT0.2s").update(
    [
        "X = 0.1 * (i % 200)",
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

    data = np.hstack([adds[data_col], modifies[data_col]])
    timestamps = np.hstack([adds[ts_col], modifies[ts_col]])

    if data.size == 0:
        return

    compute_and_publish(timestamps, data, preds_publisher)


# Listen to the ticking table and apply the model to added and modified rows
handle = listen(source_live, on_update, do_replay=True)

# Join the latest prediction for each timestamp back to the source data
result_live = source_live.natural_join(preds_history.last_by("Timestamp"), "Timestamp")

# Plot the noisy data and the fitted values
fig = dx.line(result_live, x="Timestamp", y=["Y", "YFitted"])
```

![A live line plot of the noisy `Y` values and the `YFitted` predictions joined from the table publisher](../assets/how-to/polyfit-live-publisher.gif)

In `on_update`, the [listener](./table-listeners-python.md) handles each kind of row change differently:

- **Added rows**: The [listener](./table-listeners-python.md) applies the model and publishes one prediction per row.
- **Modified rows**: The [listener](./table-listeners-python.md) applies the model again and publishes another prediction with the same `Timestamp`. [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) keeps only the latest prediction for each timestamp before the [`natural_join`](../reference/table-operations/join/natural-join.md).
- **Removed rows**: The [listener](./table-listeners-python.md) ignores them. Their old predictions stay in the prediction history. The [`natural_join`](../reference/table-operations/join/natural-join.md) drops them from the result, because it keeps only rows that are still in `source_live`.

The [time table](./time-table.md) in this example only adds rows, so the modified and removed cases never happen here. They matter when you use the same [listener](./table-listeners-python.md) on a table that modifies or removes rows.

The [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) step works because each row of the [time table](./time-table.md) has its own `Timestamp`. When several rows can share a timestamp, publish and [join](../reference/table-operations/join/natural-join.md) on the column or columns that identify each row.

The example registers the [listener](./table-listeners-python.md) with `do_replay=True`. Its first call also receives the rows already in `source_live`, so every row gets a prediction. For more information, see [Replay data](./table-listeners-python.md#replay-data).

#### Use a thread pool

Deephaven calls a [listener](./table-listeners-python.md) function inside the [update cycle](../conceptual/table-update-model.md), while the [update graph](../conceptual/dag.md#update-graph-cycles) holds its [exclusive lock](../conceptual/query-engine/engine-locking.md#exclusive-lock). That lock gives the [listener](./table-listeners-python.md) a consistent view of the data, but the cycle can't finish until the [listener](./table-listeners-python.md) returns. A slow [listener](./table-listeners-python.md) therefore makes every table in the [update graph](../conceptual/dag.md#update-graph-cycles) fall behind. To avoid this, run slow calculations on other threads.

Worker threads keep the model out of the [update cycle](../conceptual/table-update-model.md). On a Python build with the GIL (global interpreter lock), they run Python code one at a time. They speed up a model only when its heavy work runs in native code, such as [NumPy](./use-numpy.md), [PyTorch](./use-pytorch.md), or [TensorFlow](./use-tensorflow.md).

The following code uses a thread pool from Python's [`concurrent.futures`](https://docs.python.org/3/library/concurrent.futures.html) module to illustrate the concept. It builds on the [previous example](#table-listener-and-publisher), with these changes:

- It creates a `ThreadPoolExecutor` with four worker threads.
- `compute_and_publish` no longer takes the [publisher](./table-publisher.md) as an argument.
- The [listener](./table-listeners-python.md) calls `executor.submit(compute_and_publish, timestamps, data)` instead of calling `compute_and_publish` directly. The model runs on a worker thread, and the [listener](./table-listeners-python.md) returns right away.
- A `report_error` callback prints any exception a worker thread raises.

```python test-set=1 ticking-table order=null
# Import the thread pool module
from concurrent import futures

from deephaven.stream.table_publisher import table_publisher
from deephaven.table_listener import listen, TableUpdate
from deephaven.column import datetime_col, double_col
from deephaven import time_table, dtypes, new_table
from deephaven.stream import blink_to_append_only
import deephaven.plot.express as dx

import numpy as np

# This ThreadPoolExecutor runs model calculations on worker threads
executor = futures.ThreadPoolExecutor(max_workers=4)

# Create a ticking table of data
source_live = time_table("PT0.2s").update(
    [
        "X = 0.1 * (i % 200)",
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
def compute_and_publish(timestamps, features):
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


# Report any exception raised on a worker thread
def report_error(future):
    if future.exception() is not None:
        print(f"Model calculation failed: {future.exception()}")


# Table listener function
def on_update(update: TableUpdate, is_replay: bool) -> None:
    ts_col = "Timestamp"
    data_col = "X"
    adds = update.added([ts_col, data_col])
    modifies = update.modified([ts_col, data_col])

    data = np.hstack([adds[data_col], modifies[data_col]])
    timestamps = np.hstack([adds[ts_col], modifies[ts_col]])

    if data.size == 0:
        return

    # Run the model on a worker thread instead of in the listener
    executor.submit(compute_and_publish, timestamps, data).add_done_callback(
        report_error
    )


# Listen to the ticking table and apply the model to added and modified rows
handle = listen(source_live, on_update, do_replay=True)

# Join the most recently published prediction for each timestamp back to the source data
result_live = source_live.natural_join(preds_history.last_by("Timestamp"), "Timestamp")

# Plot the noisy data and the fitted values
fig = dx.line(result_live, x="Timestamp", y=["Y", "YFitted"])
```

![A live line plot of the noisy `Y` values and the `YFitted` predictions published from worker threads](../assets/how-to/polyfit-live-publisher.gif)

With more than one worker, calculations can finish out of order. If the source table modifies a row twice in quick succession, the older prediction can be published last. [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md) keeps whichever prediction was published last, so the joined value can come from the row's older inputs.

To keep predictions in order, set `max_workers=1`. A single worker runs calculations in the order the [listener](./table-listeners-python.md) submits them, and the model still runs outside the [update cycle](../conceptual/table-update-model.md).

`executor.submit` returns a [`Future`](https://docs.python.org/3/library/concurrent.futures.html#future-objects) that holds any exception `compute_and_publish` raises. The example's `report_error` callback prints it, so errors on worker threads aren't lost.

To stop the example, call [`handle.stop()`](/core/pydoc/code/deephaven.table_listener.html#deephaven.table_listener.TableListenerHandle.stop), then `executor.shutdown()`.

### Workflow trade-offs

So, you have a trained AI/ML model and aren't sure which workflow to use. Here are some things to consider:

- **[Table operations](#table-operations)**: [Table operations](../conceptual/table-operations-overview.md) are the simplest way to make predictions with a model, and they take a single line of code.
  - With [`update`](../reference/table-operations/select/update.md) or [`select`](../reference/table-operations/select/select.md), the model runs inside the [update cycle](../conceptual/table-update-model.md), so a slow model delays every table in the [update graph](../conceptual/dag.md#update-graph-cycles).
  - With [`view`](../reference/table-operations/select/view.md), [`update_view`](../reference/table-operations/select/update-view.md), or [`lazy_update`](../reference/table-operations/select/lazy-update.md), the model runs when the column is read instead, which can also be inside the [update cycle](../conceptual/table-update-model.md).
- **[Table listener and publisher](#table-listener-and-publisher)**: This workflow is more complex but more flexible.
  - It passes each update's rows to the model as [NumPy](./use-numpy.md) arrays, so the model can process them in one vectorized call.
  - The [publisher's](./table-publisher.md) outputs appear in the [update cycle](../conceptual/table-update-model.md) after the rows that produced them, so new rows in the joined result have a null `YFitted` until then. With a [thread pool](#use-a-thread-pool), they can arrive later still.
  - The [listener](./table-listeners-python.md) itself runs inside the [update cycle](../conceptual/table-update-model.md). A slow model holds back the [update graph](../conceptual/dag.md#update-graph-cycles) unless the [listener](./table-listeners-python.md) hands the work to a [thread pool](#use-a-thread-pool).

## Related documentation

- [Table types](../conceptual/table-types.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [The update graph](../conceptual/dag.md)
- [Multithreading: Synchronization, locks, and snapshots](../conceptual/query-engine/engine-locking.md)
- [`deephaven.learn`](./use-deephaven-learn.md)
- [NumPy in Deephaven](./use-numpy.md)
- [PyTorch in Deephaven](./use-pytorch.md)
- [scikit-learn in Deephaven](./use-scikit-learn.md)
- [TensorFlow in Deephaven](./use-tensorflow.md)
- [Table iterators](./iterate-table-data.md)
- [Table operations overview](../conceptual/table-operations-overview.md)
- [Create a time table](./time-table.md)
- [Listen to ticking tables](./table-listeners-python.md)
- [Write data to an in-memory, real-time table](./table-publisher.md)
- [Choose the right column selection method](./use-select-view-update.md#choose-the-right-column-selection-method)
- [Deephaven Express](/core/plotly/docs/)
- [`blink_to_append_only`](../reference/table-operations/create/blink-to-append-only.md)
- [`datetime_col`](../reference/table-operations/create/dateTimeCol.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`iter_chunk_dict`](../reference/data-import-export/iterate/iter-chunk-dict.md)
- [`iter_chunk_tuple`](../reference/data-import-export/iterate/iter-chunk-tuple.md)
- [`iter_dict`](../reference/data-import-export/iterate/iter-dict.md)
- [`iter_tuple`](../reference/data-import-export/iterate/iter-tuple.md)
- [`last_by`](../reference/table-operations/group-and-aggregate/lastBy.md)
- [`lazy_update`](../reference/table-operations/select/lazy-update.md)
- [`natural_join`](../reference/table-operations/join/natural-join.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`select`](../reference/table-operations/select/select.md)
- [`table_publisher`](../reference/table-operations/create/TablePublisher.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`update_view`](../reference/table-operations/select/update-view.md)
- [`view`](../reference/table-operations/select/view.md)
- [`deephaven.table_listener` Pydoc](/core/pydoc/code/deephaven.table_listener.html)
