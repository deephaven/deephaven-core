---
title: Use TensorBoard with Deephaven
---

This guide shows how to use [TensorBoard](https://www.tensorflow.org/tensorboard) with either [TensorFlow](https://www.tensorflow.org/) or [PyTorch](https://pytorch.org/) in Deephaven queries.

[TensorBoard](https://www.tensorflow.org/tensorboard) is one of the most useful tools when training deep neural networks. Many consider the neural network to be a black box, but TensorBoard can shed some light on how to improve your model. TensorBoard can:

- Track experiment metrics such as loss and accuracy.
- Visualize the model's architecture.
- Show model weights and biases.
- Profile your model's performance.
- Help tune hyperparameters.
- Project embeddings to a lower-dimensional space.

If training your neural network takes hours or days, you can watch its statistics update in a TensorBoard dashboard in real time. That makes TensorBoard a natural fit for Deephaven's real-time tables.

To keep the TensorBoard steps short, the examples below train on a static table. To apply a trained model to a live table that updates as new data arrives, see [Predict on a live table](./use-tensorflow.md#predict-on-a-live-table) in the TensorFlow guide and [Predict on live data](./use-pytorch.md#predict-on-live-data) in the PyTorch guide.

Don't sweat: using TensorBoard with Deephaven's powerful table engine is easy and takes only a few lines of code!

## Set up the server

To follow along, you need a Deephaven server with TensorBoard and either TensorFlow or PyTorch installed, and port 6006 published so your browser can reach TensorBoard. The image named in each section below includes TensorBoard and that section's framework, and the `docker-compose.yml` file linked there also publishes port 6006.

If you start an image with `docker run` instead, publish port 6006 alongside port 10000. This command starts the `server-tensorflow` image:

```sh
docker run --name deephaven -p 10000:10000 -p 6006:6006 ghcr.io/deephaven/server-tensorflow:latest
```

For the PyTorch example, replace `server-tensorflow` with `server-pytorch`.

For other ways to launch Deephaven from pre-built images, see the [Docker install guide](../getting-started/docker-install.md#choose-a-deployment).

## Use TensorBoard with TensorFlow

This example uses the `server-tensorflow` image, which includes TensorFlow and TensorBoard. Start it with the [TensorFlow `docker-compose.yml` file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/TensorFlow/docker-compose.yml).

This example classifies the Iris dataset with Deephaven tables, using the same neural network that the [TensorFlow guide](./use-tensorflow.md#classify-the-iris-dataset-with-deephaven-tables) builds with [Keras](https://www.tensorflow.org/guide/keras), TensorFlow's high-level API for building models. You only need to add a few lines of code to enable TensorBoard!

### Enable TensorFlow logging and start TensorBoard

First, set a log directory. The training code writes its logs to this directory, and TensorBoard reads them to build its visualizations:

```python skip-test
import os

log_dir = "tensorboard_logs"
os.makedirs(log_dir, exist_ok=True)
```

Second, run TensorBoard with the log directory and port number. The `--bind_all` flag makes TensorBoard listen on all network interfaces, so your browser can reach it through the published port from outside the container:

```python skip-test
import os

os.system("tensorboard --logdir='{}' --port 6006 --bind_all &".format(log_dir))
```

With port 6006 published, your TensorBoard dashboard is now available in the browser at the following URL:
[http://localhost:6006](http://localhost:6006)

TensorBoard keeps running in the background. If you run this code again, delete the `tensorboard_logs` directory first, or set `log_dir` to a new folder, so the new run's charts don't overlay the old ones.

The TensorBoard dashboard has no data to show yet. To give it data, use a Keras callback: an object that Keras calls during training. Create a [`TensorBoard`](https://www.tensorflow.org/api_docs/python/tf/keras/callbacks/TensorBoard) callback and pass it to the model's [`fit`](https://www.tensorflow.org/api_docs/python/tf/keras/Model#fit) method. With the settings below, it writes most of its logs at the end of each epoch:

```python skip-test
from tensorflow.keras.callbacks import TensorBoard

tensorboard_callback = TensorBoard(
    log_dir=log_dir,
    histogram_freq=1,
    write_graph=True,
    write_images=True,
    update_freq="epoch",
    profile_batch=2,
    embeddings_freq=1,
)
```

Each argument controls what the callback logs:

- `update_freq="epoch"` writes the loss and metrics once per epoch.
- `write_graph=True` logs the model's layer structure, which the Graphs tab shows as a conceptual graph.
- `histogram_freq=1` logs weight and bias histograms every epoch.
- `write_images=True` logs the weights as images each time `histogram_freq` logs histograms, so it has no effect when `histogram_freq` is 0.
- `profile_batch=2` profiles the second training batch. Viewing the profile needs an extra package; see [View the TensorFlow results](#view-the-tensorflow-results).
- `embeddings_freq=1` logs data for any embedding layers. This model has none; see [View the TensorFlow results](#view-the-tensorflow-results).

See the [`TensorBoard` callback reference](https://www.tensorflow.org/api_docs/python/tf/keras/callbacks/TensorBoard) for every option.

The final step is to pass the callback to the model's `fit` method. In the complete script below, that call is inside `train_model`, the function that Deephaven's [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function calls to train the model on table data:

```python skip-test
def train_model(x_train, y_train):
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(),
        metrics=["accuracy"],
    )
    model.fit(x=x_train, y=y_train, epochs=20, callbacks=[tensorboard_callback])
```

### Complete TensorFlow example

The complete script below does the following:

1. Reads the Iris dataset with [`read_csv`](../reference/data-import-export/CSV/readCsv.md).
2. Converts the class names to numbers with [`update`](../reference/table-operations/select/update.md). The `-> np.intc` [type hint](./python-functions.md#type-hints) on `get_class_number` makes the resulting `Class` column an `int` column.
3. Calls [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) twice: once to train the model, and once to create `iris_predicted_static`, a copy of `iris` with a `PredictedClass` column of predictions. [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input) and [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output) map table columns to and from the model, and [`gather`](/core/pydoc/code/deephaven.learn.gather.html#module-deephaven.learn.gather) converts columns to arrays. [How to use `deephaven.learn`](./use-deephaven-learn.md) explains these in detail.

Here is the full script:

```python skip-test
import os

# Set up the name of the log directory and run TensorBoard using port 6006
log_dir = "tensorboard_logs"
os.makedirs(log_dir, exist_ok=True)
os.system("tensorboard --logdir='{}' --port 6006 --bind_all &".format(log_dir))


# Deephaven imports
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

# Machine learning imports
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Input
from tensorflow.keras.callbacks import TensorBoard

# Additional required imports
import numpy as np

# Read the Iris dataset and convert the class names to numbers
iris_raw = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

iris_mappings = {"Iris-setosa": 0, "Iris-virginica": 1, "Iris-versicolor": 2}


def get_class_number(c) -> np.intc:
    return iris_mappings[c]


iris = iris_raw.update(formulas=["Class = get_class_number(Class)"])

# Our neural network
model = Sequential()
model.add(Input(shape=(4,)))
model.add(Dense(16, activation=tf.nn.relu))
model.add(Dense(12, activation=tf.nn.relu))
model.add(Dense(3, activation=tf.nn.softmax))

# Create the TensorBoard callback
tensorboard_callback = TensorBoard(
    log_dir=log_dir,
    histogram_freq=1,
    write_graph=True,
    write_images=True,
    update_freq="epoch",
    profile_batch=2,
    embeddings_freq=1,
)


# A function that trains the model and passes tensorboard_callback to fit
def train_model(x_train, y_train):
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(),
        metrics=["accuracy"],
    )
    model.fit(x=x_train, y=y_train, epochs=20, callbacks=[tensorboard_callback])


# A function that gets the model's predictions on input data
def predict_with_model(features):
    predictions = model.predict(features)
    return [np.argmax(item) for item in predictions]


# A function to gather data from table columns into a NumPy array of doubles
def table_to_array_double(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.double)


# A function to gather data from table columns into a NumPy array of integers
def table_to_array_int(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.intc)


# A function to extract a list element at a given index
def get_predicted_class(data, idx):
    return data[idx]


# Use the learn function to train our neural network
learn.learn(
    table=iris,
    model_func=train_model,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_array_double,
        ),
        learn.Input(["Class"], table_to_array_int),
    ],
    outputs=None,
    batch_size=150,
)

# Use the learn function to create a new table that contains predicted values
iris_predicted_static = learn.learn(
    table=iris,
    model_func=predict_with_model,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_array_double,
        )
    ],
    outputs=[learn.Output("PredictedClass", get_predicted_class, "int")],
    batch_size=150,
)
```

### View the TensorFlow results

After training the model, you can open the TensorBoard dashboard at [http://localhost:6006](http://localhost:6006) and explore the data the callback logged.

The Scalars tab shows how the loss, the metrics, and the optimizer's learning rate changed over the epochs. To also log training speed, add `write_steps_per_second=True` to the callback. The following screenshot shows the loss and accuracy charts:

![A TensorBoard Scalars tab showing graphs that track epoch accuracy and loss](../assets/how-to/tensorboard-scalars-tf.png)

To see your model's layers and check that the architecture looks as intended, open the Graphs tab, choose the `keras` tag, and select **Conceptual graph** under **Graph type**. Then expand the `sequential` node to show the layers. The tab opens on the `batch_2` tag instead, which the `profile_batch` argument adds. In the current `server-tensorflow` image, that op graph is empty, and TensorBoard reports that it couldn't trace a graph.

![The TensorBoard Graphs tab showing the conceptual graph of the model, with its input layer and three dense layers](../assets/how-to/tensorboard-graphs-tf.png)

The Distributions and Histograms tabs show how the weights and biases changed over the epochs. The `histogram_freq` argument logs this data.

The Images tab draws the weights that `write_images` logs as images. It logs them on the same epochs as the histograms.

Two callback arguments log data that this example can't fully display:

- `embeddings_freq`: this model has no embedding layers, so there is nothing to project. The callback still saves a weights checkpoint to the log directory's `train` folder every epoch.
- `profile_batch`: the callback records a performance profile of batch 2 and adds a `batch_2` tag to the Graphs tab, which is empty in the current `server-tensorflow` image. To display the profile, TensorBoard needs the `tensorboard-plugin-profile` package, which the `server-tensorflow` image doesn't include. To add it, see [How to install and use Python packages](./install-and-use-python-packages.md).

## Use TensorBoard with PyTorch

This example uses the `server-pytorch` image, which includes PyTorch and TensorBoard. Start it with the [PyTorch `docker-compose.yml` file](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/PyTorch/docker-compose.yml).

This example classifies the Iris dataset with Deephaven tables, using the same PyTorch neural network (`IrisANN`) built in the [PyTorch guide](./use-pytorch.md#classify-the-iris-dataset-with-deephaven-tables). You only need to add a few lines of code to enable TensorBoard!

### Enable PyTorch logging and start TensorBoard

First, create a log directory and run TensorBoard with that directory and port number. The `--bind_all` flag makes TensorBoard listen on all network interfaces, so your browser can reach it through the published port from outside the container:

```python skip-test
import os

log_dir = "tensorboard_logs"
os.makedirs(log_dir, exist_ok=True)
os.system("tensorboard --logdir='{}' --port 6006 --bind_all &".format(log_dir))
```

With port 6006 published, your TensorBoard dashboard is now available in the browser at the following URL:
[http://localhost:6006](http://localhost:6006)

TensorBoard keeps running in the background. If you run this code again, delete the `tensorboard_logs` directory first, or set `log_dir` to a new folder, so the new run's charts don't overlay the old ones.

Second, create a [`SummaryWriter`](https://pytorch.org/docs/stable/tensorboard.html#torch.utils.tensorboard.writer.SummaryWriter) instance that writes the logs TensorBoard reads:

```python skip-test
from torch.utils.tensorboard import SummaryWriter

writer = SummaryWriter(log_dir)
```

The TensorBoard dashboard has no data to show yet. To give it something to show, use the `SummaryWriter` to log the metrics you want to track, such as the training loss. In the complete script below, `train_model` runs a loop over the epochs, where `i` is the epoch number and `loss` is that epoch's loss. Inside that loop, this line logs the loss for each epoch:

```python skip-test
writer.add_scalar("Loss/train", loss, i)
```

By default, `SummaryWriter` writes pending data to disk once 10 events are queued or every two minutes, whichever comes first. To make the dashboard show the final results right away, call `flush` on the writer when training finishes. The complete script below does this at the end of `train_model`:

```python skip-test
writer.flush()
```

### Complete PyTorch example

The complete script below does the following:

1. Reads the Iris dataset with [`read_csv`](../reference/data-import-export/CSV/readCsv.md).
2. Converts the class names to numbers with [`update`](../reference/table-operations/select/update.md). The `-> np.intc` [type hint](./python-functions.md#type-hints) on `get_class_number` makes the resulting `Class` column an `int` column.
3. Calls [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) twice: once to train the model, and once to create `iris_predicted_static`, a copy of `iris` with a `PredictedClass` column of predictions. [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input) and [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output) map table columns to and from the model, and [`gather`](/core/pydoc/code/deephaven.learn.gather.html#module-deephaven.learn.gather) converts columns to arrays. [How to use `deephaven.learn`](./use-deephaven-learn.md) explains these in detail.

Here is the full script:

```python skip-test
import os

# Set up the name of the log directory and run TensorBoard using port 6006
log_dir = "tensorboard_logs"
os.makedirs(log_dir, exist_ok=True)
os.system("tensorboard --logdir='{}' --port 6006 --bind_all &".format(log_dir))

# Deephaven imports
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

# Machine learning imports
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.tensorboard import SummaryWriter

# Python imports
import numpy as np


# Read the Iris dataset and convert the class names to numbers
iris_raw = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

iris_mappings = {"Iris-setosa": 0, "Iris-virginica": 1, "Iris-versicolor": 2}


def get_class_number(c) -> np.intc:
    return iris_mappings[c]


iris = iris_raw.update(formulas=["Class = get_class_number(Class)"])


# Our neural network class
class IrisANN(nn.Module):
    def __init__(self):
        super().__init__()
        self.fc1 = nn.Linear(in_features=4, out_features=16)
        self.fc2 = nn.Linear(in_features=16, out_features=12)
        self.output = nn.Linear(in_features=12, out_features=3)

    def forward(self, x):
        x = x.float()
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.output(x)
        return x


# Create the neural network
model = IrisANN()

# SummaryWriter to log data for visualization by TensorBoard
writer = SummaryWriter(log_dir)


# A function that trains the model
def train_model(x_train, y_train):
    global model
    # Set training parameters
    criterion = nn.CrossEntropyLoss()

    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    epochs = 50

    for i in range(epochs):
        y_hat = model(x_train)
        loss = criterion(y_hat, y_train.long())
        writer.add_scalar("Loss/train", loss, i)

        if i % 10 == 0:
            print(f"Epoch: {i} Loss: {loss.item():.4f}")

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

    # Write any pending data so TensorBoard shows the full training run
    writer.flush()


# A function that gets the model's predictions from input data
def predict_with_model(features):
    preds = []

    with torch.no_grad():
        for val in features:
            y_hat = model(val)
            preds.append(y_hat.argmax().item())

    # Return the model's predictions
    return preds


# A function to gather data from table columns into a torch tensor of doubles
def table_to_tensor_double(rows, cols):
    return torch.from_numpy(gather.table_to_numpy_2d(rows, cols, np_type=np.double))


# A function to gather data from table columns into a torch tensor of integers
def table_to_tensor_int(rows, cols):
    return torch.from_numpy(
        np.squeeze(gather.table_to_numpy_2d(rows, cols, np_type=np.intc))
    )


# A function to extract a prediction and cast the value to an integer
def get_predicted_class(data, idx):
    return int(data[idx])


# Use the learn function to train our neural network
learn.learn(
    table=iris,
    model_func=train_model,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_tensor_double,
        ),
        learn.Input("Class", table_to_tensor_int),
    ],
    outputs=None,
    batch_size=150,
)

# Use the learn function to create a new table that contains predicted values
iris_predicted_static = learn.learn(
    table=iris,
    model_func=predict_with_model,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_tensor_double,
        )
    ],
    outputs=[learn.Output("PredictedClass", get_predicted_class, "int")],
    batch_size=150,
)
```

### View the PyTorch results

After training the model, you can open the TensorBoard dashboard at [http://localhost:6006](http://localhost:6006) and see the results of your neural network's training runs:

![The TensorBoard Scalars tab showing the Loss/train curve over 50 epochs](../assets/how-to/tensorboard-scalars-pytorch.png)

This example logs only the training loss, so the dashboard shows a single curve on the Scalars tab. To fill in TensorBoard's other tabs, log more data with `SummaryWriter` in `train_model`:

- [`add_histogram`](https://pytorch.org/docs/stable/tensorboard.html#torch.utils.tensorboard.writer.SummaryWriter.add_histogram) logs histograms of weights and biases.
- [`add_graph`](https://pytorch.org/docs/stable/tensorboard.html#torch.utils.tensorboard.writer.SummaryWriter.add_graph) logs the model graph.
- [`add_embedding`](https://pytorch.org/docs/stable/tensorboard.html#torch.utils.tensorboard.writer.SummaryWriter.add_embedding) logs embeddings.
- [`add_hparams`](https://pytorch.org/docs/stable/tensorboard.html#torch.utils.tensorboard.writer.SummaryWriter.add_hparams) logs hyperparameters.

## Related documentation

- [Docker install guide](../getting-started/docker-install.md)
- [How to install and use Python packages](./install-and-use-python-packages.md)
- [How to use `deephaven.learn`](./use-deephaven-learn.md)
- [How to use PyTorch](./use-pytorch.md)
- [How to use TensorFlow](./use-tensorflow.md)
- [Python functions in query strings](./python-functions.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`update`](../reference/table-operations/select/update.md)
- [`deephaven.learn` Pydoc](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn)
- [`deephaven.learn.gather` Pydoc](/core/pydoc/code/deephaven.learn.gather.html#module-deephaven.learn.gather)
