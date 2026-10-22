---
title: Use TensorFlow in Deephaven
---

This guide shows how to use [TensorFlow](https://www.tensorflow.org/) in Deephaven queries.

[TensorFlow](https://www.tensorflow.org/) is an open-source machine learning library for Python, and one of the most widely used. Its high-level [Keras](https://keras.io/) API supports building, training, and deploying models.

Deephaven's `server` Docker image doesn't include [TensorFlow](https://www.tensorflow.org/). To use it, [install it yourself](./install-and-use-python-packages.md) or choose one of the [Deephaven Docker deployments](../getting-started/docker-install.md#choose-a-deployment) that includes it:

- Without example data:
  - [Python with TensorFlow](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/TensorFlow/docker-compose.yml)
  - [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/All-AI/docker-compose.yml)
- With example data:
  - [Python with TensorFlow](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/TensorFlow/docker-compose.yml)
  - [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/All-AI/docker-compose.yml)

Both examples below classify the [Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set) from [Deephaven's examples repository](https://github.com/deephaven/examples). The first example uses [TensorFlow](https://www.tensorflow.org/) with [pandas](https://pandas.pydata.org/). The second reads the data into a Deephaven table and uses [`deephaven.learn`](./use-deephaven-learn.md) to make predictions, first on a static table and then on a live table that gains rows as new data arrives.

> [!NOTE]
> In this guide, we read data from [Deephaven's examples repository](https://github.com/deephaven/examples). You can also load files from the `/data` directory in the Docker container, which Deephaven's Docker Compose deployments mount from `./data` in the deployment directory. See [Docker data volumes](../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

## Classify the Iris dataset with pandas

This first example shows how to use [TensorFlow](https://www.tensorflow.org/) to classify Iris flowers from measurements.

The Iris flower dataset is common in introductory machine learning applications. R.A. Fisher introduced the dataset in his 1936 paper *The use of multiple measurements in taxonomic problems*. It contains 150 samples, 50 from each of three species of Iris flower. For each Iris-setosa, Iris-virginica, and Iris-versicolor flower, the dataset records these measurements in centimeters:

- Petal length
- Petal width
- Sepal length
- Sepal width

Predicting a flower's species from labeled measurements is a classification problem, so it suits a supervised machine learning algorithm. We use a feed-forward neural network, in which data flows from the input layer through the hidden layers to the output layer with no loops.

The steps are to define the network, train it on most of the data, and test it on the rest. Its accuracy is the fraction of test-set predictions that match the known species.

First, we import the packages the example needs.

```python docker-config=tensorflow test-set=1 order=null
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Input
```

Next, we import the data, convert the `Class` labels from flower names to the integers 0, 1, and 2, and split the data into training and testing sets. The `random_state` argument fixes the shuffle, so the split is the same every run.

```python test-set=1 order=null
# Read the dataset and convert class labels to integers
iris = pd.read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
iris_mappings = {"Iris-setosa": 0, "Iris-virginica": 1, "Iris-versicolor": 2}
iris["Class"] = iris["Class"].apply(lambda x: iris_mappings[x])

# Split the DataFrame into training and testing sets
iris_shuffled = iris.sample(frac=1, random_state=42)
train_size = int(0.75 * len(iris_shuffled))
train_set = iris_shuffled[:train_size]
test_set = iris_shuffled[train_size:]

# Separate our data into features and targets (x and y)
x_train = train_set.drop("Class", axis=1).values
y_train = train_set["Class"].values
x_test = test_set.drop("Class", axis=1).values
y_test = test_set["Class"].values
```

With that done, we can define and create our neural network. We use a simple feed-forward neural network with two hidden layers. Both hidden layers use the [ReLU (rectified linear unit)](https://www.tensorflow.org/api_docs/python/tf/nn/relu) activation function. The output layer has three units, one per species, and uses [softmax](https://www.tensorflow.org/api_docs/python/tf/nn/softmax) to turn them into class probabilities. Setting a random seed first fixes the network's starting weights, so training gives the same result each run.

```python test-set=1 order=null
# Fix the starting weights, then create the neural network
tf.keras.utils.set_random_seed(42)
model = Sequential()
model.add(Input(shape=(4,)))
model.add(Dense(16, activation=tf.nn.relu))
model.add(Dense(12, activation=tf.nn.relu))
model.add(Dense(3, activation=tf.nn.softmax))
```

Now comes the fun part: training the network. We use [sparse categorical cross entropy](https://www.tensorflow.org/api_docs/python/tf/keras/losses/SparseCategoricalCrossentropy) to calculate loss, and the [Adam algorithm](https://keras.io/api/optimizers/adam/) to optimize the network. Training runs for 100 epochs. An epoch is one full pass over the training data.

```python test-set=1 order=null
# Compile, fit, and evaluate the neural network
model.compile(
    optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
    loss=tf.keras.losses.SparseCategoricalCrossentropy(),
    metrics=["accuracy"],
)
model.fit(x=x_train, y=y_train, epochs=100)
model.evaluate(x_test, y_test)
```

`model.evaluate` prints the loss and accuracy on the test set. Without the fixed seeds, the data split and the starting weights would be random, and the exact numbers would vary between runs.

Let's break down the code above into reusable steps that apply to a variety of supervised machine learning problems:

- Import data from an external source.
- Convert the class labels to integers.
- Split the data into training and testing sets.
- Define the machine learning model.
- Set the loss calculation and optimization routines.
- Train the model over a set number of training epochs.
- Calculate the model's accuracy.

So we just classified the Iris dataset with a simple feed-forward neural network. That's kind of cool, but it would be cooler to classify a live feed of incoming data. Let's do that with Deephaven!

## Classify the Iris dataset with Deephaven tables

This example reads the same Iris data into a Deephaven table, trains a model with the same architecture on all 150 rows with [`deephaven.learn`](./use-deephaven-learn.md), and uses it to predict species first on that static table and then on a live, [append-only](../conceptual/table-types.md#specialization-1-append-only) table that gains rows as new data arrives.

[`deephaven.learn`](./use-deephaven-learn.md) works on static tables and on [add-only](../conceptual/table-types.md#specialization-2-add-only) and [blink](../conceptual/table-types.md#specialization-3-blink) ticking tables. [Append-only](../conceptual/table-types.md#specialization-1-append-only) tables are a kind of add-only table. On any other ticking table, it raises an error. For tables that modify or remove rows, see [AI/ML workflows](./ml-ai-no-learn.md).

### Train and predict on a static table

First, we import everything the example needs, including what the [live-table step](#predict-on-a-live-table) uses to write synthetic data: the [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) class, the [`deephaven.dtypes`](/core/pydoc/code/deephaven.dtypes.html#module-deephaven.dtypes) module, and the `random`, `threading`, and `time` modules.

```python test-set=1 order=null
# Deephaven imports
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

# Machine learning imports
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Input

# Additional required imports
import numpy as np
import random
import threading
import time
```

As in the [pandas example](#classify-the-iris-dataset-with-pandas), we next import the data and convert the class labels, but this time into a Deephaven table with [`read_csv`](../reference/data-import-export/CSV/readCsv.md). The `get_class_number` function maps each class name to the same integers as the first example, and [`update`](../reference/table-operations/select/update.md) calls it on every row. The `-> np.intc` [type hint](./python-functions.md#type-hints) makes the replacement `Class` column an `int` column.

```python test-set=1 order=null
# Read the Iris dataset and convert class labels to integers
iris_raw = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

iris_mappings = {"Iris-setosa": 0, "Iris-virginica": 1, "Iris-versicolor": 2}


def get_class_number(c) -> np.intc:
    return iris_mappings[c]


iris = iris_raw.update(formulas=["Class = get_class_number(Class)"])
```

Next, we define the same neural network as in the first example: two ReLU hidden layers and a three-unit softmax output layer. As before, the seed fixes the starting weights.

```python test-set=1 order=null
# Fix the starting weights, then create our neural network
tf.keras.utils.set_random_seed(42)
model = Sequential()
model.add(Input(shape=(4,)))
model.add(Dense(16, activation=tf.nn.relu))
model.add(Dense(12, activation=tf.nn.relu))
model.add(Dense(3, activation=tf.nn.softmax))
```

The [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function passes a table's data to a model in batches of up to `batch_size` rows. Three of its arguments define what the model reads, what runs, and what it writes:

- `inputs` is a list of [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input) objects. Each one names the columns to read and a function that gathers them into an array. This [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input) is unrelated to the Keras `Input` layer in the model.
- `model_func` is the function that receives the gathered arrays.
- `outputs` is a list of [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output) objects. Each one names a new column and a function that fills it.

See [How to use `deephaven.learn`](./use-deephaven-learn.md) for details.

Because [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) calls the model through `model_func`, we wrap training in a function, `train_model`, and add a second function, `predict_with_model`, that returns the most likely class (0, 1, or 2) for each row of input features.

```python test-set=1 order=null
# A function that trains the model
def train_model(x_train, y_train):
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(),
        metrics=["accuracy"],
    )
    model.fit(x=x_train, y=y_train, epochs=100)


# A function that gets the model's predictions on input data
def predict_with_model(features):
    predictions = model.predict(features)
    return [np.argmax(item) for item in predictions]
```

[`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) also calls the functions you pass to each [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input) and [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output):

- `table_to_array_double` and `table_to_array_int` are [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input) functions. Each calls [`gather.table_to_numpy_2d`](/core/pydoc/code/deephaven.learn.gather.html#deephaven.learn.gather.table_to_numpy_2d) to gather the current batch's rows from the named columns into a NumPy array of doubles or integers.
- `get_predicted_class` is a [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output) function. [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) calls it once per row, with the model's predictions for the batch and that row's position in the batch. Its return value fills the row's `PredictedClass` cell.

```python test-set=1 order=null
# A function to gather data from table columns into a NumPy array of doubles
def table_to_array_double(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.double)


# A function to gather data from table columns into a NumPy array of integers
def table_to_array_int(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.intc)


# A function to extract a list element at a given index
def get_predicted_class(data, idx):
    return data[idx]
```

Now we can train the model and predict the Iris species in a Deephaven table with [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn).

The first call trains the model, so it has no outputs (`outputs=None`). The second call uses the trained model to predict the Iris species of each row and stores the predictions in a new `PredictedClass` column.

```python test-set=1 order=iris_predicted_static
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

`iris_predicted_static` is the `iris` table plus a `PredictedClass` column, next to the known `Class` column.

This example trains on every row, so these are predictions on the training data. They don't measure accuracy. To measure it, hold out a test set as the [pandas example](#classify-the-iris-dataset-with-pandas) does, train on the other rows, and compare `PredictedClass` with `Class` on the held-out rows.

### Predict on a live table

So far, we've made static predictions on static data, only this time in a Deephaven table. That's not that exciting. What we really want is to predict on a live data feed as it arrives.

The model trained in [Train and predict on a static table](#train-and-predict-on-a-static-table) can do that. This step reuses the imports and functions defined there. It writes synthetic Iris measurements to a live table and predicts the species of each new row.

We can [create an in-memory real-time table](./table-publisher.md#dynamictablewriter) using [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md). The table it produces is [append-only](../conceptual/table-types.md#specialization-1-append-only), so the prediction table keeps every row. [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) works on this table because append-only is one of the [table types `learn` supports](#classify-the-iris-dataset-with-deephaven-tables).

[`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) writes one row at a time, which is what this example needs. For most other live data, the [real-time table guide](./table-publisher.md) recommends [`table_publisher`](../reference/table-operations/create/TablePublisher.md).

To create synthetic measurements, we use these ranges from the Iris dataset:

| Column          | Minimum (cm) | Maximum (cm) |
| --------------- | ------------ | ------------ |
| `PetalLengthCM` | 1.0          | 6.9          |
| `PetalWidthCM`  | 0.1          | 2.5          |
| `SepalLengthCM` | 4.3          | 7.9          |
| `SepalWidthCM`  | 2.0          | 4.4          |

The `write_to_iris` function uses these ranges to write one synthetic measurement to the table each second for one minute.

First, we create a live table and write data to it.

```python test-set=1 ticking-table order=null skip-test
# Create the table writer
table_writer = DynamicTableWriter(
    {
        "SepalLengthCM": dht.double,
        "SepalWidthCM": dht.double,
        "PetalLengthCM": dht.double,
        "PetalWidthCM": dht.double,
    }
)

# Get the live, ticking table
live_iris = table_writer.table


# This function creates synthetic Iris measurements once per second for a minute
def write_to_iris():
    for i in range(60):
        petal_length = random.randint(10, 69) / 10
        petal_width = random.randint(1, 25) / 10
        sepal_length = random.randint(43, 79) / 10
        sepal_width = random.randint(20, 44) / 10

        table_writer.write_row(sepal_length, sepal_width, petal_length, petal_width)
        time.sleep(1)


# Use a thread to write data to the table
thread = threading.Thread(target=write_to_iris)
thread.start()
```

Now we use [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) on the live table. The call matches the static prediction call above, except that the `table` argument is now `live_iris`, the [append-only](../conceptual/table-types.md#specialization-1-append-only) table from [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md). That's all it takes to move from static to live predictions.

```python test-set=1 ticking-table order=null skip-test
# Use the learn function to create a new table with live predictions
iris_predicted_live = learn.learn(
    table=live_iris,
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

As new measurements arrive, `iris_predicted_live` gains rows with a `PredictedClass` value for each one:

<LoopedVideo src='../assets/how-to/irisPredicted_live_TensorFlow.mp4' />

## Related documentation

- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [Table types](../conceptual/table-types.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [How to install and use Python packages](./install-and-use-python-packages.md)
- [How to use `deephaven.learn`](./use-deephaven-learn.md)
- [AI/ML workflows](./ml-ai-no-learn.md)
- [How to use PyTorch](./use-pytorch.md)
- [How to use scikit-learn](./use-scikit-learn.md)
- [How to use TensorBoard](./use-tensorboard.md)
- [Python functions in query strings](./python-functions.md)
- [How to write data to an in-memory, real-time table](./table-publisher.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`table_publisher`](../reference/table-operations/create/TablePublisher.md)
- [`update`](../reference/table-operations/select/update.md)
- [`deephaven.learn` Pydoc](/core/pydoc/code/deephaven.learn.html)
- [`deephaven.learn.gather` Pydoc](/core/pydoc/code/deephaven.learn.gather.html)
- [`deephaven.dtypes` Pydoc](/core/pydoc/code/deephaven.dtypes.html)
