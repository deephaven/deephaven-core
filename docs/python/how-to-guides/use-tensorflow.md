---
title: Use TensorFlow in Deephaven
sidebar_label: TensorFlow
---

This guide will show you how to use [TensorFlow](https://www.tensorflow.org/) in Deephaven queries.

[TensorFlow](https://www.tensorflow.org/) is an open-source machine learning library for Python. It is one of the most well known and widely used artificial intelligence libraries available. It is supported by the [Keras](https://keras.io/) interface, and boasts a variety of features to support the building, training, validation, and deployment of a wide variety of machine learning models.

[TensorFlow](https://www.tensorflow.org/) does not come stock with Deephaven's base Docker image. To use it within Deephaven, you can [install it yourself](./install-and-use-python-packages.md) or choose one of a [Deephaven Docker deployments](../getting-started/docker-install.md#choose-a-deployment) with support built-in. The following options will work:

- Without example data:
  - [Python with TensorFlow](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/TensorFlow/docker-compose.yml)
  - [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/All-AI/docker-compose.yml)
- With example data:
  - [Python with TensorFlow](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/TensorFlow/docker-compose.yml)
  - [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/All-AI/docker-compose.yml)

The two examples below both solve the same problem of classifying the [Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set), which can be found in [Deephaven's examples repository](https://github.com/deephaven/examples). The first example uses [TensorFlow](https://www.tensorflow.org/) alone, whereas the second integrates Deephaven tables.

> [!NOTE]
> In this guide, we read data from [Deephaven's examples repository](https://github.com/deephaven/examples). You can also load files that are in a mounted directory at the base of the Docker container. See [Docker data volumes](../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

The Iris flower dataset is a popular dataset commonly used in introductory machine learning applications. Created by R.A. Fisher in his 1936 paper, `The use of multiple measurements in taxonomic problems`, it contains 150 measurements of three types of Iris flower subspecies. The following values are measured in centimeters for Iris-setosa, Iris-virginica, and Iris-versicolor flowers:

- Petal length
- Petal width
- Sepal length
- Sepal width

This is a classification problem suitable for a supervised machine learning algorithm. We'll define and create a feed-forward neural network, train it, and test it. We'll determine its accuracy as a percentage based on the number of correct predictions compared to the known values.

## Classify the Iris dataset

This first example shows how to use [TensorFlow](https://www.tensorflow.org/) to classify Iris flowers from measurements.

Let's first import all the packages we'll need.

```python docker-config=tensorflow test-set=1 order=null skip-test
import pandas as pd
import random
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
```

Next, we'll import our data, quantize the targets we want to predict, and split the data into training and testing sets.

```python test-set=1 order=null skip-test
# Read and quantize the dataset
iris = pd.read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
iris_mappings = {"Iris-setosa": 0, "Iris-virginica": 1, "Iris-versicolor": 2}
iris["Class"] = iris["Class"].apply(lambda x: iris_mappings[x])

# Split the DataFrame into training and testing sets
iris_shuffled = iris.sample(frac=1)
train_size = int(0.75 * len(iris_shuffled))
train_set = iris_shuffled[:train_size]
test_set = iris_shuffled[train_size:]

# Separate our data into features and targets (X and Y)
X_train = train_set.drop("Class", axis=1).values
Y_train = train_set["Class"].values
X_test = test_set.drop("Class", axis=1).values
Y_test = test_set["Class"].values
```

With that done, we can define and create our neural network. We'll employ a simple feed-forward neural network with two hidden layers. Both hidden layers will use rectifier activation functions.

```python test-set=1 order=null skip-test
# Create the ANN
model = Sequential()
model.add(Dense(16, input_shape=(4,), activation=tf.nn.relu))
model.add(Dense(12, activation=tf.nn.relu))
model.add(Dense(3, activation=tf.nn.softmax))
```

Now comes the fun bit: training our network. We'll use [sparse categorical cross entropy](https://www.tensorflow.org/api_docs/python/tf/keras/losses/SparseCategoricalCrossentropy) to calculate loss, and the [Adam algorithm](https://keras.io/api/optimizers/adam/) to optimize the network.

```python test-set=1 order=null skip-test
# Compile, fit, and evaluate the predictions of the ANN
model.compile(
    optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
    loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
    metrics=["accuracy"],
)
model.fit(x=X_train, y=Y_train, epochs=100)
model.evaluate(X_test, Y_test)
```

This performance is pretty good for such a simple model.

Let's break down the code above into reusable steps that can be applied to a variety of supervised machine learning problems:

- Import, quantize, update, and store data from an external source.
- Define the machine learning model.
- Set the loss calculation and optimization routines.
- Train the model over a set number of training epochs.
- Calculate the model's accuracy.

In this case, a simple neural network is suitable for a simple dataset.

## Classify the Iris dataset with Deephaven tables

So we just classified the Iris dataset using a simple feed-forward neural network. That's kind of cool, but it would be cooler to classify a live feed of incoming data. Let's do that with Deephaven!

We extend our previous example to train our model on data in a Deephaven table. This requires some additional code.

First, we import everything we'll need.

```python test-set=1 order=null skip-test
# Deephaven imports
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

# Machine learning imports
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout

# Additional required imports
import numpy as np
import random, threading, time
```

Just as we did before, we next import our data. This time, we'll import it into a Deephaven table.

```python test-set=1 order=null skip-test
# Read and quantize the Iris dataset
iris_raw = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

classes = {}
num_classes = 0


def get_class_number(c) -> np.intc:
    global classes, num_classes
    if c not in classes:
        classes[c] = num_classes
        num_classes += 1
    return classes[c]


iris = iris_raw.update(formulas=["Class = get_class_number(Class)"])
```

Next we define and create the neural network.

```python test-set=1 order=null skip-test
# Our neural network
model = Sequential()
model.add(Dense(16, input_shape=(4,), activation=tf.nn.relu))
model.add(Dense(12, activation=tf.nn.relu))
model.add(Dense(3, activation=tf.nn.softmax))
```

This time, we'll create a function to train the neural network.

```python test-set=1 order=null skip-test
# A function that trains the model
def train_model(X_train, Y_train):
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"],
    )
    model.fit(x=X_train, y=Y_train, epochs=100)


# A function that gets the model's predictions on input data
def predict_with_model(features):
    if features.ndim == 1:
        features = np.expand_dims(features, 0)
    predictions = model.predict(features)
    return [np.argmax(item) for item in predictions]
```

Now we need a few extra functions. The first two gather data from a Deephaven table into NumPy arrays of doubles and integers, respectively, while the last extracts a value from the predictions. The extracted values will be used to create a new column in the output table.

```python test-set=1 order=null skip-test
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

Now, we can predict the Iris subspecies from these measurements. This time, we'll do it in a Deephaven table using the [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function. The first time we call [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn), we train the model. Then, we use the trained model to predict the values of the Iris subspecies in the `Class` column.

```python test-set=1 order=null skip-test
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

We've done the same thing as before: we created static predictions on static data. Only this time, we used a Deephaven table. That's not that exciting. We really want to perform the prediction stage on a _live_ data feed. To demonstrate this, we'll create some fake Iris measurements.

We can [create an in-memory real-time table](./table-publisher.md#dynamictablewriter) using [DynamicTableWriter](../reference/table-operations/create/DynamicTableWriter.md). To create semi-realistic measurements, we'll use some known quantities from the Iris dataset:

| Column          | Minimum (CM) | Maximum (CM) |
| --------------- | ------------ | ------------ |
| `PetalLengthCM` | 1.0          | 6.9          |
| `PetalWidthCM`  | 0.1          | 2.5          |
| `SepalLengthCM` | 4.3          | 7.9          |
| `SepalWidthCM`  | 2.0          | 4.4          |

These quantities will be fed to our table writer and faux measurements will be written to the table once per second for one minute. We'll apply our model on those measurements as they arrive and predict which Iris subspecies they belong to.

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


# This function creates faux Iris measurements once per second for a minute
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

Now we use [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) on the ticking table. All it takes to change from static to live data is to change the table!

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

Once we have the code to predict static data written, this is all it takes to make predictions on live data.

```python skip-test
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


# This function creates faux Iris measurements once per second for a minute
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

<LoopedVideo src='../assets/how-to/irisPredicted_live_TensorFlow.mp4' />

## Related documentation

- [How to install and use Python packages](./install-and-use-python-packages.md)
- [How to use deephaven.learn](./use-deephaven-learn.md)
- [How to use PyTorch](./use-pytorch.md)
- [How to use SciKit-Learn](./use-scikit-learn.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [How to write data to an in-memory, real-time table](./table-publisher.md)
- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [`aj`](../reference/table-operations/join/aj.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [DynamicTableWriter](../reference/table-operations/create/DynamicTableWriter.md)
- [`rename_columns`](../reference/table-operations/select/rename-columns.md)
- [`update`](../reference/table-operations/select/update.md)
  <!-- TODO: https://github.com/deephaven/deephaven.io/issues/785 REFERENCE: deephaven.learn -->
