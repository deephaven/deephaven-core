---
title: Use PyTorch in Deephaven
---

This guide shows you how to use [PyTorch](https://pytorch.org) in Deephaven queries.

[PyTorch](https://pytorch.org/) is an open-source machine learning library for Python, widely used for deep learning, with a Pythonic API for building and training neural networks. Given Deephaven's strength in real-time and big-data processing, it is natural to pair the two.

The two examples below solve the same problem: classifying the [Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set) from [Deephaven's examples repository](https://github.com/deephaven/examples). The first example uses [PyTorch](https://pytorch.org/) with [pandas](https://pandas.pydata.org/), whereas the second integrates Deephaven tables to perform predictions on live data.

## Install PyTorch

[PyTorch](https://pytorch.org) is not included in Deephaven's default `server` Docker image. To use it within Deephaven, you can [install it yourself](./install-and-use-python-packages.md) or choose one of the [Deephaven Docker deployments](../getting-started/docker-install.md#choose-a-deployment) that includes it:

- Without example data:
  - [Python with PyTorch](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/PyTorch/docker-compose.yml)
  - [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/All-AI/docker-compose.yml)
- With example data:
  - [Python with PyTorch](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/PyTorch/docker-compose.yml)
  - [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/All-AI/docker-compose.yml)

## The Iris dataset

The Iris flower dataset is a popular dataset commonly used in introductory machine learning applications. Introduced by R.A. Fisher in his 1936 paper, _The use of multiple measurements in taxonomic problems_, it contains 150 samples, 50 from each of three species of Iris flower. The following values are measured in centimeters for Iris-setosa, Iris-virginica, and Iris-versicolor flowers:

- Petal length
- Petal width
- Sepal length
- Sepal width

Predicting a flower's species from these measurements is a classification problem suitable for a supervised machine learning algorithm.

> [!NOTE]
> In this guide, we read data from [Deephaven's examples repository](https://github.com/deephaven/examples). You can also load files from the `/data` directory, which the [Deephaven Docker Compose deployments](../getting-started/docker-install.md) mount into the container from `./data` in the deployment directory. See [Docker data volumes](../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

## Classify the Iris dataset with pandas

This first example uses [PyTorch](https://pytorch.org/) and [pandas](https://pandas.pydata.org/) to classify Iris flowers from measurements. We define and create a feed-forward neural network, train it, and test it. In a feed-forward network, data passes through each layer once, in one direction, from input to output. We measure accuracy as the percentage of test-set predictions that match the known classes.

Let's first import all the packages we need.

```python docker-config=pytorch test-set=1 order=null
import pandas as pd
import torch
import torch.nn as nn
import torch.nn.functional as F
```

Next, we import our data, encode the class labels we want to predict as integers, and split the data into training and testing sets. The `random_state` argument fixes the shuffle, so the split is the same every run.

```python test-set=1 order=null
# Read the dataset and encode the class labels as integers
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

# Convert our training and testing sets to torch tensors
x_train = torch.tensor(x_train, dtype=torch.float32)
x_test = torch.tensor(x_test, dtype=torch.float32)
y_train = torch.tensor(y_train, dtype=torch.long)
y_test = torch.tensor(y_test, dtype=torch.long)
```

With that done, we can define and create our neural network. We use a simple feed-forward neural network with two hidden layers. Both hidden layers use the [ReLU activation function](https://pytorch.org/docs/stable/generated/torch.nn.ReLU.html). The seed fixes the network's starting weights, so training gives the same result every run.

```python test-set=1 order=null
# Our neural network class (simple feed-forward neural network)
class IrisANN(nn.Module):
    def __init__(self):
        # Specify two hidden layers in the neural network
        super().__init__()
        self.fc1 = nn.Linear(in_features=4, out_features=16)
        self.fc2 = nn.Linear(in_features=16, out_features=12)
        self.output = nn.Linear(in_features=12, out_features=3)

    def forward(self, x):
        # Specify what activation functions are used at each layer
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.output(x)
        return x


# Fix the starting weights, then create an instance of our neural network
torch.manual_seed(42)
model = IrisANN()
```

Now comes the fun bit: training our network. Each training epoch is one full pass of the training data through the network. We calculate loss using [cross entropy](https://pytorch.org/docs/stable/generated/torch.nn.CrossEntropyLoss.html), and we optimize the network with the [Adam algorithm](https://pytorch.org/docs/stable/generated/torch.optim.Adam.html).

```python test-set=1 order=null
# Set our loss computation and optimization algorithms
criterion = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

# Train the network over 100 epochs
epochs = 100

for i in range(epochs):
    # Run data through the network
    y_hat = model(x_train)
    loss = criterion(y_hat, y_train)

    if i % 10 == 0:
        print(f"Epoch: {i} Loss: {loss.item():.4f}")

    # Optimize the network
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
```

Lastly, we check how well our model worked.

```python test-set=1 order=null
# Save the predictions to a list
preds = []

with torch.no_grad():
    for val in x_test:
        y_hat = model(val)
        preds.append(y_hat.argmax().item())

# Calculate and display how well the network did
df = pd.DataFrame({"Y": y_test, "YHat": preds})
df["Correct"] = [1 if corr == pred else 0 for corr, pred in zip(df["Y"], df["YHat"])]
accuracy = df["Correct"].sum() / len(df) * 100
print("Neural Network accuracy: " + str(accuracy) + "%")
```

A simple model like this typically classifies most of the test set correctly. Without the fixed seeds, the data split and the starting weights would be random, and the exact numbers would vary between runs.

This example follows a basic formula for using a neural network to solve a supervised learning problem:

- Import data from an external source and encode the target labels as integers.
- Split the data into training and testing sets, and convert them to tensors.
- Define the machine learning model.
- Set the loss calculation and optimization routines.
- Train the model over a set number of training epochs.
- Calculate the model's accuracy on the testing set.

In this case, a simple neural network is suitable for a simple dataset.

## Classify the Iris dataset with Deephaven tables

So we just classified the Iris dataset using a simple feed-forward neural network. That's kind of cool, but the dataset is static. It would be cooler to classify a live feed of incoming data, and with Deephaven, the same model can do that.

Let's get there in two steps. First, we train the model and make predictions on a static Deephaven table. Then we use the trained model on a [ticking table](../conceptual/table-update-model.md), a table whose rows change as new data arrives. The [`deephaven.learn`](./use-deephaven-learn.md) package connects the model to both tables.

### Train and predict on a static table

We start by importing everything we need.

```python test-set=1 order=null
# Deephaven imports
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

# Machine learning imports
import torch
import torch.nn as nn
import torch.nn.functional as F

# Python imports
import numpy as np
import random
import threading
import time
```

Just as we did before, we next import our data and encode the class labels as integers. This time, we use [`read_csv`](../reference/data-import-export/CSV/readCsv.md) to import it into a Deephaven table. The `get_class_number` function maps each class name to the same integers as the first example, and [`update`](../reference/table-operations/select/update.md) calls it from a [query string](./python-functions.md) on every row to replace the `Class` strings with those integers.

```python test-set=1 order=iris_raw,iris
# Read the Iris dataset and encode the class labels as integers
iris_raw = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)

iris_mappings = {"Iris-setosa": 0, "Iris-virginica": 1, "Iris-versicolor": 2}


def get_class_number(c) -> np.intc:
    return iris_mappings[c]


iris = iris_raw.update(formulas=["Class = get_class_number(Class)"])
```

Next, we define and create the neural network. It is the same `IrisANN` network as in the first example, with one added line, `x = x.float()`. The gather function later in this section delivers the measurements as 64-bit doubles, and this line converts them to the 32-bit floats the network's layers use. As before, the seed fixes the starting weights.

```python test-set=1 order=null
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


# Fix the starting weights, then create the neural network
torch.manual_seed(42)
model = IrisANN()
```

This time, the [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function runs the model on table data. It gathers table columns into model inputs, calls a model function on each batch of rows, and scatters the results into columns of an output table. [Use `deephaven.learn` for AI/ML applications](./use-deephaven-learn.md) covers each step in detail.

[`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) needs three kinds of function:

- A model function, which trains the model or makes predictions.
- A gather function for each input.
- A scatter function for each output.

This guide calls [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) twice, once to train and once to predict, so it needs two model functions. We start with those, wrapping training and prediction so that [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) can call them. The training loop is the same as in the first example, except that it converts the targets with `long`. The gather function delivers the class labels as 32-bit integers, and `CrossEntropyLoss` expects 64-bit integers.

```python test-set=1 order=null
# A function that trains the model
def train_model(x_train, y_train):
    global model
    # Set training parameters
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    epochs = 100

    for i in range(epochs):
        y_hat = model(x_train)
        loss = criterion(y_hat, y_train.long())

        if i % 10 == 0:
            print(f"Epoch: {i} Loss: {loss.item():.4f}")

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


# A function that gets the model's predictions from input data
def predict_with_model(features):
    preds = []

    with torch.no_grad():
        for val in features:
            y_hat = model(val)
            preds.append(y_hat.argmax().item())

    # Return the model's predictions
    return preds
```

Next, we define the functions that [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) passes data through: two gather functions and one scatter function. The gather functions use [`gather.table_to_numpy_2d`](/core/pydoc/code/deephaven.learn.gather.html#deephaven.learn.gather.table_to_numpy_2d) to copy table data into NumPy arrays, and then `torch.from_numpy` turns each array into a PyTorch tensor.

- `table_to_tensor_double` gathers the measurement columns into a tensor of 64-bit doubles, which is why `forward` converts its input with `float`.
- `table_to_tensor_int` gathers the `Class` column into a tensor of 32-bit integers, which `train_model` converts with `long` to the 64-bit integers that `CrossEntropyLoss` expects.
- `get_predicted_class` extracts one prediction as an integer. [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) calls it once per row to fill the new output column.

```python test-set=1 order=null
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
```

Now we pass these functions to [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn), which runs them on the `iris` table. The first call to [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) trains the model. The second call uses the trained model to predict each row's species and writes the predictions to a new `PredictedClass` column.

```python test-set=1 order=iris_predicted_static
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

The `iris_predicted_static` table shows each row's `PredictedClass` beside its known `Class`, using the encoding from `iris_mappings`: 0 is Iris-setosa, 1 is Iris-virginica, and 2 is Iris-versicolor.

We've done much the same thing as before: static predictions on static data. The difference is that we use a Deephaven table, and the model trains and predicts on all 150 rows rather than on separate training and testing sets. That's not that exciting. We really want to perform the prediction stage on a _live_ data feed.

### Predict on live data

To demonstrate predictions on live data, we create simulated Iris measurements in a [ticking table](../conceptual/table-update-model.md). We can [create an in-memory ticking table](./table-publisher.md#dynamictablewriter) using [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md). To keep the simulated measurements realistic, we use the range of each measurement in the Iris dataset:

| Column          | Minimum (cm) | Maximum (cm) |
| --------------- | ------------ | ------------ |
| `PetalLengthCM` | 1.0          | 6.9          |
| `PetalWidthCM`  | 0.1          | 2.5          |
| `SepalLengthCM` | 4.3          | 7.9          |
| `SepalWidthCM`  | 2.0          | 4.4          |

The `write_to_iris` function below uses these ranges to write a simulated measurement to the table once per second for 30 seconds. We apply our model to those measurements as they arrive and predict which Iris species they belong to.

First, we create a [ticking table](../conceptual/table-update-model.md) and write data to it.

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


# This function creates simulated Iris measurements once per second for 30 seconds
def write_to_iris():
    for i in range(30):
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

Now we use [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) on the [ticking table](../conceptual/table-update-model.md). All it takes to change from static to live data is one argument: the call is the same as the static prediction above, except that the `table` argument is `live_iris`. On a [ticking table](../conceptual/table-update-model.md), [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn):

- Processes only the rows added in each [update cycle](../conceptual/table-update-model.md), in batches of at most `batch_size` rows, so each new measurement gets a prediction as it arrives.
- Works on [add-only](../conceptual/table-types.md#specialization-2-add-only) and [blink](../conceptual/table-types.md#specialization-3-blink) tables. By default, it raises an error on other ticking tables.

[`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) produces an [append-only table](../conceptual/table-types.md#specialization-1-append-only), which is a kind of [add-only table](../conceptual/table-types.md#specialization-2-add-only), so it qualifies. For tables whose rows are modified or removed, use one of the approaches in [AI/ML workflows](./ml-ai-no-learn.md).

```python test-set=1 ticking-table order=null skip-test
# Use the learn function to create a new table with live predictions
iris_predicted_live = learn.learn(
    table=live_iris,
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

As new measurements arrive, `iris_predicted_live` gains rows with a `PredictedClass` value for each one. The values use the encoding from `iris_mappings`: 0 is Iris-setosa, 1 is Iris-virginica, and 2 is Iris-versicolor.

<LoopedVideo src='../assets/how-to/irisPredicted_live_PyTorch.mp4' />

## Related documentation

- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Table types](../conceptual/table-types.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [How to install and use Python packages](./install-and-use-python-packages.md)
- [How to use `deephaven.learn`](./use-deephaven-learn.md)
- [AI/ML workflows](./ml-ai-no-learn.md)
- [How to use scikit-learn](./use-scikit-learn.md)
- [How to use TensorFlow](./use-tensorflow.md)
- [How to use TensorBoard](./use-tensorboard.md)
- [Python functions in query strings](./python-functions.md)
- [How to write data to an in-memory, real-time table](./table-publisher.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`update`](../reference/table-operations/select/update.md)
- [`deephaven.learn` Pydoc](/core/pydoc/code/deephaven.learn.html)
- [`deephaven.learn.gather` Pydoc](/core/pydoc/code/deephaven.learn.gather.html)
