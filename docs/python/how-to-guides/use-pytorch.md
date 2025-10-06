---
title: Use PyTorch in Deephaven
sidebar_label: PyTorch
---

This guide will show you how to use [PyTorch](https://pytorch.org) in Deephaven queries.

[PyTorch](https://pytorch.org/) is an open source machine learning library for use in Python. It is one of the most well known and widely used artificial intelligence libraries currently available. It boasts an easy-to-learn Pythonic API that supports a wide variety of applications and features. Given Deephaven's strength in real time and big data processing, it is natural to pair the two.

[PyTorch](https://pytorch.org) does not come stock with Deephaven's base Docker image. To use it within Deephaven, you can [install it yourself](./install-and-use-python-packages.md) or choose one of a [Deephaven Docker deployments](../getting-started/docker-install.md#choose-a-deployment) with support built-in. The following options will work:

- Without example data:
  - [Python with PyTorch](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/PyTorch/docker-compose.yml)
  - [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/All-AI/docker-compose.yml)
- With example data:
  - [Python with PyTorch](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/PyTorch/docker-compose.yml)
  - [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/All-AI/docker-compose.yml)

The two examples below both solve the same problem of classifying the [Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set), which can be found in [Deephaven's examples repository](https://github.com/deephaven/examples). The first example uses [PyTorch](https://pytorch.org/) alone, whereas the second integrates Deephaven tables to perform predictions on live data.

> [!NOTE]
> In this guide, we read data from [Deephaven's examples repository](https://github.com/deephaven/examples). You can also load files that are in a mounted directory at the base of the Docker container. See [Docker data volumes](../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

The Iris flower dataset is a popular dataset commonly used in introductory machine learning applications. Created by R.A. Fisher in his 1936 paper, `The use of multiple measurements in taxonomic problems`, it contains 150 measurements of three types of Iris flower subspecies. The following values are measured in centimeters for Iris-setosa, Iris-virginica, and Iris-versicolor flowers:

- Petal length
- Petal width
- Sepal length
- Sepal width

This is a classification problem suitable for a supervised machine learning algorithm. We'll define and create a feed-forward neural network, train it, and test it. We'll determine its accuracy as a percentage based on the number of correct predictions compared to the known values.

## Classify the Iris dataset

This first example shows how to use [PyTorch](https://pytorch.org/) to classify Iris flowers from measurements.

Let's first import all the packages we'll need.

```python docker-config=pytorch test-set=1 order=null skip-test
import pandas as pd
import random
import torch
import torch.nn as nn
import torch.nn.functional as F
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

# Convert our training and testing sets to torch tensors
X_train = torch.FloatTensor(X_train)
X_test = torch.FloatTensor(X_test)
Y_train = torch.LongTensor(Y_train)
Y_test = torch.LongTensor(Y_test)
```

With that done, we can define and create our neural network. We'll employ a simple feed-forward neural network with two hidden layers. Both hidden layers will use the [ReLU activation function](https://pytorch.org/docs/stable/generated/torch.nn.ReLU.html).

```python test-set=1 order=null skip-test
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


# Create an instance of our neural network
model = IrisANN()
```

Now comes the fun bit: training our network. We'll calculate loss using [cross entropy](https://pytorch.org/docs/stable/generated/torch.nn.CrossEntropyLoss.html), and we'll optimize the network with the [Adam algorithm](https://pytorch.org/docs/stable/generated/torch.optim.Adam.html#torch.optim.Adam).

```python test-set=1 order=null skip-test
# Set our loss computation and optimization algorithms
criterion = nn.CrossEntropyLoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

# Train the network over 100 epochs
epochs = 100
loss_arr = []

for i in range(epochs):
    # Run data through the network
    Y_hat = model.forward(X_train)
    loss = criterion(Y_hat, Y_train)
    loss_arr.append(loss)

    if i % 10 == 0:
        print(f"Epoch: {i} Loss: {loss}")

    # Optimize the network
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()
```

Lastly, we'll check how well our model worked.

```python test-set=1 order=null skip-test
# Save the predictions to a list
preds = []

with torch.no_grad():
    for val in X_test:
        Y_hat = model.forward(val)
        preds.append(Y_hat.argmax().item())

# Calculate and display how well the network did
df = pd.DataFrame({"Y": Y_test, "YHat": preds})
df["Correct"] = [1 if corr == pred else 0 for corr, pred in zip(df["Y"], df["YHat"])]
accuracy = df["Correct"].sum() / len(df) * 100
print("Neural Network accuracy: " + str(accuracy) + "%")
```

This performance is pretty good for such a simple model.

This example follows a basic formula for using a neural network to solve a supervised learning problem:

- Import, quantize, update, and store data from an external source.
- Define the machine learning model.
- Set the loss calculation and optimization routines.
- Train the model over a set number of training epochs.
- Calculate the model's accuracy.

In this case, a simple neural network is suitable for a simple dataset.

## Classify the Iris dataset with Deephaven tables

So we just classified the Iris dataset using a simple feed-forward neural network. That's kind of cool, but it would be cooler to classify a live feed of incoming data. Let's do that with Deephaven!

First, we extend our previous example to train our model on data in a Deephaven table. This requires some additional code.

First, we import everything we need.

```python test-set=1 order=null skip-test
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
import numpy as np, random, threading, time
```

Just as we did before, we next import our data. This time, we'll import it into a Deephaven table.

```python test-set=1 order=iris_raw,iris skip-test
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
```

This time, we'll create a function to train the neural network.

```python test-set=1 order=null skip-test
# A function that trains the model
def train_model(X_train, Y_train):
    global model
    # Set training parameters
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    epochs = 100

    loss_arr = []

    for i in range(epochs):
        Y_hat = model.forward(X_train)
        loss = criterion(Y_hat, Y_train.long())
        loss_arr.append(loss)

        if i % 10 == 0:
            print(f"Epoch: {i} Loss: {loss}")

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


# A function that gets the model's predictions from input data
def predict_with_model(features):
    if features.dim() == 1:
        features = torch.unsqueeze(features, 0)
    preds = []

    with torch.no_grad():
        for val in features:
            Y_hat = model.forward(val)
            preds.append(Y_hat.argmax().item())

    # Return the model's predictions
    return preds
```

Now we need some extra functions. The first two gather data from a Deephaven table into torch tensors of doubles and integers, respectively, while the third extracts a value from the predictions. The extracted values will be used to create a new column in the output table.

```python test-set=1 order=null skip-test
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

Now, we can predict the Iris subspecies from these measurements. This time, we'll do it in a Deephaven table using the [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function. The first time we call [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn), we train the model. Then, we use the trained model to predict the values of the Iris subspecies in the `Class` column.

```python test-set=1 order=null skip-test
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

We've done the same thing as before: we created static predictions on static data. Only this time, we used a Deephaven table. That's not that exciting. We really want to perform the prediction stage on a _live_ data feed. To demonstrate this, we'll create some fake Iris measurements.

We can [create an in-memory real-time table](./table-publisher.md#dynamictablewriter) using [DynamicTableWriter](../reference/table-operations/create/DynamicTableWriter.md). To create semi-realistic measurements, we'll use some known quantities from the Iris dataset:

| Column          | Minimum (CM) | Maximum (CM) |
| --------------- | ------------ | ------------ |
| `PetalLengthCM` | 1.0          | 6.9          |
| `PetalWidthCM`  | 0.1          | 2.5          |
| `SepalLengthCM` | 4.3          | 7.9          |
| `SepalWidthCM`  | 2.0          | 4.4          |

These quantities will be fed to our table writer and faux measurements will be written to the table once per second for 30 seconds. We'll apply our model on those measurements as they arrive and predict which Iris subspecies they belong to.

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

Now we use [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) on the ticking table. All it takes to change from static to live data is to change the table!

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

All of the code to create and use our trained model on the live, ticking table is below.

```python test-set=1 ticking-table order=live_iris,iris_predicted_live skip-test
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

<LoopedVideo src='../assets/how-to/irisPredicted_live_PyTorch.mp4' />

## Related documentation

- [How to install and use Python packages](./install-and-use-python-packages.md)
- [How to use deephaven.learn](./use-deephaven-learn.md)
- [How to use SciKit-Learn](./use-scikit-learn.md)
- [How to use TensorFlow](./use-tensorflow.md)
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
