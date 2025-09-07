---
title: Use TensorBoard with Deephaven
sidebar_label: TensorBoard
---

This guide will show you how to use [TensorBoard](https://www.tensorflow.org/tensorboard) with either [TensorFlow](https://www.tensorflow.org/) or [PyTorch](https://pytorch.org/) in Deephaven queries.

[TensorBoard](https://www.tensorflow.org/tensorboard) is one of the most useful tools when training deep neural networks. Many consider the neural network to be a black box, but TensorBoard can shed some light on how to improve your model. TensorBoard can track your experiment metrics like loss and accuracy, visualize the model’s architecture, check model weights and biases, profile your model to see its performance, help tune hyperparameters, project embeddings to a lower dimensional space, and much more. If the training time of your neural network is a couple of hours/days, you can watch updating statistics in a TensorBoard dashboard in real time, making it a natural pairing with Deephaven.

Don't sweat: using TensorBoard with Deephaven's powerful table engine is easy and requires just a couple of lines of code!

## Using TensorBoard with TensorFlow

In our example, we will be using one of the pre-built Deephaven Docker images: [Python with TensorFlow](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/TensorFlow/docker-compose.yml). If you need detailed instructions on how to launch Deephaven from pre-built images, please see [this guide](../getting-started/docker-install.md#choose-a-deployment).

### Classification example

We will classify the Iris dataset with Deephaven tables and use the same Keras neural network we built in [our TensorFlow tutorial](./use-tensorflow.md#classify-the-iris-dataset-with-deephaven-tables). We will only need to add a few lines of code to enable TensorBoard!

First, we have to set a log directory. This is where TensorBoard will store the logs. It will read these logs to show various visualizations:

```python skip-test
log_dir = "tensorboard_logs"
os.system("mkdir '{}'".format(logdir))
```

Second, we need to run TensorBoard and provide the log directory and port number:

```python skip-test
os.system("tensorboard --logdir='{}' --port 6006 --bind_all &".format(log_dir))
```

After that, your TensorBoard dashboard will be available via the browser using the following URL:
[http://localhost:6006](http://localhost:6006)

The TensorBoard dashboard is not active yet. To have some meaningful information in it, we need to specify the TensorBoard callback during the model’s fit method. The TensorBoard callback is just an object that will allow us to write to Tensorboard logs after every epoch:

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

The final step is to pass the callback to the function that trains the model:

```python skip-test
def train_model(X_train, Y_train):
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"],
    )
    model.fit(x=X_train, y=Y_train, epochs=20, callbacks=[tensorboard_callback])
```

So the complete script looks like this:

```python skip-test
import os

# Set up the name of the log directory and run TensorBoard using port 6006
log_dir = "tensorboard_logs"
os.system("mkdir '{}'".format(logdir))
os.system("tensorboard --logdir='{}' --port 6006 --bind_all &".format(log_dir))


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
from tensorflow.keras.callbacks import TensorBoard

# Additional required imports
import numpy as np
import random, threading, time

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

# Our neural network
model = Sequential()
model.add(Dense(16, input_shape=(4,), activation=tf.nn.relu))
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


# A function that trains the model with tensorboard_callback passed as a parameter
def train_model(X_train, Y_train):
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"],
    )
    model.fit(x=X_train, y=Y_train, epochs=20, callbacks=[tensorboard_callback])


# A function that gets the model's predictions on input data
def predict_with_model(features):
    if features.ndim == 1:
        features = np.expand_dims(features, 0)
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

After training the model, you can open the Tensorboard dashboard with [http://localhost:6006](http://localhost:6006) and explore the gathered data.
The Scalars tab shows changes in the loss and metrics over the epochs. It can be used to track other scalar values, such as learning rate and training speed:

![A TensorBoard Scalars tab showing graphs that track epoch accuracy and loss](../assets/how-to/tensorboard-scalars-tf.png)

The Graphs tab shows your model’s layers. You can use this to check if the model's architecture looks as intended.

![The TensorBoard Graphs tab](../assets/how-to/tensorboard-graphs-tf.png)

You can also use TensorBoard to check the distribution of the weights and biases over each epoch, visualize any vector representation (e.g., word embeddings and images), track the performance of every TensorFlow operation that has been executed, monitor hyperparameters and other cool things!

## Using TensorBoard with PyTorch

In this example, we will be using one of the pre-built Deephaven Docker images: [Python with PyTorch](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/PyTorch/docker-compose.yml). If you need detailed instructions on how to launch Deephaven from pre-built images, please see [this guide](../getting-started/docker-install.md#choose-a-deployment).

### Classification example

We will classify the Iris dataset with Deephaven tables and use the same Keras neural network we built in our [how-to use PyTorch guide](./use-pytorch.md#classify-the-iris-dataset-with-deephaven-tables). We will only need to add a few lines of code to enable TensorBoard!

First, we need to create a `SummaryWriter` instance to log data for consumption and visualization by TensorBoard:

```python skip-test
from torch.utils.tensorboard import SummaryWriter

writer = SummaryWriter()
```

Writer will output to ./runs/ directory by default.

Second, we need to run TensorBoard and provide the log directory and port number:

```python skip-test
os.system("tensorboard --logdir='runs' --port 6006 --bind_all &".format(log_dir))
```

After that, your TensorBoard dashboard will be available via the browser using the following URL:
[http://localhost:6006](http://localhost:6006)

The TensorBoard dashboard is not active yet. To have some meaningful information in it, let's use our `SummaryWriter` to write the model evaluation features that we want, e.g. loss:

```python skip-test
writer.add_scalar("Loss/train", loss, epoch)
```

The complete script looks like this:

```python skip-test
import os

# Set up the name of the log directory and run TensorBoard using port 6006
log_dir = "tensorboard_logs"
os.system("tensorboard --logdir='{}' --port 6006 --bind_all &".format(log_dir))

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
from torch.utils.tensorboard import SummaryWriter

# Python imports
import numpy as np, random, threading, time


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
def train_model(X_train, Y_train):
    global model
    # Set training parameters
    criterion = nn.CrossEntropyLoss()

    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)
    epochs = 50

    loss_arr = []

    for i in range(epochs):
        Y_hat = model.forward(X_train)
        loss = criterion(Y_hat, Y_train.long())
        writer.add_scalar("Loss/train", loss, i)
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

After training the model, you can open the Tensorboard dashboard with [http://localhost:6006](http://localhost:6006) and see the results of your neutral networks training runs:

![The TensorBoard dashboard, opened at `http://localhost:6006`](../assets/how-to/tensorboard-scalars-pytorch.png)

You can also use TensorBoard to check the distribution of the weights and biases over each epoch, visualize any vector representation (e.g., word embeddings and images), track the performance of every TensorFlow operation that has been executed, monitor hyperparameters, and more.

## Related documentation

- [How to install and use Python packages](./install-and-use-python-packages.md)
- [How to use deephaven.learn](./use-deephaven-learn.md)
- [How to use PyTorch](./use-pytorch.md)
- [How to use TensorFlow](./use-tensorflow.md)
- [How to use SciKit-Learn](./use-scikit-learn.md)
- [Python variables in query strings](./python-variables.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [How to write data to an in-memory, real-time table](./table-publisher.md)
