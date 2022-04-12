# Deephaven with PyTorch

PyTorch is an open source machine learning library for Python.  It is a popular and widely used module that is based on the Torch framework for Lua.  PyTorch is used in some well-known applications, the most notable of which is Tesla's Autopilot advanced driver-assistance system.
\
\
In this notebook, we will use PyTorch to classify the Iris flower dataset.  This dataset contains 150 measurements of the petal length, petal width, sepal length, and sepal width of three different Iris flower subspecies: Iris-setosa, Iris-virginica, and Iris-versicolor.  The Iris flower dataset is commonly used in introductory AI/ML applications.  It is a classification problem: determine the class of Iris subspecies based off the four measurements given.
\
\
Let's start by importing everything we need.  We split up the imports into three categories: Deephaven imports, PyTorch imports, and standard Python imports.

```python
from deephaven.pandas import to_pandas, to_table
from deephaven import DynamicTableWriter
from deephaven import dtypes as dht
from deephaven.learn import gather
from deephaven.csv import read
from deephaven import learn

import torch.nn.functional as F
import torch.nn as nn
import torch

import random, threading, time, pandas as pd, numpy as np
```
\
\
We will now import our data into Deephaven as a Pandas DataFrame and as a table.  The Iris dataset is available in many different places.  We'll grab it from a CSV file contained in the Examples repository.

```python
iris_raw = read("/data/examples/Iris/csv/iris.csv")
```
\
\
With the data in memory, we need to convert the `Class` column to numeric values so that we can use them to train a neural network.

```python
classes = {}
num_classes = 0
def get_class_number(c):
    global classes, num_classes
    if c not in classes:
        classes[c] = num_classes
        num_classes += 1
    return classes[c]

iris = iris_raw.update(formulas=["Class = (int)get_class_number(Class)"])
```
\
\
With the `Class` column quantized, we can now split this data into training and testing sets.  We'll do this with Pandas, where we'll split the data 80%/20% for train/test sets respectively.

```
df_iris = to_pandas(table = iris)

df_iris_train = df_iris.sample(frac = 0.8)
df_iris_test = df_iris.drop(df_iris_train.index)

iris_train = to_table(df = df_iris_train)
iris_test = to_table(df = df_iris_test)
```
\
\
With our data quantized and split into training/testing sets, we can get to work doing the classification.  Let's start by defining and constructing a neural network to do it.

```python
class IrisANN(nn.Module):
    def __init__(self):
        super().__init__()
        self.input_layer = nn.Linear(in_features = 4, out_features = 16)
        self.hidden_layer = nn.Linear(in_features = 16, out_features = 12)
        self.output_layer = nn.Linear(in_features = 12, out_features = 3)

    def forward(self, x):
        x = x.float()
        x = F.relu(self.input_layer(x))
        x = F.relu(self.hidden_layer(x))
        return self.output_layer(x)

model = IrisANN()
```
\
\
With our model created, it's time to construct a function that will train our network on the Iris training dataset.

```python
def train_model(x_train, y_train):
    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr = 0.01)
    epochs = 100

    loss_arr = []

    for epoch in range(epochs):
        y_hat = model.forward(x_train)
        loss = criterion(y_hat, y_train.long())
        loss_arr.append(loss)

        if epoch % 10 == 0:
            print(f"Epoch: {epoch} loss: {loss}")
        
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
```
\
\
Before we can train the model on the training table, we have to define some helper functions.  In this case, we need two functions to gather table data into Torch tensors: one for double values, and a second for integers.

```python
def table_to_tensor_double(rows, cols):
    return torch.from_numpy(gather.table_to_numpy_2d(rows, cols, np_type = np.double))

def table_to_tensor_int(rows, cols):
    return torch.from_numpy(np.squeeze(gather.table_to_numpy_2d(rows, cols, np_type = np.intc)))
```
\
\
It's time to train the model.  We do this with the learn function, which takes five inputs.  It takes a table, model function, list of inputs, list of outputs, and a batch size.  To make things easy, we'll set two variable: `x_cols` and `y_cols`, which contain the features and targets, respectively.

```python
x_cols = ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"]
y_cols = ["Class"]

learn.learn(
    table = iris_train,
    model_func = train_model,
    inputs = [learn.Input(x_cols, table_to_tensor_double), learn.Input(y_cols, table_to_tensor_int)],
    outputs = None,
    batch_size = iris_train.size
)
```
\
\
We now have a fully trained neural network in memory.  We need a way to use this trained model to make predictions, so let's define a function to do just that.

```python
def predict_with_model(features):
    if features.dim() == 1:
        features = torch.unsqueeze(features, 0)
    preds = []

    with torch.no_grad():
        for val in features:
            Y_hat = model.forward(val)
            preds.append(Y_hat.argmax().item())

    return preds
```
\
\
We lastly need to define a function to transfer the predictions from our model from Torch back into Deephaven tables.  This function is called a scatter function, and is simple.  We'll call it `get_predicted_class`.

```python
def get_predicted_class(data, idx):
    return data[idx]
```
\
\
Let's see how well the trained model performs on the training data.

```python
iris_training_results = learn.learn(
    table = iris_train,
    model_func = predict_with_model,
    inputs = [learn.Input(x_cols, table_to_tensor_double)],
    outputs = [learn.Output("Prediction", get_predicted_class, "int")],
    batch_size = iris_train.size
)
```
\
\
We can do the same thing on the testing table.

```python
iris_testing_results = learn.learn(
    table = iris_test,
    model_func = predict_with_model,
    inputs = [learn.Input(x_cols, table_to_tensor_double)],
    outputs = [learn.Output("Prediction", get_predicted_class, "int")],
    batch_size = iris_test.size
)
```
\
\
Our model worked great!  So, we classified a static table of Iris flowers.  That's kind of cool, but let's take this to the next level by doing the classification in real-time.  We'll be able to use the work we've already done to make the live classifications.  We do, however, have to set up a live stream of fake Iris measurements.  Let's get the minimum and maximum values of each observation so that we can keep these faux measurements realistic.

```python
table_writer = DynamicTableWriter(
    {"SepalLengthCM": dht.double, "SepalWidthCM": dht.double, "PetalLengthCM": dht.double, "PetalWidthCM": dht.double}
)

live_iris = table_writer.table

def write_to_iris():
    for i in range(60):
        petal_length = random.randint(10, 69) / 10
        petal_width = random.randint(1, 25) / 10
        sepal_length = random.randint(43, 79) / 10
        sepal_width = random.randint(20, 44) / 10

        table_writer.write_row(sepal_length, sepal_width, petal_length, petal_width)
        time.sleep(1)

thread = threading.Thread(target = write_to_iris)
thread.start()
```
\
\
Now we've got some faux live incoming measurements.  We can just use the model we've already trained with the functions we've already created to do live classification!

```python
iris_predictions_live = learn.learn(
    table = live_iris,
    model_func = predict_with_model,
    inputs = [learn.Input(x_cols, table_to_tensor_double)],
    outputs = [learn.Output("Prediction", get_predicted_class, "int")],
    batch_size = 10
)
```
\
\
And there we have it.  Our model is working on live data.  The only extra work we needed to make that happen was to set up the real-time data stream.  Pretty simple!  This may be a simple problem, but the approach to solving it holds true to more complex ones.