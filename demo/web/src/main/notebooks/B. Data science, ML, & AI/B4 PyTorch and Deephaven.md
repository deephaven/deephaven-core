# Deephaven with PyTorch

PyTorch is an open source machine learning library for Python.  It is a popular and widely used module that is based on the Torch framework for Lua.  PyTorch is used in some well-known applications, the most notable of which is Tesla's Autopilot advanced driver-assistance system.
\
\
In this notebook, we will use PyTorch to classify the Iris flower dataset.  This dataset contains 150 measurements of the petal length, petal width, sepal length, and sepal width of three different Iris flower subspecies: Iris-setosa, Iris-virginica, and Iris-versicolor.  The Iris flower dataset is commonly used in introductory AI/ML applications.  It is a classification problem: determine the class of Iris subspecies based off the four measurements given.
\
\
Let's start by importing everything we need.  We split up the imports into three categories: Deephaven imports, PyTorch imports, and additional required imports.

```python
# Deephaven imports
from deephaven import dataFrameToTable, tableToDataFrame
from deephaven import DynamicTableWriter
from deephaven import learn, read_csv
from deephaven.learn import gather
import deephaven.Types as dht

# PyTorch imports
import torch.nn.functional as F
import torch.nn as nn
import torch

# Additional required imports
import random, threading, time
import pandas as pd
import numpy as np
```
\
\
We will now import our data into Deephaven as a Pandas DataFrame and as a table.  The Iris dataset is available in many different places.  We'll grab it from a CSV file at a URL in our Examples repository.

```python
iris_raw = read_csv("/data/examples/Iris/csv/iris.csv")
raw_iris = tableToDataFrame(iris_raw)
```
\
\
We now have the data in memory.  We need to split it into training and testing sets.  We'll use 120 random rows as our training set, and the other 30 as the testing set.

```python
raw_iris_train = raw_iris.sample(frac = 0.8)
raw_iris_test = raw_iris.drop(raw_iris_train.index)

iris_train_raw = dataFrameToTable(raw_iris_train)
iris_test_raw = dataFrameToTable(raw_iris_test)
```
\
\
In order to classify Iris subspecies, they need to be quantized.  Let's quantize the `Class` column in our train and test tables using a simple function.

```python
classes = {}
num_classes = 0
def get_class_number(c):
    global classes, num_classes
    if c not in classes:
        classes[c] = num_classes
        num_classes += 1
    return classes[c]

iris_train = iris_train_raw.update("Class = (int)get_class_number(Class)")
iris_test = iris_train_raw.update("Class = (int)get_class_number(Class)")
```
\
\
With our data quantized and split into training and testing sets, we can get to work doing the classification.  Let's start by defining and creating a neural network to do it.

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
        
# Create the IrisANN model
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
            print(f"Epoch: {epoch} Loss: {loss}")

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
```
\
\
When our model is trained, we then need to use the trained network on the test dataset.  Let's construct a function to do just that.

```python
def predict_with_model(features):
    if features.dim() == 1:
        features = torch.unsqueeze(features, 0)
    preds = []

    with torch.no_grad():
        for val in features:
            y_hat = model.forward(val)
            preds.append(y_hat.argmax().item())

    # Return the model's predictions
    return preds
```
\
\
There's one last step we need to take before we do the machine learning.  We need to define how our model and the two functions will interact with data in our tables.  There will be two functions that gather data from a table, and one that scatters data back into an output table.

```python
# A function to gather double values from a table into a torch tensor
def table_to_tensor_double(rows, columns):
    return torch.from_numpy(np.squeeze(gather.table_to_numpy_2d(rows, columns, dtype = np.double)))

# A function to gather integer values from a table into a torch tensor
def table_to_tensor_integer(rows, cols):
    return torch.from_numpy(np.squeeze(gather.table_to_numpy_2d(rows, cols, dtype = np.intc)))
    
# A function to scatter integer model predictions back into a table
def tensor_to_table_integer(predictions, index):
    return int(predictions[index])
```
\
\
With that done, it's time to put everything together.  Let's start by training the neural network on our training table.

```python
learn.learn(
    table = iris_train,
    model_func = train_model,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_tensor_double), 
              learn.Input(["Class"], table_to_tensor_integer)],
    outputs = None,
    batch_size = iris_train.intSize()
)
```
\
\
Lastly, it's time to test how well our model works on the testing table.

```python
iris_test_predictions = learn.learn(
    table = iris_test,
    model_func = predict_with_model,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_tensor_double)],
    outputs = [learn.Output("PredictedClass", tensor_to_table_integer, "int")],
    batch_size = iris_test.intSize()
)
```
\
\
Our model worked great!  So, we classified a static table of Iris flowers.  That's kind of cool, but let's take this to the next level by doing the classification in real-time.  We'll be able to use the work we've already done to make the live classifications.  We do, however, have to set up a live stream of fake Iris measurements.  Let's get the minimum and maximum values of each observation so that we can keep these faux measurements realistic.

```python
min_petal_length, max_petal_length = raw_iris["PetalLengthCM"].min(), raw_iris["PetalLengthCM"].max()
min_petal_width, max_petal_width = raw_iris["PetalWidthCM"].min(), raw_iris["PetalWidthCM"].max()
min_sepal_length, max_sepal_length = raw_iris["SepalLengthCM"].min(), raw_iris["SepalLengthCM"].max()
min_sepal_width, max_sepal_width = raw_iris["SepalWidthCM"].min(), raw_iris["SepalWidthCM"].max()
```
\
\
With these quantities now calculated and stored in memory, we need to set up a live table that we can write faux measurements to.  To keep it simple, we'll write measurements to the table once per second for a minute.

```python
table_writer = DynamicTableWriter(
    ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
    [dht.double] * 4
)

live_iris = table_writer.getTable()

def write_faux_iris_measurements():
    for i in range(60):
        sepal_length = np.round(random.uniform(min_sepal_length, max_sepal_length), 1)
        sepal_width = np.round(random.uniform(min_sepal_width, max_sepal_width), 1)
        petal_length = np.round(random.uniform(min_petal_length, max_petal_length), 1)
        petal_width = np.round(random.uniform(min_petal_width, max_petal_width), 1)

        table_writer.logRow(sepal_length, sepal_width, petal_length, petal_width)
        time.sleep(1)

thread = threading.Thread(target = write_faux_iris_measurements)
thread.start()
```
\
\
Now we've got some faux live incoming measurements.  We can just use the model we've already trained with the functions we've already created to do live classification!

```python
iris_predictions_live = learn.learn(
    table = live_iris,
    model_func = predict_with_model,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_tensor_double)],
    outputs = [learn.Output("PredictedClass", tensor_to_table_integer, "int")],
    batch_size = 5
)
```
\
\
And there we have it.  Our model is working on live data.  The only extra work we needed to make that happen was to set up the real-time data stream.  Pretty simple!  This may be a toy problem, but the steps hold true to more complex ones.
