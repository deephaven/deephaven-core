# Deephaven with TensorFlow

TensorFlow is an open source machine learning library for Python.  It boasts an intuitive API and specializes in the training and inference of deep learning models.
\
\
In this notebook, we'll use TensorFlow to classify the Iris flower dataset.  This dataset contains 150 measurements of petal length, petal width, sepal length, and sepal width of three different Iris flower subspecies: Iris-setosa, Iris-virginica, and Iris-versicolor.  The Iris flower dataset is commonly used in introductory AI/ML applications.  It is a classification problem: determine the class of Iris subspecies based off the four measurements given.
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

# TensorFlow imports
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout

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
iris_test = iris_test_raw.update("Class = (int)get_class_number(Class)")
```
\
\
With our data quantized and split into training and testing sets, we can get to work doing the classification.  Let's start by defining and creating a neural network to do it.

```python
model = Sequential()
model.add(Dense(16, input_shape = (4,), activation = tf.nn.relu))
model.add(Dense(12, input_shape = (16,), activation = tf.nn.relu))
model.add(Dense(3, input_shape = (12,), activation = tf.nn.softmax))
```
\
\
With our model created, it's time to construct a function that will train our network on the Iris training dataset.

```python
def train_model(x_train, y_train):
    model.compile(optimizer = tf.keras.optimizers.Adam(learning_rate = 0.01), loss = tf.keras.losses.SparseCategoricalCrossentropy(from_logits = True), metrics = ["accuracy"])
    model.fit(x = x_train, y = y_train, epochs = 100)
```
\
\
When our model is trained, we will need a function to use the trained network on the testing dataset.  Let's make a function to do that.

```python
def use_trained_model(x_test):
    if x_test.ndim == 1:
        x_test = np.expand_dims(x_test, 0)
    predictions = model.predict(x_test)
    return [np.argmax(item) for item in predictions]
```
\
\
There's one last step we need to take before we do the machine learning.  We need to define how our model and the two functions will interact with data in our tables.  There will be two functions that gather data from a table, and one that scatters data back into an output table.

```python
# A function to gather double values from a table into a torch tensor
def table_to_numpy_double(rows, columns):
    return np.squeeze(gather.table_to_numpy_2d(rows, columns, dtype = np.double))

# A function to gather integer values from a table into a torch tensor
def table_to_numpy_integer(rows, columns):
    return np.squeeze(gather.table_to_numpy_2d(rows, columns, dtype = int))

# A function to scatter integer model predictions back into a table
def numpy_to_table_integer(predictions, index):
    return int(predictions[index])
```
\
\
With that done, it's time to put everything together.  Let's start by training the neural network on our training table.

```python
learn.learn(
    table = iris_train,
    model_func = train_model,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_numpy_double), 
              learn.Input(["Class"], table_to_numpy_integer)],
    outputs = None,
    batch_size = iris_train.intSize()
)
```
\
\
Lastly, it's time to use our trained model on the testing table.

```python
iris_test_predictions = learn.learn(
    table = iris_test,
    model_func = use_trained_model,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_numpy_double)],
    outputs = [learn.Output("PredictedClass", numpy_to_table_integer, "int")],
    batch_size = iris_test.intSize()
```
\
\
The model works like a charm!  We classified the Iris flower dataset using Deephaven tables and TensorFlow.  That's kind of cool, but it's cooler to do classifications on a live feed of incoming observations.  We'll now show how to do that.  As you'll see, it turns out it's incredibly easy to do so with Deephaven!  We first need to set up a table that we can write faux Iris measurements in real-time to.  We can use the minimum and maximum of each measurement to make sure the faux measurements are realistic.

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
iris_classifications_live = learn.learn(
    table = live_iris,
    model = use_trained_model,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_numpy_double)],
    outputs = [learn.Output("PredictedClass", numpy_to_table_integer, "int")],
    batch_size = 5
)
```
\
\
And that's all there is to it.  The only extra work we needed to do in order to use our model on real-time data is set up the real-time data itself.  This might be a simple problem, but this holds true for more complex problems.
