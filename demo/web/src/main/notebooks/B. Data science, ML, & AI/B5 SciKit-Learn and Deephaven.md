# Deephaven with SciKit-Learn

SciKit-Learn is an open source machine learning library for Python.  It is a popular and widely used module that features a wide variety of built-in algorithms for classification, regression, clustering, and deep learning.
\
\
In this notebook, we'll use SciKit-Learn to create a K-Nearest neighbors classifier and classify the Iris flower dataset.  This dataset contains 150 measurements of the petal length, petal width, sepal length, and sepal width of three different Iris flower subspecies: Iris-setosa, Iris-virginica, and Iris-versicolor.  The Iris flower dataset is commonly used in introductory AI/ML applications.  It is a classification problem: determine the class of Iris subspecies based off the four measurements given.
\
\
Let's start by importing everything we need.  We split up the imports into three categories: Deephaven imports, SciKit-Learn imports, and additional required imports.

```python
# Deephaven imports
from deephaven import dataFrameToTable, tableToDataFrame
from deephaven import DynamicTableWriter
from deephaven import learn, read_csv
from deephaven.learn import gather
import deephaven.Types as dht

# SciKit-Learn imports
from sklearn.neighbors import KNeighborsClassifier as knn

# Additional required imports
import random, threading, time
import pandas as pd
import numpy as np
```
\
\
We will now import our data into Deephaven as a Pandas dataframe and as a table.  The Iris dataset is available at the URL in the `read_csv` line of the code blow, along with a variety of other places (even SciKit-Learn has it built in).

```python
iris_raw = read_csv("https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv")
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

iris_train = iris_raw_train.update("Class = (int)get_class_number(Class)")
iris_test = iris_raw_test.update("Class = (int)get_class_number(Class)")
```
\
\
With our data quantized and split into training and testing sets, we can get to work doing the classification.  Let's start by constructing functions to fit and use a fitted K-Neighbors classifier on the data.

```python
neigh = 0

def fit_knn(x_train, y_train):
    global neigh
    neigh = knn(n_neighbors = 3)
    neigh.fit(x_train, y_train)

def use_fitted_knn(x_test):
    if x_test.ndim == 1:
        x_test = x_test.expand_dims(x_test, 0)

    predictions = np.zeros(len(x_test))
    for i in range(len(x_test)):
        predictions[i] = neigh.predict([x_test[i]])

    return predictions
```
\
\
There's one more step we need to take before we use these functions.  We have to define how our K-Neighbors classifier will interact with data in Deephaven tables.  There will be two functions that gather data from a table, and one that scatters data back into an output table.

```python
# A function to gather double values from a table into a NumPy ndarray
def table_to_numpy_double(rows, columns):
    return np.squeeze(gather.table_to_numpy_2d(rows, columns, dtype = np.double))

# A function to gather integer values from a table into a NumPy ndarray
def table_to_numpy_integer(rows, columns):
    return np.squeeze(gather.table_to_numpy_2d(rows, columns, dtype = int))

# A function to scatter integer predictions back into a table
def numpy_to_table_integer(predictions, index):
    return int(data[idx])
```
\
\
With that done, it's time to put everything together.  Let's start by fitting our classifier using our training table.

```python
learn.learn(
    table = iris_train,
    model_func = fit_knn,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_numpy_double), learn.Input("Class", table_to_numpy_integer)],
    outputs = None,
    batch_size = iris_train.intSize()
)
```
\
\
We've got a fitted classifier now.  Let's test it out on our test table.

```python
iris_test_knn = learn.learn(
    table = iris_test,
    model_func = use_fitted_knn,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_numpy_double)],
    outputs = [learn.Output("PredictedClass", numpy_to_table_integer, "int")],
    batch_size = iris_test.size()
)
```
\
\
The K-Nearest neighbors classifier worked great!  We just did some classifications using a simple model on a static data set.  That's kind of cool, but this data set is neither large or real-time.  Let's create a real-time table of faux Iris measurements and apply our fitted classifier.
We need to make sure our faux measurements are realistic, so let's grab the minimum and maximum observation values and use those to generate random numbers.

```python
min_petal_length, max_petal_length = raw_iris["PetalLengthCM"].min(), raw_iris["PetalLengthCM"].max()
min_petal_width, max_petal_width = raw_iris["PetalWidthCM"].min(), raw_iris["PetalWidthCM"].max()
min_sepal_length, max_sepal_length = raw_iris["SepalLengthCM"].min(), raw_iris["SepalLengthCM"].max()
min_sepal_width, max_sepal_width = raw_iris["SepalWidthCM"].min(), raw_iris["SepalWidthCM"].max()
```
\
\
With these quantities now calculated and stored in memory, we need to set up alive table that we can write measurements to.  To keep it simple, we'll write measurements to the table once per second for a minute.

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
Now we've got some faux live incoming measurements.  We can just use our fitted K-Neighbors classifier on the live table!

```python
iris_classifications_live = learn.learn(
    table = live_iris, 
    model_func = use_fitted_knn,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_tensor_double)],
    outputs = [learn.Output("PredictedClass", tensor_to_table_integer, "int")],
    batch_size = 5
)
```
\
\
And there we have it.  Our classifier is working on live data.  The only extra work we needed to make that happen was to set up the real-time data stream.  Pretty simple!  This may be a toy problem, but the steps hold true to more complex ones.