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
With our data quantized and split into training and testing sets, we can get to work doing the classification.  Let's start by constructing functions to fit and use a fitted K-Neighbors classifier on the data.

```python
neigh = 0

# Construct and classify the Iris dataset
def fit_knn(X_train, Y_train):
    global neigh
    neigh = knn(n_neighbors = 3)
    neigh.fit(X_train, Y_train)

# Use the K-Nearest neighbors classifier on the Iris dataset
def use_knn(features):
    if features.ndim == 1:
        features = np.expand_dims(features, 0)

    predictions = np.zeros(len(features))
    for i in range(0, len(features)):
        predictions[i] = neigh.predict([features[i]])

    return predictions
```
\
\
There's one more step we need to take before we use these functions.  We have to define how our K-Neighbors classifier will interact with data in Deephaven tables.  There will be two functions that gather data from a table, and one that scatters data back into an output table.

```python
# A function to gather data from columns into a NumPy array of doubles
def table_to_array_double(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, dtype = np.double)

# A function to gather data from columns into a NumPy array of integers
def table_to_array_int(rows, cols):
    return np.squeeze(gather.table_to_numpy_2d(rows, cols, dtype = np.intc))

# A function to extract a list element and cast to an integer
def get_predicted_class(data, idx):
    return int(data[idx])
```
\
\
With that done, it's time to put everything together.  Let's start by fitting our classifier using our training table.

```python
learn.learn(
    table = iris_train,
    model_func = fit_knn,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_array_double), learn.Input("Class", table_to_array_int)],
    outputs = None,
    batch_size = 150
)
```
\
\
We've got a fitted classifier now.  Let's test it out on our test table.

```python
iris_knn_classified = learn.learn(
    table = iris_test,
    model_func = use_knn,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_array_double)],
    outputs = [learn.Output("ClassifiedClass", get_predicted_class, "int")],
    batch_size = 150
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
# Create the table writer
table_writer = DynamicTableWriter(
    ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
    [dht.double, dht.double, dht.double, dht.double]
)

# Get the live, ticking table
live_iris = table_writer.getTable()

# This function creates faux Iris measurements once per second for a minute
def write_to_iris():
    for i in range(60):
        petal_length = random.randint(10, 69) / 10
        petal_width = random.randint(1, 25) / 10
        sepal_length = random.randint(43, 79) / 10
        sepal_width = random.randint(20, 44) / 10

        table_writer.logRow(sepal_length, sepal_width, petal_length, petal_width)
        time.sleep(1)

# Use a thread to write data to the table
thread = threading.Thread(target = write_to_iris)
thread.start()
```
\
\
Now we've got some faux live incoming measurements.  We can just use our fitted K-Neighbors classifier on the live table!

```python
iris_classified_live = learn.learn(
    table = live_iris,
    model_func = use_knn,
    inputs = [learn.Input(["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"], table_to_array_double)],
    outputs = [learn.Output("LikelyClass", get_predicted_class, "int")],
    batch_size = 150
)
```
\
\
And there we have it.  Our classifier is working on live data.  The only extra work we needed to make that happen was to set up the real-time data stream.  Pretty simple!  This may be a toy problem, but the steps hold true to more complex ones.
