---
title: Use scikit-learn in Deephaven
---

This guide shows how to use [scikit-learn](https://scikit-learn.org/stable/) in Deephaven queries.

[scikit-learn](https://scikit-learn.org/stable/) is an open-source machine learning library for Python. It features a variety of methods for classification, regression, clustering, and dimensionality reduction.

This guide gives two examples. Both classify observations in the [Iris dataset](https://en.wikipedia.org/wiki/Iris_flower_data_set), which is available in [Deephaven's examples repository](https://github.com/deephaven/examples). The first example uses [scikit-learn](https://scikit-learn.org/stable/) with pandas DataFrames only, as a baseline. The second trains the same classifier on a Deephaven table and then applies it to a [live, ticking table](../conceptual/table-types.md#standard-streaming-tables) that receives new rows while the query runs.

The Iris flower dataset is a popular dataset commonly used in introductory machine learning applications. Introduced by R.A. Fisher in his 1936 paper, _The use of multiple measurements in taxonomic problems_, it contains 150 samples, 50 from each of three Iris species. The `Class` column holds the species: `Iris-setosa`, `Iris-versicolor`, or `Iris-virginica`. Each row also holds four measurements, in centimeters:

- `SepalLengthCM`: sepal length
- `SepalWidthCM`: sepal width
- `PetalLengthCM`: petal length
- `PetalWidthCM`: petal width

Predicting a flower's species from these measurements is a classification problem. This guide solves it with the [K-Nearest Neighbors](https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.KNeighborsClassifier.html) classifier. If you already know [scikit-learn](https://scikit-learn.org/stable/), skip to [Classify the Iris dataset with Deephaven tables](#classify-the-iris-dataset-with-deephaven-tables).

## Install scikit-learn

[scikit-learn](https://scikit-learn.org/stable/) is not included in Deephaven's default `server` Docker image. To use it within Deephaven, you can [install it yourself](./install-and-use-python-packages.md) or choose one of the [Deephaven Docker deployments](../getting-started/docker-install.md#choose-a-deployment) that include it:

- [Python with scikit-learn](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/SciKit-Learn/docker-compose.yml), without example data.
- [Python with scikit-learn and example data](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/SciKit-Learn/docker-compose.yml).
- [Python with All AI](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python/All-AI/docker-compose.yml) (NLTK, PyTorch, scikit-learn, and TensorFlow), without example data.
- [Python with All AI and example data](https://raw.githubusercontent.com/deephaven/deephaven-core/main/containers/python-examples/All-AI/docker-compose.yml).

> [!NOTE]
> In this guide, we read data from [Deephaven's examples repository](https://github.com/deephaven/examples). You can also load files from the `/data` directory in the Docker container, which Deephaven's Docker Compose deployments mount from `./data` on your computer. See [Docker data volumes](../conceptual/docker-data-volumes.md) to learn more about the relation between locations in the container and the local file system.

## Classify the Iris dataset with pandas

This first example shows how to use [scikit-learn](https://scikit-learn.org/stable/) with pandas DataFrames to classify Iris flowers from measurements.

Let's first import all the packages we need.

```python docker-config=scikit_learn test-set=1 order=null
from sklearn.neighbors import KNeighborsClassifier as knn
import pandas as pd
```

Next, we import the data, encode the species names in the `Class` column as integers, and split the data into training and testing sets. The encoding maps `Iris-setosa` to 0, `Iris-virginica` to 1, and `Iris-versicolor` to 2.

```python test-set=1 order=null
# Read the dataset and encode the class labels as integers
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

# Separate our data into features and targets (x and y)
x_train = train_set.drop("Class", axis=1).values
y_train = train_set["Class"].values
x_test = test_set.drop("Class", axis=1).values
y_test = test_set["Class"].values
```

Next, we construct a [K-Nearest Neighbors classifier](https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.KNeighborsClassifier.html) with 3 as the number of neighbors. We fit it to the training set.

```python test-set=1 order=null
neigh = knn(n_neighbors=3)
neigh.fit(x_train, y_train)
```

Lastly, we use the classifier to predict the species of each flower in the testing set and count how many predictions are correct.

```python test-set=1 order=:log
test_size = len(x_test)
n_correct = 0
for i in range(test_size):
    prediction = neigh.predict([x_test[i]])[0]
    if prediction == y_test[i]:
        n_correct += 1

accuracy = n_correct / test_size
print(str(accuracy * 100) + "% correct")
```

Around 95% correct for such a simple solution. Pretty neat! The exact figure changes from run to run, because `sample` shuffles the rows randomly before the code splits them.

This example follows a basic formula for using a classifier to solve a classification problem:

- Import data from an external source.
- Encode its class labels as integers.
- Split the data into training and testing sets.
- Define the classification model.
- Fit the classification model to the training data.
- Test the model on the testing data.
- Assess its accuracy.

In this case, a simple classifier is suitable for a simple dataset.

## Classify the Iris dataset with Deephaven tables

So we just classified the Iris dataset with [K-Nearest Neighbors](https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.KNeighborsClassifier.html). That's kind of cool, but wouldn't it be cooler to do it on a live feed of incoming data? Let's do that with Deephaven!

This section trains the same [K-Nearest Neighbors](https://scikit-learn.org/stable/modules/generated/sklearn.neighbors.KNeighborsClassifier.html) classifier as the [pandas example](#classify-the-iris-dataset-with-pandas), this time on a Deephaven table, and then uses it to classify rows of a [live table](../conceptual/table-types.md#standard-streaming-tables) as they arrive. To keep the focus on how Deephaven moves data into and out of the model, this version trains on all 150 rows and skips the testing and accuracy steps.

First, we import the packages we need.

```python test-set=1 order=null
# Deephaven imports
from deephaven import DynamicTableWriter
import deephaven.dtypes as dht
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

# Machine learning imports
from sklearn.neighbors import KNeighborsClassifier as knn

# Additional required imports
import numpy as np
import random
import threading
import time
```

### Load the data into a table

Next, we import the data with [`read_csv`](../reference/data-import-export/CSV/readCsv.md) and encode the `Class` column as integers, as in the first example. Here the data goes into a Deephaven table, and an [`update`](../reference/table-operations/select/update.md) operation calls a [Python function](./python-functions.md) to replace each species name with its integer label. The `-> np.intc` [type hint](./python-functions.md#type-hints) makes the new `Class` column an `int` column.

```python test-set=1 order=null
iris_raw = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Iris/csv/iris.csv"
)
iris_mappings = {"Iris-setosa": 0, "Iris-virginica": 1, "Iris-versicolor": 2}


def get_class_number(c) -> np.intc:
    return iris_mappings[c]


iris = iris_raw.update(formulas=["Class = get_class_number(Class)"])
```

### Train and classify with `learn`

To train and classify inside Deephaven, we use the [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function. [How to use `deephaven.learn`](./use-deephaven-learn.md) covers it in depth. [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) takes these arguments:

- `table`: the table to read from.
- `model_func`: the Python function that receives the gathered inputs for each batch of rows and runs the model on them.
- `inputs`: a list of [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input)s that gather table columns into the model function's arguments.
- `outputs`: a list of [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output)s that write the model function's results to new columns.
- `batch_size`: the maximum number of rows the model function receives at once.

First, we write the two model functions: one fits the classifier, and one uses it.

```python test-set=1 order=null
neigh = None


# Construct the K-Nearest Neighbors classifier and fit it to the training data
def fit_knn(x_train, y_train):
    global neigh
    neigh = knn(n_neighbors=3)
    neigh.fit(x_train, y_train)


# Use the K-Nearest Neighbors classifier on the Iris dataset
def use_knn(features):
    return neigh.predict(features)
```

Next, we write the functions that move data between the table and the model. [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) calls each [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input)'s function once per batch, with the batch's rows (`rows`) and that input's columns (`cols`). It calls each [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output)'s function once per row, with the model function's result (`data`) and the row's position in the batch (`idx`). It writes the returned value to the column that the [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output) names.

- `table_to_array_double` uses [`gather.table_to_numpy_2d`](/core/pydoc/code/deephaven.learn.gather.html#deephaven.learn.gather.table_to_numpy_2d) to gather the measurement columns into a 2D NumPy array of doubles.
- `table_to_array_int` gathers the `Class` column as integers in the same way, then calls `np.squeeze` to flatten the one-column result into the 1D array of labels that [scikit-learn](https://scikit-learn.org/stable/)'s `fit` expects.
- `get_predicted_class` returns one row's prediction as an integer.

```python test-set=1 order=null
# A function to gather data from columns into a NumPy array of doubles
def table_to_array_double(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.double)


# A function to gather data from columns into a NumPy array of integers
def table_to_array_int(rows, cols):
    return np.squeeze(gather.table_to_numpy_2d(rows, cols, np_type=np.intc))


# A function to extract one prediction from the array and cast it to an integer
def get_predicted_class(data, idx):
    return int(data[idx])
```

Now, we can classify the Iris species from these measurements, with two calls to [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn):

- The first call fits the model to the training data.
- The second call uses the fitted model to classify each row and writes the prediction to a new `ClassifiedClass` column.

Two arguments in the training call matter:

- `outputs=None`, because `fit_knn` produces no output. With `outputs=None`, [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) runs `fit_knn` on every batch even though nothing reads a result.
- `batch_size=150`, the number of rows in the Iris dataset, so `fit_knn` sees every row in one batch. `fit_knn` builds a new classifier for each batch it receives, so a smaller batch size would leave a model trained only on the last batch.

```python test-set=1 order=iris_knn_classified
# Use the learn function to fit our KNN classifier
learn.learn(
    table=iris,
    model_func=fit_knn,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_array_double,
        ),
        learn.Input("Class", table_to_array_int),
    ],
    outputs=None,
    batch_size=150,
)

# Use the learn function to create a new table that contains classified values
iris_knn_classified = learn.learn(
    table=iris,
    model_func=use_knn,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_array_double,
        )
    ],
    outputs=[learn.Output("ClassifiedClass", get_predicted_class, "int")],
    batch_size=150,
)
```

The second [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) call classifies the same rows the model was trained on, so the result shows how [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) works rather than how accurate the model is. These are still static classifications on [static data](../conceptual/table-types.md#static-tables). That's not that exciting. We really want to perform the classification stage on a _live_ data feed.

### Classify live data

To demonstrate classification on live data, we create some fake Iris measurements. To keep each measurement in a plausible range, we draw it at random from the range of that column in the Iris dataset:

| Column          | Minimum (cm) | Maximum (cm) |
| --------------- | ------------ | ------------ |
| `SepalLengthCM` | 4.3          | 7.9          |
| `SepalWidthCM`  | 2.0          | 4.4          |
| `PetalLengthCM` | 1.0          | 6.9          |
| `PetalWidthCM`  | 0.1          | 2.5          |

A background thread adds one row of fake measurements, drawn from these ranges, to a [live table](../conceptual/table-types.md#standard-streaming-tables) once per second for one minute. As each row arrives, the model predicts which Iris species it belongs to.

First, we [create an in-memory real-time table](./table-publisher.md#dynamictablewriter) with [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) and write data to it. [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) produces an [append-only table](../conceptual/table-types.md#specialization-1-append-only), so `iris_classified_live` keeps every prediction.

> [!NOTE]
> For most other live-ingestion cases, we recommend [`table_publisher`](../reference/table-operations/create/TablePublisher.md). It produces a [blink table](../conceptual/table-types.md#specialization-3-blink), which holds only the rows added in the current [update cycle](../conceptual/table-update-model.md), so it would not keep every prediction here.

```python ticking-table test-set=1 order=null reset
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


# This function creates fake Iris measurements once per second for a minute
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

All it takes to change from [static](../conceptual/table-types.md#static-tables) to live data is an [add-only](../conceptual/table-types.md#specialization-2-add-only) source table, such as the [append-only table](../conceptual/table-types.md#specialization-1-append-only) that [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md) produces. See [How to use `deephaven.learn`](./use-deephaven-learn.md) for the table types [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) supports.

Now we use [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) on the [live table](../conceptual/table-types.md#standard-streaming-tables). The call matches the static classification above with two changes. Its `table` argument is now `live_iris`, and its output column is named `LikelyClass`. Here, `batch_size=150` caps how many new rows `use_knn` receives at once.

```python ticking-table test-set=1 order=null
# Use the learn function to create a new table with live predictions
iris_classified_live = learn.learn(
    table=live_iris,
    model_func=use_knn,
    inputs=[
        learn.Input(
            ["SepalLengthCM", "SepalWidthCM", "PetalLengthCM", "PetalWidthCM"],
            table_to_array_double,
        )
    ],
    outputs=[learn.Output("LikelyClass", get_predicted_class, "int")],
    batch_size=150,
)
```

As the thread writes each row to `live_iris`, `iris_classified_live` gains a matching row with its predicted species in `LikelyClass`:

<LoopedVideo src='../assets/how-to/iris_classified_live_sklearn.mp4' />

## Related documentation

- [Docker data volumes](../conceptual/docker-data-volumes.md)
- [Table types](../conceptual/table-types.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [How to install and use Python packages](./install-and-use-python-packages.md)
- [How to use `deephaven.learn`](./use-deephaven-learn.md)
- [AI/ML workflows](./ml-ai-no-learn.md)
- [How to use PyTorch](./use-pytorch.md)
- [How to use TensorFlow](./use-tensorflow.md)
- [Python functions in query strings](./python-functions.md)
- [How to write data to an in-memory, real-time table](./table-publisher.md)
- [`DynamicTableWriter`](../reference/table-operations/create/DynamicTableWriter.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`table_publisher`](../reference/table-operations/create/TablePublisher.md)
- [`update`](../reference/table-operations/select/update.md)
- [`deephaven.learn` Pydoc](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn)
- [`deephaven.learn.gather` Pydoc](/core/pydoc/code/deephaven.learn.gather.html#module-deephaven.learn.gather)
