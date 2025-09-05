---
title: Use deephaven.learn for AI/ML applications
sidebar_label: deephaven.learn
---

This guide will show you how to perform calculations in Python with Deephaven tables through the [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) submodule.

[`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) allows you to perform gather-scatter operations on Deephaven tables. During the gather operation, data from specified columns and rows of a table is gathered into input objects. Then, calculations are performed on the input objects. Finally, the results of calculations are scattered into new columns in an output table.

The [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) methods are agnostic to the Python object types or libraries used to perform calculations. Thus, functioning Python code that does not use Deephaven tables can easily be extended to work with [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn). Additionally, because [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) is agnostic to the libraries used to perform calculations, libraries using specialized hardware, such as GPUs, TPUs, or FPGAs, will work without additional modification.

For Artificial Intelligence / Machine Learning (AI/ML) applications, data is gathered into tensors. Calculations then either (1) optimize the AI/ML model using the input tensors, or (2) generate predictions from the model using the input tensors. Predicted and other output values are then scattered into columns of the output table.

The [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) gather-scatter operations work on both static and dynamic real-time tables. For static tables, the rows of the table are separated into batches. These batches are processed in sequence. For real-time tables, only the rows of the table that changed during the most recent update cycle are separated into batches and processed. Because of this, AI/ML predictions are only computed on input data that has changed, minimizing how much computational power is needed to predict streaming data.

<!-- TODO: https://github.com/deephaven/deephaven.io/issues/785 Link to deephaven.learn REF doc -->

## The gather-compute-scatter paradigm

Deephaven's [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) package has been built with a gather-compute-scatter paradigm in mind. These are the building blocks:

- Gather data from Deephaven tables into the appropriate Python object.
- Compute solutions using the gathered data.
- Scatter the solutions back into a Deephaven table.

The [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) package coordinates how user-defined models operate on data in Deephaven tables.

## Usage and syntax

Data science libraries typically interact with input and output data stored in Python objects. These objects include (but are not limited to):

- Lists
- [NumPy](https://numpy.org/doc/2.1/reference/generated/numpy.ndarray.html) ndarrays
- [pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html)
- Torch Tensors
- TensorFlow Tensors

Data scientists and AI engineers spend a lot of their time transforming and cleaning data. Relatively little time is spent doing the fun analytical work. Deephaven queries make these data manipulation tasks easier, while Deephaven's [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function allows user-defined models to generate static or real-time predictions without needing to change code.

A [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function call looks like this:

```python skip-test
from deephaven import learn

result = learn.learn(table, model_func, inputs, outputs, batch_size)
```

Each of the five function inputs is described below. The ordering of sections follows the usual process for construting a solution to a data science/AI problem:

- Define the model that will calculate solutions from the input data (compute).
- Determine how input data is best represented for your chosen model (gather).
- Determine what the output of the model will look like (scatter).

## Compute

An AI scientist should have a basic understanding of the problem at hand when constructing a solution. There are many factors to consider when choosing a model that will be used. Some basic questions that must be answered are:

- Do I need a supervised or unsupervised model?
- What are the pros and cons of certain models for the problem at hand?
- What kind of computing constraints do I have?

Regardless of what model is chosen for a problem, it must be implemented in a function in Deephaven.

### Implement a model

In this step, we apply calculations to some input data. Those calculations can train, test, or validate a chosen model. The model will be implemented in a function.

Every detail and parameter of a chosen model does not need to be determined at this point. However, the following should be known:

- The number of input values the model needs to work.
- The overarching class of model to implement.
- The computational constraints on the model.
- How many output values the model will generate.

In a typical AI application, a model will be trained, validated, and tested in separate steps. The training will continually update the model until a goal function is maximized or minimized. It likely produces no output. The validation determines whether or not the trained model is sufficiently accurate when used on the training data. Lastly, the testing phase is where the trained model is tested against a new data set. If a trained model shows satisfactory results during testing, it will be deployed and used on real-world data.

For a toy example, we will construct a "model" that sums values together. Unlike a real AI model, this needs no training or testing. Regardless, this toy example is used to illustrate basic usage of [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn).

Values will be summed on a per-row basis. The input values are contained in a variable called `features`.

```python test-set=1
def summation_model(features):
    return np.sum(features, axis=1)
```

## Gather

The gather step of [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) requires the user to map data from a Deephaven table to a Python object in a way that makes sense in terms of the compute model.

### Input table

Real-world applications of AI use data sets that contain information from one or more of a vast array of sources. Sensors, observations, and experimental results cover only a small portion of the data sources that AI models use. In a real application, this data will be contained in a Deephaven table. The static or real-time nature of the data will not affect how [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) is used.

For our toy problem, we'll create a static table called `source` using [`empty_table`](../reference/table-operations/create/emptyTable.md) and fill it with data using [`update`](../reference/table-operations/select/update.md). `source` serves as our input table.

```python test-set=1 order=source
from deephaven import empty_table

source = empty_table(100).update(formulas=["A = sqrt(i)", "B = -sqrt(sqrt(i))"])
```

Later on, we'll tell [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) that `source` contains the data used as input for our model.

### Batch size

Next, we should determine a batch size. The batch size defines the **maximum** number of samples (rows) propagated through a model during one iteration. The effect of the batch size on the performance of an AI model tends to follow these general rules:

- Smaller batch sizes use less memory.
- Models tend to train faster with smaller batch sizes.
- Larger batch sizes correspond to more accurate gradient estimates during training.
- Larger batch sizes can allow more data to be processed in parallel, resulting in faster calculations.

Be sure to carefully consider these trade-offs if the input table is sufficiently large.

The batch size doesn't mean a model will _always_ use `batch_size` number of data points. For instance, `source` contains 100 rows. Specifying a batch size of 40 would mean that three batches get processed: points 1 - 40, points 41 - 80, and points 81-100. Thus, the batch size should be chosen based upon the largest memory footprint that can be handled for a given problem.

In our toy problem, we'll use all rows in `source` as the batch size.

### Inputs and the gather function

Gathering input values into a Python object requires specifying the columns and number of rows in the input table from which the data will be gathered. This data will be mapped from a Deephaven table to a Python object via a gather function.

For our toy problem, we want all of the data from input columns `A` and `B` in the `source` table. Thus, our input takes data from two columns, and maps it to a NumPy `ndarray`. The resulting `ndarray` will have the same number of rows and columns as the input table.

Deephaven has a submodule for gathering data called `learn.gather`. This submodule has a built-in function for gathering table data into a two-dimensional `ndarray` called `table_to_numpy_2d`.

The function `table_to_numpy_2d` can take up to four inputs:

- `rows`: A set of rows that will be copied in to the `ndarray`.
- `columns`: A set of columns that will be copied into the `ndarray`.
- `order`: How the array is stored in memory. Can be either column-major or row-major.
  - The default value is row-major.
  - Specifying the memory layout is done with an enumeration called `gather.MemoryLayout`. There are four accepted values:
    - `gather.MemoryLayout.ROW_MAJOR`: row-major order.
    - `gather.MemoryLayout.COLUMN_MAJOR`: column-major order.
    - `gather.MemoryLayout.C`: C memory layout (row-major).
    - `gather.MemoryLayout.FORTRAN`: Fortran memory layout (column-major).
- `np_type`: The data type of all values in the output `ndarray`. Any non-NumPy data types will be cast to the corresponding NumPy `dtype`.

In this example, we'll use row-major order. If we were to use column-major order, the resulting NumPy `ndarray` would still look the same.

```python skip-test
from deephaven.learn import gather


def table_to_numpy(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, np_type=float)
```

These are put together in the [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function call like this:

```python skip-test
inputs = [learn.Input(["A", "B"], table_to_numpy)]
```

> [!NOTE]
> If you only want to gather one column, `gather.table_to_numpy_2d` will create a 2-D array with a single column. You can convert this to a 1-D array with [`np.squeeze`](https://numpy.org/doc/stable/reference/generated/numpy.squeeze.html).

## Scatter

The scatter step of [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) occurs after the model has been applied to the input data. The output of the model will be scattered back into a Deephaven table.

### Outputs and the scatter function

Scattering computed values back into Deephaven tables requires specifying output columns and defining the way in which the output data is mapped to output table cells via a scatter function.

In our toy example, we have two input columns, and a function that sums the values on a per-row basis. Thus, our output is one single column (called `C`) containing the result of each summation.

Our scatter function simply takes the summation result at a particular index and returns it so that it can be written to the output table.

```python skip-test
def numpy_to_table(data, idx):
    return data[idx]
```

We put these together in the [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function call like this:

```python skip-test
outputs = [learn.Output("C", numpy_to_table, "double")]
```

Specifying `double` at the end tells [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) to cast the output values as doubles in the result table. All of the [Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) are supported, and the type is specified as a string input.

## Putting it all together

We can combine everything we just covered to make a program that uses [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) to sum two columns in one table and return the result in another.

```python test-set=2 order=source,result
# Imports
from deephaven import empty_table
from deephaven.learn import gather
from deephaven import learn

import numpy as np

# Define the number of rows in our source table
n_rows = 100

# Create a table to be used as input
source = empty_table(n_rows).update(formulas=["A = sqrt(i)", "B = -sqrt(sqrt(i))"])


# This function applies a summation "model" to the input data
def summation_model(features):
    return np.sum(features, axis=1)


# This function gathers data from a table into a NumPy ndarray
def table_to_numpy(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, np_type=np.double)


# This function scatters data back into a Deephaven table
def numpy_to_table(data, idx):
    return data[idx]


# The learn function call
result = learn.learn(
    table=source,
    model_func=summation_model,
    inputs=[learn.Input(["A", "B"], table_to_numpy)],
    outputs=[learn.Output("C", numpy_to_table, "double")],
    batch_size=n_rows,
)
```

This is overkill for simply adding two columns. This can easily be done with some simple table operations. However, these steps generalize to AI code, where the amount of added complexity is small relative to the problem itself.

## Examples

In this section, we integrate the following popular AI packages:

- [SciKit-Learn](https://scikit-learn.org/stable/)
- [TensorFlow](https://www.tensorflow.org/)

These modules are not a part of the base Deephaven Docker image. Refer to our guide [How to install Python packages](./install-and-use-python-packages.md) for an explanation of how to install these modules.

Examples exist in our documentation that cover specific Python modules. Those, on top of what's presented below, serve as a good reference for how to use the Python modules themselves, as well as [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn). Here is a list of how-to guides for specific packages that cover their use both with and without [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn):

- [How to use PyTorch in Deephaven](./use-pytorch.md)
- [How to use SciKit-Learn in Deephaven](./use-scikit-learn.md)
- [How to use TensorFlow in Deephaven](./use-tensorflow.md)

### Predict insurance charges using SciKit-Learn and Deephaven

This first example uses a linear regression model from [SciKit-Learn](https://scikit-learn.org/stable/). This model will predict insurance charges for customers depending on a few different health factors.

The code uses this [insurance dataset](https://www.kaggle.com/datasets/teertha/ushealthinsurancedataset), which can be found in [Deephaven's Examples repository](https://github.com/deephaven/examples). If you are using a Deephaven deployment with example data, it is mounted at `/data/examples/Insurance/csv` within the Deephaven Docker container. For more information on this location, see our guide [Access your file system with Docker data volumes](../conceptual/docker-data-volumes.md).

The example below:

- Reads and quantizes the insurance dataset from a CSV file.
- Constructs a linear regression model.
- Fits the model to the data.
- Defines the scatter and gather functions.
- Uses [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) to apply the fitted model to the data.

```python skip-test
from deephaven import learn, read_csv
from deephaven.learn import gather

from sklearn import linear_model
import numpy as np

# Read CSV into a deephaven table and quantize data
insurance = read_csv("/data/examples/Insurance/csv/insurance.csv")


# Function to convert all non-double and non-numeric columns to doubles
def convert_to_numeric_double(table, columns):
    for column in columns:
        table = (
            table.group_by(column).update(formulas=[column + " = (double)i"]).ungroup()
        )
    return table


# Convert non-numeric and non-double columns to double values so we can use them in our ML model
insurance_numeric = convert_to_numeric_double(
    insurance, ["age", "children", "region", "sex", "smoker"]
)

# Create a linear regression model
linreg = linear_model.LinearRegression()


# A function to fit our linear model
def fit_linear_model(features, target):
    linreg.fit(features, target)


# A function to use the fitted model
def use_fitted_model(features):
    return linreg.predict(features)


# Our gather function
def table_to_numpy(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.double)


# Our scatter function
def scatter(data, idx):
    return np.round(data[idx][0], 2)


# Train the linear regression model
learn.learn(
    table=insurance_numeric,
    model_func=fit_linear_model,
    inputs=[
        learn.Input(
            ["age", "bmi", "children", "region", "sex", "smoker"], table_to_numpy
        ),
        learn.Input("expenses", table_to_numpy),
    ],
    outputs=None,
    batch_size=insurance_numeric.size,
)

# Use the fitted model to make predictions
predicted_skl = learn.learn(
    table=insurance_numeric,
    model_func=use_fitted_model,
    inputs=[
        learn.Input(
            ["age", "bmi", "children", "region", "sex", "smoker"], table_to_numpy
        )
    ],
    outputs=[learn.Output("predicted_charges", scatter, "double")],
    batch_size=insurance_numeric.size,
)
```

![The above `predicted_skl` table](../assets/how-to/insurance_predictedSKL.png)

### Predict fraudulent credit card charges using a neural network

Our second example uses [TensorFlow](https://tensorflow.org) to predict whether or not credit card purchases are fraudulent based on 28 anonymized purchase metrics.

The code uses this [credit card fraud CSV file](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud). It contains 48 hours' worth of credit card purchases by insurance cardholders. Out of 284,807 purchases, 492 are fraudulent. The numerical data describing each purchase is broken into 28 different columns, which are anonymized and PCA transformed. The low rate of fraudulent purchases (0.173%) makes this an outlier detection problem.

The code below:

- Reads the CSV file into memory.
- Splits the tables into training and testing sets.
- Constructs the model.
- Trains the model on the training data.
- Tests the trained model on the testing data.

```python skip-test
# Deephaven imports
from deephaven import DynamicTableWriter
from deephaven import dtypes as dht
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

# Python imports
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense
import tensorflow as tf
import numpy as np
import threading
import time

# Read the CSV into Deephaven
credit_card = read_csv("/data/examples/CreditCardFraud/csv/creditcard.csv")

# Break the data into training and testing sets
training_data = credit_card.where(filters=["Time > 57599 || Time < 50400"])
testing_data = credit_card.where(filters=["Time > 50399 && Time < 57600"])

# Create our neural network
model = Sequential()
model.add(Dense(36, input_shape=(28,), activation=tf.nn.relu))
model.add(Dense(14, input_shape=(36,), activation=tf.nn.relu))
model.add(Dense(2, input_shape=(14,), activation=tf.nn.softmax))


# A function to train the model
def train_model(features, targets):
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True),
        metrics=["accuracy"],
    )
    model.fit(x=features, y=targets, epochs=5)


# Make predictions with the trained model
def predict_with_model(features):
    predictions = model.predict(features)
    return [np.argmax(item) for item in predictions]


# A function to gather table data into a NumPy ndarray of doubles
def table_to_numpy_double(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.double)


# A function to gather table data into a NumPy ndarray of shorts
def table_to_numpy_short(rows, cols):
    return gather.table_to_numpy_2d(rows, cols, np_type=np.short)


# A function to scatter data back into an output table
def scatter(data, idx):
    return int(data[idx])


# These are our input column names (["V1" -> "V28"])
inps = ["V" + str(item) for item in range(1, 29)]

# Train the model
learn.learn(
    table=training_data,
    model_func=train_model,
    inputs=[
        learn.Input(inps, table_to_numpy_double),
        learn.Input("Class", table_to_numpy_short),
    ],
    outputs=None,
    batch_size=training_data.size,
)

# Apply the trained model to training data
training_predictions = learn.learn(
    table=training_data,
    model_func=predict_with_model,
    inputs=[learn.Input(inps, table_to_numpy_double)],
    outputs=[learn.Output("PredictedClass", scatter, "int")],
    batch_size=training_data.size,
)

# Apply the trained model to the testing data
testing_predictions = learn.learn(
    table=testing_data,
    model_func=predict_with_model,
    inputs=[learn.Input(inps, table_to_numpy_double)],
    outputs=[learn.Output("PredictedClass", scatter, "int")],
    batch_size=testing_data.size,
)

testing_predictions = testing_predictions.view(
    formulas=["Time", "Amount", "Class", "PredictedClass"]
)
```

![The above `testingPredictions` table](../assets/how-to/ccfraud_testingPredictions.png)

## Related documentation

- [How to install and use Python packages in Deephaven](./install-and-use-python-packages.md)
- [How to use PyTorch](./use-pytorch.md)
- [How to use SciKit-Learn](./use-scikit-learn.md)
- [How to use TensorFlow](./use-tensorflow.md)
- [How to write data to a real-time, in-memory table](./table-publisher.md)
- [`drop_columns`](../reference/table-operations/select/drop-columns.md)
- [DynamicTableWriter](../reference/table-operations/create/DynamicTableWriter.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`rename_columns`](../reference/table-operations/select/rename-columns.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
- [`update`](../reference/table-operations/select/update.md)
