---
title: Use deephaven.learn for AI/ML applications
---

This guide shows how to perform calculations in Python with Deephaven tables through the [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) package. It walks through each argument of the [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function with a toy example, puts them together, runs the result on a [ticking table](../conceptual/table-update-model.md), and then applies the same steps to [scikit-learn](./use-scikit-learn.md) and [TensorFlow](./use-tensorflow.md) models.

Data scientists and AI engineers spend a lot of their time transforming and cleaning data, and relatively little time doing the fun analytical work. Deephaven queries make that data preparation easier. The [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) package then runs your model on the prepared data. It hands the data to your model in batches and writes the results back into a table.

Your model can use any Python objects or libraries, so working Python code that doesn't use Deephaven tables extends to Deephaven by wrapping it in a model function. Libraries that run on specialized hardware, such as GPUs, TPUs, or FPGAs, work without changes to the library code. The same model produces static predictions on a static table and real-time predictions on a [ticking table](../conceptual/table-update-model.md).

## The gather-compute-scatter paradigm

Deephaven's [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) package follows a gather-compute-scatter paradigm with three building blocks:

- Gather data from specified columns and rows of a Deephaven table into the appropriate Python object.
- Compute solutions using the gathered data.
- Scatter the solutions back into new columns of a Deephaven table.

Data science libraries typically interact with input and output data stored in Python objects. Common examples of these objects include:

- Lists
- [NumPy](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html) `ndarray`s
- [pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html)
- [PyTorch tensors](https://pytorch.org/docs/stable/tensors.html)
- [TensorFlow tensors](https://www.tensorflow.org/guide/tensor)

Artificial intelligence and machine learning (AI/ML) applications typically gather data into arrays or tensors. Calculations then use that data either to optimize the model or to generate predictions from it. The application scatters predictions and other output values into columns of the output table.

## Usage and syntax

A [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) function call looks like this:

```python syntax
from deephaven import learn

result = learn.learn(table, model_func, inputs, outputs, batch_size)
```

The sections below describe each of the five arguments, in the order you usually decide on them when constructing a solution to a data science or AI problem:

- [Compute](#compute): define the model that calculates solutions from the input data (`model_func`).
- [Input table](#input-table): choose the table that holds the data (`table`).
- [Batch size](#batch-size): choose how many rows to process at once (`batch_size`).
- [Gather](#gather): determine how input data is best represented for your chosen model (`inputs`).
- [Scatter](#scatter): determine what the output of the model looks like (`outputs`).

## Compute

Whatever model you choose, you implement it as a Python function. In this step, you apply calculations to some input data.

[`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) calls the model function once per batch of rows:

- The function receives one argument for each input you define in the [Gather](#gather) step, in the same order.
- [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) passes the function's return value to each scatter function in the [Scatter](#scatter) step.

You don't need to decide every detail and parameter of the model yet. You do need to know:

- The number of input values the model needs to work.
- The overarching class of model to implement.
- The computational constraints on the model.
- The number of output values the model generates.

You typically train an AI model, validate it on held-out data, and test it on new data in separate steps. Each step can be its own [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) call with its own model function, as the [examples](#examples) below show for training and prediction.

For a toy example, this guide constructs a "model" that sums values together. Unlike a real AI model, this needs no training or testing. Regardless, this toy example illustrates basic usage of [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn).

The model receives the gathered input as its `features` parameter. `features` is a two-dimensional NumPy array with one row for each table row in the batch and one column for each input column. The [Gather](#gather) section shows the gather function that builds it. The toy model sums the values in each row, so it passes `axis=1` to [`np.sum`](https://numpy.org/doc/stable/reference/generated/numpy.sum.html).

```python test-set=1
import numpy as np


def summation_model(features):
    return np.sum(features, axis=1)
```

## Input table

Real-world AI applications use datasets drawn from many sources. Sensors, observations, and experimental results cover only a small portion of the data sources that AI models use. In a real application, a Deephaven table holds this data.

You call [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) the same way on a static table and on a [ticking table](../conceptual/table-update-model.md), one whose rows change as new data arrives. On a [ticking table](../conceptual/table-update-model.md), [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) batches and processes only the rows added during each [update cycle](../conceptual/table-update-model.md), the periodic step in which Deephaven applies new data to [ticking tables](../conceptual/table-update-model.md). The model therefore computes predictions only on new input data, which minimizes the computational power that streaming predictions need.

Because [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) identifies rows by their [row keys](../reference/community-questions/shifts.md#key-terms), it raises an error when you call it on a [ticking table](../conceptual/table-update-model.md), unless the table is one of these types:

- [Add-only](../conceptual/table-types.md#specialization-2-add-only), which includes [append-only](../conceptual/table-types.md#specialization-1-append-only) tables.
- [Blink](../conceptual/table-types.md#specialization-3-blink).

For other [ticking tables](../conceptual/table-update-model.md), such as ones that modify or remove rows, use one of the approaches in the [AI/ML workflows guide](./ml-ai-no-learn.md) instead.

For the toy problem, create a static table called `source` using [`empty_table`](../reference/table-operations/create/emptyTable.md) and fill it with data using [`update`](../reference/table-operations/select/update.md). `source` serves as the input table.

```python test-set=1 order=source
from deephaven import empty_table

source = empty_table(100).update(formulas=["A = sqrt(i)", "B = -sqrt(sqrt(i))"])
```

Later on, you pass `source` to [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) as the table that contains the model's input data.

## Batch size

The batch size is the **maximum** number of rows that each call to the model function receives. A batch can hold fewer rows than `batch_size`. For instance, `source` contains 100 rows. With a batch size of 40, [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) processes three batches: the first 40 rows, the next 40, and the last 20.

`batch_size` is required. [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) raises an error if you don't set it.

For a static table, [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) splits all of the table's rows into batches. For a [ticking table](../conceptual/table-update-model.md), it batches only the rows added in each [update cycle](../conceptual/table-update-model.md), as [Input table](#input-table) describes.

`batch_size` is separate from any batch size your ML library uses internally. For example, Keras `model.fit` still splits the rows it receives into its own mini-batches.

Smaller batches use less memory per call. Larger batches make fewer calls and let vectorized libraries work on more rows at once. Choose the batch size based on the largest memory footprint your problem can handle.

For training, many fitting functions, such as [scikit-learn](./use-scikit-learn.md)'s `LinearRegression.fit`, replace the previous fit each time they are called. Pass all training rows in one batch, as the [examples](#examples) do by setting `batch_size` to the table's size.

In the toy problem, the batch size is the number of rows in `source`, so all 100 rows are processed in one batch.

## Gather

In the gather step of [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn), a gather function maps data from a Deephaven table to a Python object that suits the compute model. You specify the columns in the input table to gather data from, and [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) supplies the rows in each batch.

The [`learn.gather`](/core/pydoc/code/deephaven.learn.gather.html#module-deephaven.learn.gather) module gathers data. Its built-in function [`table_to_numpy_2d`](/core/pydoc/code/deephaven.learn.gather.html#deephaven.learn.gather.table_to_numpy_2d) gathers table data into a two-dimensional NumPy [`ndarray`](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html).

[`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) calls the gather function once per batch with two arguments: the set of rows in the batch and the column sources for the gathered columns. A column source is the object that holds a column's data. A gather function usually passes both straight to [`table_to_numpy_2d`](/core/pydoc/code/deephaven.learn.gather.html#deephaven.learn.gather.table_to_numpy_2d).

[`table_to_numpy_2d`](/core/pydoc/code/deephaven.learn.gather.html#deephaven.learn.gather.table_to_numpy_2d) takes four parameters:

- `row_set`: The rows to copy into the `ndarray`.
- `col_set`: The column sources to copy into the `ndarray`.
- `order`: A [`gather.MemoryLayout`](/core/pydoc/code/deephaven.learn.gather.html#deephaven.learn.gather.MemoryLayout) value, either `ROW_MAJOR` (the default) or `COLUMN_MAJOR`.
  - `C` and `FORTRAN` are aliases for `ROW_MAJOR` and `COLUMN_MAJOR`, respectively.
  - The returned array has one row per table row and one column per gathered column for either value, so most models can leave the default.
  - `order` does not set the memory layout you would expect. `ROW_MAJOR` returns a column-major array, and `COLUMN_MAJOR` returns a row-major one. If your library needs a specific layout, convert the result with [`np.ascontiguousarray`](https://numpy.org/doc/stable/reference/generated/numpy.ascontiguousarray.html) for row-major or [`np.asfortranarray`](https://numpy.org/doc/stable/reference/generated/numpy.asfortranarray.html) for column-major.
- `np_type`: The NumPy `dtype` of all values in the output `ndarray`.
  - Supported types are `np.byte`, `np.short`, `np.intc`, `np.int_`, `np.single`, and `np.double`.
  - The Python types `int` and `float` convert to `np.intc` and `np.double`, respectively.
  - The default is `np.intc`, so pass `np_type` explicitly for floating-point data.

The gathered columns must already hold the Java type that matches `np_type`, such as `double` columns for `np.double` or `int` columns for `np.intc`. Cast any other column with [`update`](../reference/table-operations/select/update.md) before calling [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn), as the [scikit-learn example](#predict-insurance-charges-using-scikit-learn-and-deephaven) does.

For the toy problem, the model needs all of the data from input columns `A` and `B` in the `source` table. Thus, the gather step takes data from two columns and maps it to an `ndarray`. The resulting `ndarray` has one row for each row in the batch and one column for each gathered column. In the toy problem, the batch holds all 100 rows of `source`, so the array has 100 rows and 2 columns.

The toy gather function uses the default `order` and gathers the values as doubles:

```python skip-test
from deephaven.learn import gather
import numpy as np


def table_to_numpy(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, np_type=np.double)
```

An input pairs the column names to gather with the gather function. [`learn.Input`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Input) takes a column name or a list of column names, followed by the gather function. Pass the inputs to [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) as a list:

```python skip-test
from deephaven import learn

inputs = [learn.Input(["A", "B"], table_to_numpy)]
```

> [!NOTE]
> If you only want to gather one column, [`gather.table_to_numpy_2d`](/core/pydoc/code/deephaven.learn.gather.html#deephaven.learn.gather.table_to_numpy_2d) creates a two-dimensional array with a single column. You can convert it to a one-dimensional array with [`np.squeeze`](https://numpy.org/doc/stable/reference/generated/numpy.squeeze.html).

## Scatter

The scatter step of [`learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn) runs after the model processes the input data. It writes the model's output back into a Deephaven table. You specify the output columns and a scatter function that maps the output data to table cells.

The toy example has two input columns and a function that sums them on a per-row basis. Its output is a single column, `C`, that holds each sum.

The scatter function receives the model's result and the index of a row within its batch. The toy scatter function returns the sum at that index, and [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) writes that value to the output table.

```python skip-test
def numpy_to_table(data, idx):
    return data[idx]
```

An output pairs a new column name with the scatter function and the column's type. [`learn.Output`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.Output) takes the output column name, the scatter function, and the type. Pass the outputs to [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) as a list:

```python skip-test
outputs = [learn.Output("C", numpy_to_table, "double")]
```

Specifying `"double"` at the end tells [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) to cast the output values as doubles in the result table. The type is a string that names a Java type, such as one of the [Java primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) (`"int"`, `"long"`, `"double"`, and so on).

If the model function produces no output, such as one that only trains a model, pass `outputs=None`, as the training steps in the [examples](#examples) do.

## Putting it all together

You can combine everything covered so far to make a program that uses [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) to sum two columns in one table and return the result in another.

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

This is overkill for adding two columns. One [`update`](../reference/table-operations/select/update.md) call does the same thing: `source.update(formulas=["C = A + B"])`. However, these steps generalize to AI code, where the added complexity is small relative to the problem itself.

## Predict on a ticking table

The same [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) call works on live data. The example below reuses the functions from the previous block. Its input comes from [`time_table`](../reference/table-operations/create/timeTable.md), which creates a [time table](./time-table.md) that adds a row every second. Because a time table is [append-only](../conceptual/table-types.md#specialization-1-append-only), [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) accepts it.

```python test-set=2 ticking-table order=result_live
from deephaven import time_table

source_live = time_table("PT1s").update(formulas=["A = sqrt(i)", "B = -sqrt(sqrt(i))"])

result_live = learn.learn(
    table=source_live,
    model_func=summation_model,
    inputs=[learn.Input(["A", "B"], table_to_numpy)],
    outputs=[learn.Output("C", numpy_to_table, "double")],
    batch_size=n_rows,
)
```

On each [update cycle](../conceptual/table-update-model.md), [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) gathers only the rows added since the previous cycle, runs the model on them, and fills in their `C` values. Rows that are already in `result_live` keep their values.

## Examples

This section uses these AI packages:

- [scikit-learn](https://scikit-learn.org/stable/)
- [TensorFlow](https://www.tensorflow.org/)

These packages are not in the base `server` Docker image. To get them, either [run an image that includes them](../getting-started/docker-install.md#choose-a-deployment), such as `server-sklearn`, `server-tensorflow`, or `server-all-ai`, or install them as described in [Install and use Python packages](./install-and-use-python-packages.md).

The [scikit-learn](./use-scikit-learn.md) and [TensorFlow](./use-tensorflow.md) guides cover each package both with and without [`deephaven.learn`](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn).

### Predict insurance charges using scikit-learn and Deephaven

This first example uses a linear regression model from [scikit-learn](https://scikit-learn.org/stable/). This model predicts each customer's insurance charges from demographic and health factors such as age, BMI, and smoking status.

The code uses the [insurance dataset](https://github.com/deephaven/examples/tree/main/Insurance) from [Deephaven's Examples repository](https://github.com/deephaven/examples), which was originally published on [Kaggle](https://www.kaggle.com/datasets/brandonyongys/insurance-charges).

The example below:

- Reads the insurance dataset from a CSV file with [`read_csv`](../reference/data-import-export/CSV/readCsv.md).
- Converts the string columns to numeric category codes with [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md), [`update`](../reference/table-operations/select/update.md), and [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md).
- Casts the integer columns to doubles.
- Constructs a linear regression model and defines model functions that fit it and use it to predict.
- Defines the gather and scatter functions.
- Uses [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) to fit the model to the data, with no outputs.
- Uses [`learn`](/core/pydoc/code/deephaven.learn.html#deephaven.learn.learn) again to apply the fitted model and write the predictions to a new column.

```python docker-config=scikit_learn order=predicted_skl
from deephaven import learn, read_csv
from deephaven.learn import gather

from sklearn import linear_model
import numpy as np

# Read the CSV into a Deephaven table
insurance = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/Insurance/csv/insurance.csv"
)


# Function to replace each value of a categorical column with its category's index, as a double
def encode_categorical(table, columns):
    for column in columns:
        table = (
            table.group_by(column).update(formulas=[column + " = (double)i"]).ungroup()
        )
    return table


# Encode the string columns and cast the integer columns to doubles, so we can use them in our ML model
insurance_numeric = encode_categorical(insurance, ["region", "sex", "smoker"]).update(
    formulas=["age = (double)age", "children = (double)children"]
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
    outputs=[learn.Output("PredictedCharges", scatter, "double")],
    batch_size=insurance_numeric.size,
)
```

Each row of `predicted_skl` keeps its actual `expenses` next to the model's prediction in `PredictedCharges`.

### Predict fraudulent credit card charges using TensorFlow and Deephaven

This second example uses [TensorFlow](https://www.tensorflow.org/) to predict whether credit card purchases are fraudulent.

The code uses the [credit card fraud dataset](https://github.com/deephaven/examples/tree/main/CreditCardFraud) from [Deephaven's Examples repository](https://github.com/deephaven/examples), which was originally published on [Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud).

The dataset contains 48 hours' worth of credit card purchases. Out of 284,807 purchases, 492 are fraudulent. Principal component analysis (PCA) anonymizes 28 of the numeric columns, `V1` through `V28`, which the model uses as its inputs. The low rate of fraudulent purchases (0.173%) makes this an outlier detection problem.

The code below:

- Reads the CSV file into memory with [`read_csv`](../reference/data-import-export/CSV/readCsv.md).
- Casts the `Class` column to `short` so it can be gathered with `np.short`.
- Splits the table into training and testing sets with [`where`](../reference/table-operations/filter/where.md).
- Constructs the model and defines model functions that train it and use it to predict.
- Defines the gather and scatter functions.
- Trains the model on the training data, with no outputs.
- Applies the trained model to the training data.
- Applies the trained model to the testing data.
- Keeps the columns of interest with [`view`](../reference/table-operations/select/view.md).

```python skip-test
# Deephaven imports
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

# Python imports
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Input
import tensorflow as tf
import numpy as np

# Read the CSV into Deephaven
credit_card = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CreditCardFraud/csv/creditcard.csv"
).update(formulas=["Class = (short)Class"])

# Break the data into training and testing sets
training_data = credit_card.where(filters=["Time > 57599 || Time < 50400"])
testing_data = credit_card.where(filters=["Time > 50399 && Time < 57600"])

# Create our neural network
model = Sequential()
model.add(Input(shape=(28,)))
model.add(Dense(36, activation=tf.nn.relu))
model.add(Dense(14, activation=tf.nn.relu))
model.add(Dense(2, activation=tf.nn.softmax))


# A function to train the model
def train_model(features, targets):
    model.compile(
        optimizer=tf.keras.optimizers.Adam(learning_rate=0.001),
        loss=tf.keras.losses.SparseCategoricalCrossentropy(from_logits=False),
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

![The testing_predictions table, with predictions in the PredictedClass column](../assets/how-to/ccfraud_testingPredictions.png)

`PredictedClass` holds the model's prediction (0 for a valid purchase, 1 for fraud) next to the true `Class` for each testing purchase.

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Incremental update model](../conceptual/table-update-model.md)
- [AI/ML workflows](./ml-ai-no-learn.md)
- [Install and run with Docker](../getting-started/docker-install.md)
- [Install and use Python packages](./install-and-use-python-packages.md)
- [Use PyTorch in Deephaven](./use-pytorch.md)
- [Use scikit-learn in Deephaven](./use-scikit-learn.md)
- [Use TensorFlow in Deephaven](./use-tensorflow.md)
- [Create a time table](./time-table.md)
- [How do row keys and positional indices behave during table operations?](../reference/community-questions/shifts.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`group_by`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`read_csv`](../reference/data-import-export/CSV/readCsv.md)
- [`time_table`](../reference/table-operations/create/timeTable.md)
- [`ungroup`](../reference/table-operations/group-and-aggregate/ungroup.md)
- [`update`](../reference/table-operations/select/update.md)
- [`view`](../reference/table-operations/select/view.md)
- [`where`](../reference/table-operations/filter/where.md)
- [`deephaven.learn` Pydoc](/core/pydoc/code/deephaven.learn.html#module-deephaven.learn)
- [`deephaven.learn.gather` Pydoc](/core/pydoc/code/deephaven.learn.gather.html#module-deephaven.learn.gather)
