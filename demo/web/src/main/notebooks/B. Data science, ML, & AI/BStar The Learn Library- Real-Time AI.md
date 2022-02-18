# Real-Time AI with Deephaven's Learn Library

This notebook will demonstrate the use of [`deephaven.learn`](https://deephaven.io/core/pydoc/code/deephaven.learn.html?highlight=learn#module-deephaven.learn) to facilitate the data interchange between [Deephaven tables](https://deephaven.io/core/javadoc/io/deephaven/engine/table/package-summary.html) and [NumPy ndarrays](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html).  The [`deephaven.learn`](https://deephaven.io/core/pydoc/code/deephaven.learn.html?highlight=learn#module-deephaven.learn) package comprises of two primary components:

- [`deephaven.learn.learn`](https://deephaven.io/core/pydoc/code/deephaven.learn.html?highlight=learn#deephaven.learn.learn)
  - The learn function, which uses a gather-compute-scatter paradigm to apply calculations to data stored in Python objects taken to and placed back in Deephaven tables.
- [`deephaven.learn.gather`](https://deephaven.io/core/pydoc/code/deephaven.learn.gather.html?highlight=gather)
  - The gather subpackage, which provides utilities for data exchange from Deephaven tables to 2-d NumPy arrays (and by proxy other Python objects).

The goal of this notebook is to show the usage of [`deephaven.learn`](https://deephaven.io/core/pydoc/code/deephaven.learn.html?highlight=learn#module-deephaven.learn), thoroughly document its syntax, and provide an example of use on real-time data.

## Syntax

```python
from deephaven.TableTools import emptyTable
from deephaven.learn import gather
from deephaven import learn

import numpy as np

t = emptyTable(5).update("X = i", "Y = 2 * i")

def sum_columns(data):
    print(type(data))
    print(data.shape)
    print(data.dtype)

    return np.sum(data, axis = 1)

def table_to_numpy_integer(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, dtype = np.intc)

def numpy_to_table(data, index):
    return data[index]

t2 = learn.learn(
    table = t,
    model_func = sum_columns,
    inputs = [learn.Input(["X", "Y"], table_to_numpy_integer)],
    outputs = [learn.Output("Z", numpy_to_table, "int")],
    batch_size = t.intSize()
)
```

The code block above shows usage syntax for a simple example that sums each row in two columns of a table.  Let's break down each step of the code above.

### Imports

The following two imports are always required when using [`deephaven.learn`](https://deephaven.io/core/pydoc/code/deephaven.learn.html?highlight=learn#module-deephaven.learn):

```python
from deephaven.learn import gather
from deephaven import learn
```

The other two are used in the code, but not explicitly required when using the package.  The output of [`deephaven.learn.gather`](https://deephaven.io/core/pydoc/code/deephaven.learn.gather.html?highlight=gather) is by default a [NumPy ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html), so in order to perform any manipulation on that, a [NumPy](https://www.numpy.org/) import is required.  If, however, you wish to immediately convert the output to another type, you don't need to import [NumPy](https://www.numpy.org/).

### Model function

The learn package interacts with data in [Deephaven tables](https://deephaven.io/core/javadoc/io/deephaven/engine/table/package-summary.html).  Thus, that interaction generally takes place inside of a function.  In the case of the code block, the function is `sum_columns`.  The function will take a single input, print its type, shape, and the type of data stored in the array.  It returns the sum of each row in the table in a new one-dimensional array.

### Gather and scatter functions

In this example, the gather and scatter functions are `table_to_numpy_integer` and `numpy_to_table`, respectively.

The gather function will return a two-dimensional [NumPy ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html) that contains the data from a set of columns and rows in the table.  The function itself doesn't decide what columns and rows are used.  That's handled in the learn function call, which will be covered shortly.  It uses [`deephaven.learn.gather`](https://deephaven.io/core/pydoc/code/deephaven.learn.gather.html?highlight=gather), which provides the method for data exchange between a table and a Python object.  The method used here is `table_to_numpy_2d`, which converts table data into a two-dimensional [NumPy ndarray](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html).  The method takes up to four inputs:

- `rows`
  - A set of rows in a table.  The actual set of rows is specified in the learn function call.
- `columns`
  - A set of columns in a table.  The actual set of columns is specified in the learn function call.
- `order`
  - The contiguity of the output array.  There are two possibilities:
    - `MemoryLayout.COLUMN_MAJOR` or `MemoryLayout.FORTRAN`: The return array is stored in column-major order.
    - `MemoryLayout.ROW_MAJOR` or `MemoryLayout.C`: The return array is stored in row-major order.
  - Array contiguity is row major by default.  Array contiguity becomes a significant factor in performance when data becomes sufficiently large.
- `dtype`
  - The [NumPy dtype](https://numpy.org/doc/stable/reference/arrays.dtypes.html) of data in the return array.
    - There exists a 1:1 mapping between supported [NumPy dtypes](https://numpy.org/doc/stable/reference/arrays.dtypes.html) and Java primitives.  Supported types are:
      - Bytes, shorts, integers, longs, floats, and doubles

The scatter function will return a single value from the output of the model function.  In this case, the output is a five-element-long 1D NumPy array.  A scatter function will _always_ take two inputs.  In the case that a model function returns a single value, the scatter function will look like this:

```python
def numpy_to_table(data, index):
    return data
```

### The learn function call

This is where everything comes together.  This function call is where everything that's already been set up gets used.  The learn function takes five inputs:

- `table`
  - The table that contains the data to process.  In the example, the table is `t`.
- `model_func`
  - The function that applies processing to data.  In the example, the function is `sum_columns`.
- `inputs`
  - The set of columns which will be used for data processing.  These must come in the form of one or more `learn.Input` objects.  In the example, the rows are `X` and `Y`.  Additionally, the gather function is specified here.  In the example, the gather function is `table_to_numpy_integer`.
- `outputs`
  - The set of outputs which will be seen as new columns in the output table.  These must come in the form of one or more `learn.Output` objects.  In the example, the output column is `Z`.  Additionally, the scatter function is specified here.  In the example, the scatter function is `numpy_to_table`.  Lastly, you can specify the Java primitive type of the output column.  This output must be a string, and correspond to Java primitive.  In this case, a primitive `int` is chosen as the output type.
- `batch_size`
  - The maximum number of rows in the table that will be gathered in a single iteration.  In the example, the batch size is the number of rows in `t`.

## A static example

This example will take data from the `metriccentury.csv` file in `/data/examples/MetricCentury` and process it.  The data contains various metrics related to a century bike ride (100 km).  The model function will calculate an approximate calories burned as the bike ride progresses, with the assumption that the rider is a 30 year old male that weighs 70 kg.  The formula used to calculate calories is [here](https://tourdevines.com.au/blog/how-many-calories-does-cycling-burn/#:~:text=The%20Actual%20Formula%20to%20Calculate,20.4022%5D%20x%20Time%20%2F%204.184.)

```python
from deephaven.learn import gather
from deephaven import read_csv
from deephaven import learn

import numpy as np

bikeride = read_csv("/data/examples/MetricCentury/csv/metriccentury.csv").update("Num_Seconds = i")

weight_term = 70 * 0.09036
age_term = 30 * 0.2017
heart_rate_coeff = 0.6309
time_coeff = 4.184 * 60
extra_term = 55.0969
cal_count = 0

def calc_calories(data):
    global cal_count
    cals = cal_count + \
        (age_term - weight_term + data[0][1] * heart_rate_coeff - extra_term) / (time_coeff)
    cal_count = cals
    return cals

def table_to_numpy_integer(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, dtype = np.intc)

def numpy_to_table(data, index):
    return data[index]

bikeride_with_calories = learn.learn(
    table = bikeride,
    model_func = calc_calories,
    inputs = [learn.Input(["Num_Seconds", "HeartRate"], table_to_numpy_integer)],
    outputs = [learn.Output("Calories_Burned", numpy_to_table, "double")],
    batch_size = bikeride.intSize()
)
```

## Doing the same thing in real time

I want to see how many calories I'm burning on a bike ride as the bike ride happens.  Theoretically, other metrics could be published as well, such as those described in the metric century dataset [README](https://github.com/deephaven/examples/blob/main/MetricCentury/README.md).  That way, a user could actually track their progress as the bike ride is happening.  Additionally, there are some cool possibilities for comparing rides with one another to see how a user's fitness and ability as a cyclist improve over time.  To simulate the real-time data, we'll replay the `bikeride` table using [`Replayer`](https://deephaven.io/core/pydoc/code/deephaven.TableManipulation.html?highlight=replayer#deephaven.TableManipulation.Replayer).  For more information on replaying historical table data, see [How to replay table data in Deephaven](https://deephaven.io/core/docs/how-to-guides/replay-data/).

We can use the same model, gather, and scatter functions as earlier for the real-time data.

```python
from deephaven.TableManipulation import Replayer
import deephaven.DateTimeUtils as dhtu

# Replay 15 minutes of the bike ride data
start_time = dhtu.convertDateTime("2019-08-25T11:34:55 NY")
end_time = dhtu.convertDateTime("2019-08-25T11:49:57 NY")

bikeride_replayer = Replayer(start_time, end_time)

bikeride_replayed = bikeride_replayer.replay(bikeride, "Time")
bikeride_replayer.start()

# We have to reset our total calories burned count before re-applying our model
cal_count = 0

bikeride_replayed = learn.learn(
    table = bikeride_replayed,
    model_func = calc_calories,
    inputs = [learn.Input(["Num_Seconds", "HeartRate"], table_to_numpy_integer)],
    outputs = [learn.Output("Calories_Burned", numpy_to_table, "double")],
    batch_size = bikeride.intSize()
)
```

The code works on both static and real-time tables!  Pretty slick.

###  Observations

- A `batch_size` of 1 was chosen for the static use case because the time stamps in the `Time` column are each separated by one second.  That means that the model will be applied to a single new row once per second.  If a static-only analysis were done, the `batch_size` would be set to `bikeride.intSize`, and the `calc_calories` function would look a bit different.  Generally speaking, for static data, larger batch sizes translate to faster execution.  However, in this case, the table isn't sufficiently large for minor speed differences to play a large factor in overall performance.
- The `end_time` is set to approximately 15 minutes after the `start_time` for the data replayer because this demo session will last 45 minutes from when it was first started.  Additionally, it's not that exciting to watch that for 45 whole minutes, so the cutoff was made to be earlier than the full length of the bike ride (over four hours).
- The examples used in this guide are very simple, but do translate to real-world applications.  It was mentioned earlier that other metrics of a bike ride could be calculated.  It would be a good exercise for newcomers to try to implement one of those on their own.


## Conclusion

[`deephaven.learn`](https://deephaven.io/core/pydoc/code/deephaven.learn.html?highlight=learn#module-deephaven.learn) makes implementing models on table data approachable and intuitive.  Since it works on [Deephaven tables](https://deephaven.io/core/javadoc/io/deephaven/engine/table/package-summary.html), it has the added benefit of utilizing [Deephaven's table update model](https://deephaven.io/core/docs/conceptual/table-update-model/), which removes concerns about the nature of incoming data, and allows you to focus on the analysis itself.  The examples presented in this notebook are meant to be instructional and demonstrative.  Hopefully you have found them to be informative.  If you've made it this far, thank you for taking the time to try for yourself and best of luck in your Deephaven endeavors!