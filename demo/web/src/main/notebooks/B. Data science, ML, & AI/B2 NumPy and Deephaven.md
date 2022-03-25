# Deephaven, the learn module, and NumPy

This notebook will show you how to integrate Deephaven's learn module and NumPy into your Python queries.  The learn module pairs well with NumPy because of their utilities for processing data.

NumPy is a numerical computing library for Python that is widely used by data scientists, analysts, AI engineers, and casual developers alike.  It offers a fast, versatile, and comprehensive suite of data structures, mathematical functions, operators, and generators that is backed by C.  Its intuitive syntax makes all of these functionalities accessible to all types of users.  If any list of most-used Python modules doesn't include NumPy, that list is inaccurate.

Deephaven is a real-time, time-series, column-oriented data engine with relational database features.  Its Python API enables users to interact with table data using any Python module of their choice.  Given NumPy's popularity and capabilities, it makes perfect sense to pair it with Deephaven and the learn module.  The learn module provides efficient data transfer to and from Deephaven tables and NumPy arrays.  So how can you use these two in tandem to bring solutions to fruition faster and more efficiently than ever before?  In this demo notebook, we will show you how you can use these two to do some interesting analysis on both static an real-time data.
\

## NumPy ndarrays

NumPy is used to create N-dimensional arrays through its `ndarray` object.  To create one, use the `array` method.

```python
import numpy as np

a = np.array([[1, 2, 3], [4, 5, 6]])
b = np.array([[-1, -2], [-3, -4], [-5, -6]])

print(a)
print(b)
```
\
\
There are many more methods that can be used to create arrays.

```python
c = np.ones((3, 1))
d = np.zeros((2, 4))
e = np.linspace(0, 1, 5)
f = np.arange(10)
print(c)
print(d)
print(e)
print(f)
```
\
\
Accessing rows and columns in NumPy `ndarray` objects works similar to that of standard Python lists.

```python
first_row_of_a = a[0, :]
second_column_of_a = a[:, 1]
print(first_row_of_a)
print(second_column_of_a)
```

NumPy supports a wide variety of data types in its `ndarray` objects.  The complete list can be found [here](https://numpy.org/doc/stable/user/basics.types.html).  To create an array of a specific data type, declare it in the method call via the `dtype` argument.

```python
g = np.array([1, 2, 3], dtype = np.short)
h = np.array([1, 2, 3], dtype = np.double)
print(g.dtype)
print(h.dtype)
```
\
\
These data types will be important in Deephaven queries, since they each have a mapping to a Java primitive supported by `deephaven.learn`.  This will be covered later in this notebook.  For more information, see [Inputs and the gather function](https://deephaven.io/core/docs/how-to-guides/use-deephaven-learn/#inputs-and-the-gather-function).

## Basic NumPy array operations

Arrays are the building block of applications that use NumPy.  Thus, there are a wide variety of array operations that NumPy supports.  Here are just a few.

```python
a_transpose = a.T
b_transpose = np.transpose(b)
a_dot_b = a @ b
a_cross_b_transpose = np.cross(a, b_transpose)
b_flattened = np.ravel(b)
print(f"Transpose of a:\n{a_transpose}")
print(f"Tranpsose of b:\n{b_transpose}")
print(f"Dot product:\n{a_dot_b}")
print(f"Cross product:\n{a_cross_b_transpose}")
print(f"b flattened:\n{b_flattened}")
```
\
\
## Create a table with data

Let's integrate NumPy with Deephaven by creating a table with some data.  That data will be an independent variable (X) and sum of sine waves of different amplitudes and frequencies (Y).  We can construct this noisy signal with NumPy's built in function for a sine wave.

```python
from deephaven.TableTools import emptyTable

def generate_noisy_signal(x):
    return 3.5 * np.sin(x) + 1.5 * np.sin(2.5 * x) + 0.75 * np.sin(3.5 * x) + np.random.normal()

data_table = emptyTable(1000).update(
    "X = 0.01 * (int)i",
    "Y = (double)generate_noisy_signal(X)"
)
```
\

## Plot data

Let's plot the `X` and `Y` columns to see what this signal like.

```python
from deephaven import Plot

data_plot = Plot.plot("Noisy Signal", data_table, "X", "Y").show()
```
\

## Remove noise

Our signal is cyclic, but with some clear irregularities, since we have the sum of three different sine waves of differing amplitudes and frequencies.  Without the prior knowledge we have (since we just made the signal), we might know that it's cyclic, but not much else.  We would, however, probably want to learn more about the signal beneath the noise.  There are many different ways we could get more information, so let's explore a few of them.
\
\
We can start by quantizing each value in the `Y` column.  This probably won't help too much, but hey, let's give it a shot.  Remember, we're pretending we didn't just create this data ourselves!

```python
data_table_quantized = data_table.update("Quantized_Y = (double)np.round(Y)")

quantized_plot = Plot.plot("Quantized Signal", data_table_quantized, "X", "Quantized_Y").show()
```
\
\
Hmm.  Rounding each data point to the nearest integer doesn't really help much.  That's also not really surprising.  We're not really going to be able to reduce noise in the data by only working on one data point at a time.  We could operate on multiple data points by using globals to access them in a function, but that's generally not good coding practice.  Instead, let's use the learn module to operate on however many values we please.  Before we dive into real analysis, let's start with the basics of the learn module.
\

## deephaven.learn

`deephaven.learn` is a package designed to facilitate the transfer of data to and from Deephaven tables and NumPy arrays.  We can exemplify this with some simple code.

```python
from deephaven import learn
from deephaven.learn import gather

def print_num_rows_and_cols(data):
    n_rows = len(data[:, 0])
    n_cols = len(data[0, :])
    print(f"{n_rows} rows and {n_cols} columns.")

def table_to_numpy_double(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, dtype = np.double)

learn.learn(
    table = data_table,
    model_func = print_num_rows_and_cols, 
    inputs = [learn.Input(["X", "Y"], table_to_numpy_double)],
    outputs = None,
    batch_size = 101
)
```
\
\
Let's break down the code above step by step:

- Import `deephaven.learn` and `deephaven.learn.gather`.
- Create a function called `print_num_rows_and_cols`.
  - This function will print the number of rows and columns in the input data object.
- Create a function alled `table_to_numpy_double`.
  - This function will transfer Deephaven table data to a NumPy array of doubles using `deephaven.learn.gather`.
- Call the `learn` function.
  - There are five inputs:
    - `table`: The table from which to source data.
    - `model_func`: The function that will operate on the data sourced from the table.
    - `inputs`: A list of `learn.Input` objects, each of which defines the columns in the table to grab data from, and how it will be transferred to a NumPy array.
    - `outputs`: A list of `learn.Output` objects.  Right now, our `model_func` returns nothing, so this is `None`.  This will not be `None` in the next code block.
    - `batch_size`: The maximum number of rows that `deephaven.learn` will grab from a table at once.

There are 1000 rows in `data_table`.  We've specified a `batch_size` of 101 to demonstrate that the `deephaven.learn` will take _**up to**_ `batch_size` rows at once.  The first nine times data is taken from the table, 101 rows are used.  The last time it's called, only 91 rows remain in the table, so 91 are used.  This is an important concept that plays a critical role for real-time applications, as we'll demonstrate later on.
\

## Quantize the signal with deephaven.learn

We previously "quantized" the signal by rounding each value to the nearest integer.  It was a very poor attempt to smooth the noise out of the data.  This time, let's properly quantize the data by quantizing segments at a time using `deephaven.learn`.  We can reuse the `table_to_numpy_double` function we just made.  This time around, we'll have to use a function that defines how data is placed from a NumPy array back into a table.

```python
def numpy_to_table(data, index):
    return data[index]

def quantize_signal(data):
    y = data[:, 1]
    n_rows = len(y)
    return np.array([np.mean(y)] * n_rows)

data_table_quantized = learn.learn(
    table = data_table,
    model_func = quantize_signal,
    inputs = [learn.Input(["X", "Y"], table_to_numpy_double)],
    outputs = [learn.Output("Quantized_Y", numpy_to_table, "double")],
    batch_size = 20
)

data_plot_quantized = Plot.plot("Raw Signal", data_table_quantized, "X", "Y").plot("Quantized Signal", data_table_quantized, "X", "Quantized_Y").show()
```

\

## Rolling ball filter

That helps a bit, but the real signal clearly isn't quantized like this underneath the noise.  We are taking a step in the right direction.  This time around, let's do some real signal filtering by applying a discrete convolution of the signal based on the `batch_size` we provide.

```python
def numpy_to_table(data, index):
    return data[index]

def rolling_ball(data):
    y = data[:, 1]
    n_rows = len(y)
    kernel_size = 20
    kernel = np.ones(kernel_size) / kernel_size
    return np.convolve(y, kernel, mode = 'same')

data_table_smoothed = learn.learn(
    table = data_table,
    model_func = rolling_ball,
    inputs = [learn.Input(["X", "Y"], table_to_numpy_double)],
    outputs = [learn.Output("Smoothed_Y", numpy_to_table, "double")],
    batch_size = 100
)

data_plot_smoothed = Plot.plot("Raw Signal", data_table_smoothed, "X", "Y").plot("Quantized Signal", data_table_smoothed, "X", "Smoothed_Y").show()
```
\
\
Convolving a signal in this manner is known as a rolling ball filter.  We're applying this filter to every 100 data points in the plot, for a total of 10 iterations on the static data.  There are noticeable spikes at certain multiples of our `batch_size` due to how the data is processed.  Visually, this would look like rolling a ball over points 0 - 99, picking it up, dropping it on point 100, and repeating until every data point is smoothed.  If we supply a `batch_size` equal to the table size (`data_table.intSize()`), we will see these irregularities disappear.  That approach, though, won't translate to real-time.

## Polynomial fit

Let's do one last thing for our static table: perform a polynomial fit using NumPy's built in functions.

```python
def table_to_numpy_double(rows, columns):
    return gather.table_to_numpy_2d(rows, columns, dtype = np.double)

def numpy_to_table(data, index):
    return data[index]

def polynomial_fit(data):
    x = data[:, 0]
    y = data[:, 1]
    poly_order = 3
    z = np.polyfit(x, y, poly_order)
    p = np.poly1d(z)
    return p(x)

data_table_polyfitted = learn.learn(
    table = data_table,
    model_func = polynomial_fit,
    inputs = [learn.Input(["X", "Y"], table_to_numpy_double)],
    outputs = [learn.Output("Fitted_Y", numpy_to_table, "double")],
    batch_size = 100
)

data_plot_polyfitted = Plot.plot("Raw Signal", data_table, "X", "Y").plot("Polyfitted Signal", data_table_polyfitted, "X", "Fitted_Y").show()
```
\
\
That looks pretty good!  Once again, we see discontinuities in our fitted curve at each multiple of our `batch_size`.  We don't enforce continuity between our windows, so this is expected.  There are ways to mitigate this, but we won't cover them here.

## Create the noisy signal in real-time

As a final exercise, let's set up a real-time data feed based on the static examples we've done so far.  To do that, we'll first need to set up the real-time table itself.  We will set it up such that 100 values are written a second for 30 seconds.

```python
from deephaven import DynamicTableWriter
import deephaven.Types as dht

import threading, time

sleep_time = 1

table_writer = DynamicTableWriter(
    ["X", "Y"],
    [dht.double, dht.double]
)

data_table_live = table_writer.getTable()

def write_noisy_signal():
    x0 = 0
    x1 = 1
    step_size = 0.01
    for i in range(30):
        start = time.time()
        x = np.arange(x0, x1, step_size)
        y = 3.5 * np.sin(x) + 1.5 * np.sin(x) + 0.75 * np.sin(3.5 * x) + np.random.normal(0, 1, 100)
        for i in range(len(y)):
            table_writer.logRow(x[i], y[i])
        x0 += 1
        x1 += 1
        end = time.time()
        elapsed = end - start
        time.sleep(sleep_time - elapsed)
    
thread = threading.Thread(target = write_noisy_signal)
thread.start()

data_plot_live = Plot.plot("Raw Signal", data_table_live, "X", "Y").show()
```


# Polynomial fit in real-time

Alright, with a live feed set up and ready to go, let's do the polynomial fitting.  But this time, it's in real-time.  Oh yea, and we don't have to do any more work.  Everything is already set up that we need!

```python
data_table_live_polyfitted = learn.learn(
    table = data_table_live,
    model_func = polynomial_fit, 
    inputs = [learn.Input(["X", "Y"], table_to_numpy_double)],
    outputs = [learn.Output("Fitted_Y", numpy_to_table, "double")],
    batch_size = 100
)

data_plot_live_polyfitted = Plot.plot("Raw Signal", data_table_live, "X", "Y").plot("Polyfitted Signal", data_table_live_polyfitted, "X", "Fitted_Y").show()
```
\
\
And there we have it!  Piece of cake.  What kind of real-time data processing applications can you cook up?  Whatever you decide to do, Deephaven will make it easier.
\
\
Got questions for us?  Check out our [Slack](https://join.slack.com/t/deephavencommunity/shared_invite/zt-11x3hiufp-DmOMWDAvXv_pNDUlVkagLQ) or [GitHub discussions](https://github.com/deephaven/deephaven-core/discussions) pages.  We're happy to help!
