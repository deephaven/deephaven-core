# Deephaven and Numba

This notebook will show you how `Numba` can speed up Deephaven Python queries.
It will also demonstrate two common Numba decorators, `@jit` and `@vectorize`.

`Numba` is a Python package that can help speed up code via JIT compilation.
JIT compiles code at runtime into optimized machine code, which _can_ greatly decrease execution time.

## Decorators in Python

Decorators in Python are denoted by the `@` symbol.  The `@` symbol comes on the line before a function definition.  When using `Numba`, the decorator will specify what type of JIT compilation you'd like to use.

```python
from numba import jit, vectorize

@jit
def func_one(x, y):
    return x + y

@vectorize
def func_two(x, y):
    return x * y
```

In this code block, the first decorator is `@jit`, which is a general-purpose decorator, and the second is `@vectorize`, which allows functions that operate on arrays to be written as if they operate on scalars.  We'll cover both of these shortly.

## What is JIT?

JIT stands for Just-In-Time.  JIT compilation means a function is compiled into optimized machine code at runtime.  Traditionally, the Python interpreter will translate the Python code into machine code, then run it.  When JIT is used, a JIT-decorated function is translated into optimized machine code just before it is run during program execution.  This adds additional overhead to the first function call, but will (most of the time) significantly speed up subsequent function calls.

## Lazy vs Eager compilation

### Lazy compilation

Let's circle back to the code block above.  The functions `func_one` and `func_two` are each decorated.  The decorators are simple, and contain no function signatures.  When using Numba to JIT-compile code, this type of decoration is called "lazy compilation".

The lack of function signature means that Numba must figure out the input and output data types on the fly.  Each time the function is called, Numba will either compile the code into optimized machine code, or use machine code that's already been optimized for that data type.

This means that, for every time you call a lazy compiled function for the first time for a new data type, new machine code will be compiled.  Let's see what this looks like:

```python
from numba import jit
import numpy as np
import time

x = 0.025 * np.arange(1000000)
y = 0.033 * np.arange(1000000)

start = time.time()
z1 = func_one(x, y)
end = time.time()
print(f"First function call: {end - start} seconds.")

start = time.time()
z2 = func_one(x, y)
end = time.time()
print(f"Second function call: {end - start} seconds.")
```

The first time we call `func_one`, it's slow because the execution time include the time required to compile the code into optimized machine code.  The second time we call `func_one`, it's already been compiled into optimized machine code (**for these input and output data types only**).  Numba tells the Python interpreter to just go use the optimized machine code that already exists, resulting in a lightning fast execution time.

Let's see what happens when we use different data types:

```python
from numba import jit
import numpy as np
import time

@jit
def func_one(x, y):
    return x + y

x1 = 0.025 * np.arange(1000000)
y1 = 0.033 * np.arange(1000000)

start = time.time()
z1 = func_one(x1, y1)
end = time.time()
print(f"First function call: {end - start} seconds.")

x2 = np.arange(1000000)
y2 = 2 * np.arange(1000000)

start = time.time()
z2 = func_one(x2, y2)
end = time.time()
print(f"Second function call: {end - start} seconds.")
```

This time around, both function calls are slow.  The second time we call the function, the inputs are arrays of whole numbers (integers), not decimal values (floats), so it has to re-compile the machine code.

### Eager compilation

Eager compilation specifies the function signature in the decorator.  This causes the compilation to occur prior to runtime for every function signature given.  Be careful with eager compilation, as using the wrong data type will result in an error.

In the code below, we'll use eager compilation and call `func_one` four times: twice for each function signature.

```python
from numba import vectorize, int64, double
import numpy as np
import time

@vectorize([int64(int64, int64),
            double(double, double)])
def func_one(x, y):
    z = x + y
    return z

x1 = 0.01 * np.arange(10000)
y1 = 0.02 * np.arange(10000)
x2 = 1 * np.arange(10000)
y2 = 2 * np.arange(10000)

start = time.time()
z1 = func(x1, y1)
end = time.time()
print("Add vectors of doubles (first run): " + str(end - start) + " seconds.")

start = time.time()
z2 = func(x1, y1)
end = time.time()
print("Add vectors of doubles (second run): " + str(end - start) + " seconds.")

start = time.time()
z3 = func(x2, y2)
end = time.time()
print("Add vectors of integers (first run): " + str(end - start) + " seconds.")

start = time.time()
z4 = func(x2, y2)
end = time.time()
print("Add vectors of integers (second run): " + str(end - start) + " seconds.")
```

All four execution times are nearly identical.  That's because the optimized machine code is available for the given function signature every time the function gets called.  If we were to try any other type of variable (not an int or double), an error would get raised.

## The `@jit` decorator

`@jit` is a general-purpose decorator that can be used on a wide variety of functions.  It can work on basically any type of calculation and any type of input or output variables.

## The `@vectorize` decorator

`@vectorize` is used to write expressions that can be applied element-wise to arrays.  For instance, our function in the code blocks in the previous sections, `func_one`, could be decorated with `@vectorize`.  The function was called on arrays, despite the ability to be called on singular values as well.

## Speeding up Deephaven queries

If you are considering using Numba in your Deephaven queries, `@vectorize` is probably the decorator you'll want to use.  Columns in Deephaven tables can be thought of as one-dimensional columnar arrays.  Thus, `@vectorize` is the Numba decorator that applies more to functions that operate on data in Deephaven tables.

Let's see how queries in Deephaven can be sped up with the `@vectorize` decorator:

```python
from deephaven.TableTools import emptyTable
from numba import jit, vectorize, double, int64
import time

def add_columns(a, b):
    return a + b

@jit([int64(int64, int64)])
def add_columns_jit(a, b):
    return a + b

@vectorize([int64(int64, int64)])
def add_columns_vectorize(a, b):
    return a + b

def cubic_func(c):
    return 0.0025 * c**3 - 1.75 * c**2 - c + 10

@jit([double(int64)])
def cubic_func_jit(c):
    return 0.0025 * c**3 - 1.75 * c**2 - c + 10

@vectorize([double(int64)])
def cubic_func_vectorize(c):
    return 0.0025 * c**3 - 1.75 * c**2 - c + 10

t = emptyTable(625000).update("A = ii", "B = ii")

# Time column addition without Numba
start = time.time()
t2 = t.update("C = add_columns(A, B)")
end = time.time()
print("column addition - Execution time (without Numba) = %s" % (end - start))

# Time column addition with jit
start = time.time()
t3 = t.update("C = add_columns_jit(A, B)")
end = time.time()
print("column addition - Execution time (jit) = %s" % (end - start))

# Time column addition with vectorize
start = time.time()
t4 = t.update("C = add_columns_vectorize(A, B)")
end = time.time()
print("column addition - Execution time (vectorize) = %s" % (end - start))

# Time cubic polynomial without Numba
start = time.time()
t5 = t2.update("D = cubic_func(C)")
end = time.time()
print("cubic polynomial - Execution time (without Numba) = %s" % (end - start))

# Time cubic polynomial with jit
start = time.time()
t5 = t2.update("D = cubic_func_jit(C)")
end = time.time()
print("cubic polynomial - Execution time (jit) = %s" % (end - start))

# Time a cubic polynomial with vectorize
start = time.time()
t6 = t2.update("D = cubic_func_vectorize(C)")
end = time.time()
print("cubic polynomial - Execution time (vectorize) = %s" % (end - start))
```

The functions decorated with `@vectorize` run extremely fast compared to the others.

Perhaps Numba can help you speed up your Deephaven Python queries!  When working with big data in real-time, time is critical.  Numba could mean the difference between queries that keep up with high volumes of incoming data and queries that slow down and possibly crash due to it.