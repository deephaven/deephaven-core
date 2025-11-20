---
title: Use Numba in Deephaven queries
sidebar_label: Numba
---

This guide will show you how to use [Numba](https://numba.pydata.org/) in your Python queries in Deephaven.

[Numba](https://numba.pydata.org/) is an open-source just-in-time (JIT) compiler for Python. It can be used to translate portions of Python code into optimized machine code using [LLVM](https://llvm.org/). The use of [Numba](https://numba.pydata.org/) can make your queries faster and more responsive.

## What is just-in-time (JIT) compilation?

JIT compiles code into optimized machine code at runtime. When using [Numba](https://numba.pydata.org/), you can specify which functions and blocks of code you want to be compiled at runtime. Compiling code blocks into optimized machine code adds some overhead, but subsequent function calls can be much faster. Thus, JIT can be powerful when used on functions that are complex, large, or will be used many times.

> [!CAUTION]
> JIT does not guarantee faster code. Sometimes, code cannot be optimized well by [Numba](https://numba.pydata.org/) and may actually run slower.

## Usage

[Numba](https://numba.pydata.org/) uses [decorators](https://peps.python.org/pep-0318/) to specify which blocks of code to JIT compile. Common [Numba](https://numba.pydata.org/) decorators include [`@jit`](https://numba.readthedocs.io/en/stable/user/jit.html), [`@vectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html), and [`@guvectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html#the-guvectorize-decorator). See [Numba's documentation](https://numba.readthedocs.io/en/stable/index.html) for more details.

In the following example, `@jit` and `@vectorize` are used to JIT compile `function_one` and `function_two`.

```python
import math
from numba import jit, vectorize, double, int64


@jit
def function_one(a, b):
    return math.sqrt(a**2 + b**2)


@vectorize([double(double, int64)])
def function_two(decimal_value, integer_value):
    return decimal_value * integer_value
```

### Lazy compilation

If the decorator is used without a function signature, Numba will infer the argument types at call time and generate optimized code based upon the inferred types. Numba will also compile separate specializations for different input types. The compilation is deferred to the first function call. This is called lazy compilation.

The example below uses lazy compilation on the function `func`. When the code is first run, the function is compiled to optimized machine code, which takes extra overhead and results in longer execution time. On the second function call, Python uses the already optimized machine code, which results in very fast execution.

```python
import numpy as np
from numba import jit
import time


# Lazy optimized addition function
@jit
def func(x, y):
    return x + y


x = 0.01 * np.arange(100000)
y = 0.02 * np.arange(100000)

# This function call is slow.  It has to be compiled into optimized machine code.
start = time.time()
z1 = func(x, y)
end = time.time()
print("First function call took " + str(end - start) + " seconds.")

# This function call is fast.  It uses the previously JIT-compiled machine code.
start = time.time()
z2 = func(x, y)
end = time.time()
print("Second function call took " + str(end - start) + " seconds.")
```

### Eager compilation

If the decorator is used with a function signature, Numba will compile the function when the function is defined. This is called eager compilation

The example below uses eager compilation on the function `func` by specifying one or more function signatures. These function signatures denote the allowed input and output data types to and from the function. With eager compilation, the code is compiled into optimized machine code at the function definition. When the code is run the first time, it has already been compiled into optimized machine code, so it's just as fast as the second function call. If the compiled function is used with a data type not specified in the function signature, an error will occur.

```python
from numba import vectorize, int64, double
import numpy as np
import time


# Eager compilation for the int64(int64, int64) and double(double, double) function signatures happens here
@vectorize([int64(int64, int64), double(double, double)])
def func(x, y):
    z = x + y
    return z


x1 = 0.01 * np.arange(10000)
y1 = 0.02 * np.arange(10000)
x2 = 1 * np.arange(10000)
y2 = 2 * np.arange(10000)

# Run it the first time on doubles
start = time.time()
z1 = func(x1, y1)
end = time.time()
print("Add vectors of doubles (first run): " + str(end - start) + " seconds.")

# Run it a second time on doubles
start = time.time()
z2 = func(x1, y1)
end = time.time()
print("Add vectors of doubles (second run): " + str(end - start) + " seconds.")

# Run it the first time on integers
start = time.time()
z3 = func(x2, y2)
end = time.time()
print("Add vectors of integers (first run): " + str(end - start) + " seconds.")

# Run it a second time on integers
start = time.time()
z4 = func(x2, y2)
end = time.time()
print("Add vectors of integers (second run): " + str(end - start) + " seconds.")
```

> [!CAUTION]
> Eager compilation creates functions that only support specific function signatures. If these functions are applied to arguments of mismatched types, an error will occur.

## @jit vs @vectorize

We've seen the `@jit` and `@vectorize` decorators used to optimize various functions. So far, they've been used without an explanation of how they differ. So, what is different about each decorator?

- `@jit` is a general-purpose decorator that can optimize a wide variety of different functions.
- `@vectorize` is a decorator meant to allow functions that operate on arrays to be written as if they operate on scalars.

This can be visualized with a simple example:

```python skip-test
from numba import jit, vectorize, int64
import numpy as np

x = np.arange(5)
y = x + 5


@jit([int64(int64, int64)])
def jit_add_arrays(x, y):
    return x + y


@vectorize([int64(int64, int64)])
def vectorize_add_arrays(x, y):
    return x + y


z1 = vectorize_add_arrays(x, y)

print(z1)

z2 = jit_add_arrays(x, y)

print(z2)
```

![An error message in the Deephaven IDE log](../assets/how-to/Numba_jit_error.png)

Functions created using `@vectorize` can be applied to arrays. However, attempting to do the same with `@jit` results in an error.

## @vectorize vs @guvectorize

The `@vectorize` decorator allows Python functions taking scalar input arguments to be used as [NumPy ufuncs](https://numpy.org/doc/stable/reference/ufuncs.html). Creating these `ufuncs` with NumPy alone is a tricky process, but `@vectorize` makes it significantly easier. It allows users to write functions as if they're working on scalars, even though they're working on arrays.

The `@guvectorize` decorator takes the concepts from `@vectorize` one step further. It allows users to write ufuncs that work on an arbitrary number of elements of input arrays, as well as take and return arrays of differing dimensions. Functions decorated with `@guvectorize` don't return their result value; rather, thay take it as an array input argument, which will be filled in by the function itself.

The following example shows a `@guvectorize` decorated function.

```python order=:log
from numba import guvectorize, int64
import numpy as np


@guvectorize([(int64[:], int64, int64[:])], "(n),()->(n)")
def g(x, y, res):
    for i in range(x.shape[0]):
        res[i] = x[i] + y


a = np.arange(5)
print(g(a, 2))
```

The `@guvectorize` decorator is much different than the previous `@vectorize` decorators used in this document.

- The first part, `[(int64[:], int64, int64[:])]`, is largely similar. It tells Numba that the function takes three inputs: an array, a scalar, and another array, all of Numba's `int64` data type.
- The second part (after the comma), tells NumPy that the function takes an n-element one dimensional array (`(n)`) and a scalar (`()`) as input, and returns an n-element one dimensional array (`(n)`).

## Examples

### [NumPy](./use-numpy.md) matrix

The following example uses `@jit` to calculate the element-wise sum of a 250x250 matrix. This is a total of 62,500 additions.

Here, the `nopython=True` option is used. This option produces faster, primitive-optimized code that does not need the Python interpreter to execute. Without this flag, Numba will fall back to the slower [object mode](https://numba.readthedocs.io/en/stable/glossary.html#term-object-mode) in some circumstances.

This example looks at three cases:

- A regular function without JIT.
- A JIT function that needs compilation.
- A JIT function that is already compiled.

The first time the JIT-enabled function is run on a matrix of integers, it's almost ten times slower than the standard function. However, after compilation, the JIT-enabled function is almost two hundred times faster!

```python
from numba import jit
import numpy as np
import time

x = np.arange(62500).reshape(250, 250)
y = 0.01 * x


def calc(a):
    matrix_sum = 0.0
    for i in range(a.shape[0]):
        for j in range(a.shape[1]):
            matrix_sum += a[i, j]
    return matrix_sum


@jit(nopython=True)
def jit_calc(a):
    matrix_sum = 0.0
    for i in range(a.shape[0]):
        for j in range(a.shape[1]):
            matrix_sum += a[i, j]
    return matrix_sum


# Time without JIT
start = time.time()
calc(x)
end = time.time()
print("Execution time (without JIT) = %s" % (end - start))

# Time with compilation and JIT
start = time.time()
jit_calc(x)
end = time.time()
print("Execution time (JIT + compilation) = %s" % (end - start))

# Time with JIT (already compiled)
start = time.time()
jit_calc(x)
end = time.time()
print("Execution time (JIT) = %s" % (end - start))
```

### Using Deephaven tables

To show how the performance of `@jit` and `@vectorize` differ when applied to Deephaven tables, we will create identical functions that use these decorators. We then measure the performance of creating new columns in a 625,000 row table when using the functions.

```python order=t,t2,t3,t4,t5,t6
from deephaven import empty_table
from numba import jit, vectorize, double, int64
import time


def add_columns(A, B) -> int:
    return A + B


@jit([int64(int64, int64)])
def jit_add_columns(A, B) -> int:
    return A + B


@vectorize([int64(int64, int64)])
def vectorize_add_columns(A, B) -> int:
    return A + B


def cubic_func(C) -> double:
    return 0.0025 * C**3 - 1.75 * C**2 - C + 10


@jit([double(int64)])
def jit_cubic_func(C) -> double:
    return 0.0025 * C**3 - 1.75 * C**2 - C + 10


@vectorize([double(int64)])
def vectorize_cubic_func(C) -> double:
    return 0.0025 * C**3 - 1.75 * C**2 - C + 10


t = empty_table(625000).update(formulas=["A = ii", "B = ii"])

# Time column addition without Numba
start = time.time()
t2 = t.update(formulas=["C = add_columns(A, B)"])
end = time.time()
print("column addition - Execution time (without Numba) = %s" % (end - start))

# Time column addition with jit
start = time.time()
t3 = t.update(formulas=["C = jit_add_columns(A, B)"])
end = time.time()
print("column addition - Execution time (jit) = %s" % (end - start))

# Time column addition with vectorize
start = time.time()
t4 = t.update(formulas=["C = vectorize_add_columns(A, B)"])
end = time.time()
print("column addition - Execution time (vectorize) = %s" % (end - start))

# Time cubic polynomial without Numba
start = time.time()
t5 = t2.update(formulas=["D = cubic_func(C)"])
end = time.time()
print("cubic polynomial - Execution time (without Numba) = %s" % (end - start))

# Time cubic polynomial with jit
start = time.time()
t5 = t2.update(formulas=["D = jit_cubic_func(C)"])
end = time.time()
print("cubic polynomial - Execution time (jit) = %s" % (end - start))

# Time a cubic polynomial with vectorize
start = time.time()
t6 = t2.update(formulas=["D = vectorize_cubic_func(C)"])
end = time.time()
print("cubic polynomial - Execution time (vectorize) = %s" % (end - start))
```

The use of `@jit` with functions operating on Deephaven tables results in a very small performance increase over its standard counterparts. This performance increase is small enough to make the additional overhead of compiling the function into optimized machine code not worth it.

The use of `@vectorize` with functions operating on Deephaven tables results in a large performance increase over its standard counterparts. This performance increase is large enough to warrant the additional overhead associated with compiling the function into optimized machine code.

### @guvectorize

The following example uses the `@guvectorize` decorator on the function `g`, which is used in an [`update`](../reference/table-operations/select/update.md) operation.

```python order=result,source
from numba import guvectorize, int64
from deephaven import empty_table
from numpy import typing as npt
import numpy as np


def array_from_val(val) -> npt.NDArray[np.int64]:
    return np.array([val, val + 1, val + 2], dtype=np.int64)


@guvectorize([(int64[:], int64, int64[:])], "(n),()->(n)")
def g(x, y, res) -> npt.NDArray[np.int64]:
    for i in range(x.shape[0]):
        res[i] = x[i] + y


source = empty_table(5).update(["X = i", "Y = array_from_val(X)"])
result = source.update(["Z = g(Y, X)"])
```

## Related documentation

- [How to select data in tables](./use-select-view-update.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
