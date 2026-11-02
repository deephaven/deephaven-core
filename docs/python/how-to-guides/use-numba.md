---
title: Use Numba in Deephaven queries
---

This guide shows how to use [Numba](https://numba.pydata.org/) in Deephaven Python queries. The first sections cover the Numba basics the examples rely on: JIT compilation, Numba's decorators, lazy and eager compilation, and how `@jit`, `@vectorize`, and `@guvectorize` differ. [Use Numba functions in query strings](#use-numba-functions-in-query-strings) covers how Deephaven calls Numba functions in [table operations](../conceptual/table-operations-overview.md) and the rules those functions must follow.

Numba is an open-source just-in-time (JIT) compiler for Python. It uses [LLVM](https://llvm.org/) to translate portions of Python code into optimized machine code, which can make your queries faster and more responsive.

A [Python function](./python-functions.md) in a [query string](./query-string-overview.md) runs for every row Deephaven computes, including the new and modified rows [each time a real-time table updates](../conceptual/table-update-model.md). A faster function speeds up every update cycle.

> [!NOTE]
> Numba is an optional package, and whether you already have it depends on how you run Deephaven:
>
> - Since 41.3, the `deephaven-core` and `deephaven-server` pip packages don't install it.
> - The `ghcr.io/deephaven/server` Docker images include it from version 43.0. The 41.3 through 42.x images don't.
>
> Each example on this page runs `pip install numba` first, which does nothing if Numba is already installed. For the fastest [query string](./query-string-overview.md) calls, install Numba before Deephaven starts, as [Use Numba functions in query strings](#use-numba-functions-in-query-strings) explains.

## What is just-in-time (JIT) compilation?

JIT compiles code into optimized machine code at runtime. With Numba, you mark which functions to compile at runtime. Compiling code blocks into optimized machine code adds some overhead, but subsequent function calls can be much faster. JIT is most useful for functions that are complex, large, or called many times.

> [!CAUTION]
> JIT does not guarantee faster code. Numba can't optimize some code well, and that code may run slower.

## Numba decorators and compilation

Numba uses [decorators](https://peps.python.org/pep-0318/) to specify which functions to JIT compile. Common Numba decorators include [`@jit`](https://numba.readthedocs.io/en/stable/user/jit.html), [`@vectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html), and [`@guvectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html#the-guvectorize-decorator). See [Numba's documentation](https://numba.readthedocs.io/en/stable/index.html) for more details.

`@jit` and `@vectorize` differ in what they're for:

- `@jit` is a general-purpose decorator that can optimize a wide variety of functions.
- `@vectorize` lets you write a function that takes scalars, and Numba applies it element-wise to arrays.

[`@jit` vs `@vectorize`](#jit-vs-vectorize) shows this difference in action, and [`@vectorize` vs `@guvectorize`](#vectorize-vs-guvectorize) covers how `@guvectorize` differs from `@vectorize`.

The following example uses `@jit` to compile `function_one` and `@vectorize` to compile `function_two`, then calls each one. `function_one` takes two scalars. `function_two` is written for scalars, and Numba applies it element-wise to the two arrays.

```python docker-config=numba
import math
import os

os.system("pip install numba")
from numba import jit, vectorize, double, int64
import numpy as np


@jit
def function_one(a, b):
    return math.sqrt(a**2 + b**2)


@vectorize([double(double, int64)])
def function_two(decimal_value, integer_value):
    return decimal_value * integer_value


print(function_one(3.0, 4.0))
print(function_two(np.array([1.5, 2.5]), np.array([2, 3])))
```

> [!NOTE]
> Since Numba 0.59, `@jit` compiles in [nopython mode](https://numba.readthedocs.io/en/stable/glossary.html#term-nopython-mode) by default. Nopython mode produces machine code that runs without the Python interpreter. Numba no longer falls back to the slower [object mode](https://numba.readthedocs.io/en/stable/glossary.html#term-object-mode) when it can't compile a function this way. Passing `nopython=True`, or using the equivalent `@njit` decorator, makes this explicit.

### Lazy compilation

A function signature lists the input and output data types a compiled function accepts, such as `double(double, int64)` in the earlier `function_two` example. When you use a Numba decorator such as `@jit` or `@vectorize` without a function signature, Numba waits until the first call to compile the function. This is called lazy compilation. With lazy compilation, Numba:

- Infers the argument types at call time.
- Generates optimized code for those types.
- Compiles a separate specialization for each new combination of input types.

The example below uses lazy compilation on the function `func`. The first call is slow because Numba compiles `func` to machine code before running it. The second call reuses the compiled code and runs much faster.

```python docker-config=numba
import numpy as np
import os

os.system("pip install numba")
from numba import jit
import time


# Lazy optimized addition function
@jit
def func(x, y):
    return x + y


x = 0.01 * np.arange(100000)
y = 0.02 * np.arange(100000)

# This function call is slow. It has to be compiled into optimized machine code.
start = time.time()
z1 = func(x, y)
end = time.time()
print("First function call took " + str(end - start) + " seconds.")

# This function call is fast. It uses the previously JIT-compiled machine code.
start = time.time()
z2 = func(x, y)
end = time.time()
print("Second function call took " + str(end - start) + " seconds.")
```

### Eager compilation

When you use a Numba decorator such as `@jit` or `@vectorize` with a function signature, Numba compiles the function when Python defines it. This is called eager compilation.

The example below uses eager compilation on the function `func` by specifying two function signatures. Numba compiles `func` when Python defines it, so even the first call on each input type skips compilation and runs about as fast as the second call.

```python docker-config=numba
import os

os.system("pip install numba")
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
> Eager compilation creates functions that only support specific function signatures. Arguments that Numba can't convert to one of the declared signatures raise an error.

## `@jit` vs `@vectorize`

The difference between [`@jit`](https://numba.readthedocs.io/en/stable/user/jit.html) and [`@vectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html) shows up when you pass arrays to a function compiled with a scalar signature. In the following example, both decorators compile the same addition function with the scalar signature `int64(int64, int64)`. The `@jit` call is wrapped in `try`/`except` so the example prints the error instead of stopping:

```python order=:log docker-config=numba
import os

os.system("pip install numba")
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

try:
    z2 = jit_add_arrays(x, y)
    print(z2)
except TypeError as e:
    print(e)
```

Functions created with `@vectorize` apply their scalar signature element-wise to arrays, so `vectorize_add_arrays` prints `[ 5  7  9 11 13]`. A `@jit` function compiled with a scalar signature such as `int64(int64, int64)` rejects array arguments, so the call to `jit_add_arrays` raises a `TypeError` because Numba has no compiled definition for array arguments. The lazily compiled `@jit` function in [Lazy compilation](#lazy-compilation) accepts arrays because Numba compiles it for the argument types it receives.

## `@vectorize` vs `@guvectorize`

The [`@vectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html) decorator turns a Python function that takes scalar arguments into a [NumPy ufunc](https://numpy.org/doc/stable/reference/ufuncs.html). Without Numba, a fast ufunc has to be written in C.

The [`@guvectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html#the-guvectorize-decorator) decorator creates [generalized ufuncs](https://numpy.org/doc/stable/reference/c-api/generalized-ufuncs.html), which differ from `@vectorize` functions in these ways:

- They can work on any number of elements of the input arrays at once.
- They can take and return arrays of different dimensions.
- They don't return their result. Instead, the function fills in an output array that it takes as its last parameter.

When you call a `@guvectorize` function, you can leave out the output argument, and NumPy creates the output array and returns it.

The following example defines `g`, a `@guvectorize` function that adds a scalar to every element of an array, and calls it on `[0, 1, 2, 3, 4]` with the scalar `2`.

```python order=:log docker-config=numba
import os

os.system("pip install numba")
from numba import guvectorize, int64
import numpy as np


@guvectorize([(int64[:], int64, int64[:])], "(n),()->(n)")
def g(x, y, res):
    for i in range(x.shape[0]):
        res[i] = x[i] + y


a = np.arange(5)
print(g(a, 2))
```

The `@guvectorize` decorator in this example takes two arguments:

- The first, `[(int64[:], int64, int64[:])]`, is a list of type signatures like the one passed to `@vectorize`. It tells Numba that the function takes three parameters: a one-dimensional `int64` input array (`int64[:]`), an `int64` input scalar, and a one-dimensional `int64` output array.
- The second, `"(n),()->(n)"`, is the layout signature. In it, `(n)` before the arrow is an n-element one-dimensional input array, `()` is a scalar input, and `(n)` after the arrow is the n-element one-dimensional output array, which the function fills in.

## Use Numba functions in query strings

You call a Numba function in a [query string](./query-string-overview.md) by name, like any other [Python function](./python-functions.md). Deephaven checks whether Numba is installed when the server starts, and the result decides how Deephaven calls the function.

Deephaven treats a [`@jit`](https://numba.readthedocs.io/en/stable/user/jit.html) function like a plain [Python function](./python-functions.md), whether or not Numba was installed at startup. Each call receives one row's values.

When Numba is installed at startup, these rules apply to [`@vectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html) and [`@guvectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html#the-guvectorize-decorator) functions in [query strings](./query-string-overview.md):

- For both decorators, Deephaven reads argument and return types from the function's Numba signature, so the function must have exactly one explicit signature. A function with no signature or with several signatures, such as the two-signature `func` in [Eager compilation](#eager-compilation), raises an error.
- Deephaven passes a `@vectorize` function whole [chunks](../conceptual/vectorization-and-recipes.md#the-chunk-architecture) of column data as arrays, so one call computes many rows. A `@guvectorize` function still receives one row's values per call.
- A `@vectorize` function must be the whole [formula](./formulas.md), as in `"C = f(A, B)"`. You can't use it inside a larger expression or cast its result.
- Each argument to a `@vectorize` function must be a column, a variable, or a constant.

If you install Numba with `pip` in a running session instead, Deephaven treats `@vectorize` and `@guvectorize` functions like plain [Python functions](./python-functions.md) until the server restarts. Each call receives one row's values, and Deephaven takes the column types from [type hints](./python-functions.md#type-hints), not from the Numba signature.

> [!IMPORTANT]
> To install Numba before the server starts, use a `ghcr.io/deephaven/server` image at version 43.0 or later, or build a [custom Docker image](./install-and-use-python-packages.md#permanently-add-packages-to-a-custom-docker-image) that adds it. If you run Deephaven from the `deephaven-server` pip package, install Numba in the same Python environment before you start the server.

### Compare `@jit` and `@vectorize` in a query

The following example compares the performance of [`@jit`](https://numba.readthedocs.io/en/stable/user/jit.html) and [`@vectorize`](https://numba.readthedocs.io/en/stable/user/vectorize.html) on Deephaven tables. It defines a column addition function and a cubic polynomial function, each in three versions: plain Python, `@jit`, and `@vectorize`. It then times how long each version takes to create a new column in a 625,000-row table.

If this example's `pip install` is what installs Numba, the `@vectorize` functions run like plain [Python functions](./python-functions.md), and Deephaven takes their column types from the `-> int` and `-> float` [type hints](./python-functions.md#type-hints). To time chunked `@vectorize` calls, run the example on a server that had Numba installed at startup.

```python order=t,t2,t3,t4,t5,t6,t7 docker-config=numba
from deephaven import empty_table
import os

os.system("pip install numba")
from numba import jit, vectorize, double, int64
import time


def add_columns(a, b) -> int:
    return a + b


@jit([int64(int64, int64)])
def jit_add_columns(a, b) -> int:
    return a + b


@vectorize([int64(int64, int64)])
def vectorize_add_columns(a, b) -> int:
    return a + b


def cubic_func(c) -> float:
    return 0.0025 * c**3 - 1.75 * c**2 - c + 10


@jit([double(int64)])
def jit_cubic_func(c) -> float:
    return 0.0025 * c**3 - 1.75 * c**2 - c + 10


@vectorize([double(int64)])
def vectorize_cubic_func(c) -> float:
    return 0.0025 * c**3 - 1.75 * c**2 - c + 10


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
t6 = t2.update(formulas=["D = jit_cubic_func(C)"])
end = time.time()
print("cubic polynomial - Execution time (jit) = %s" % (end - start))

# Time cubic polynomial with vectorize
start = time.time()
t7 = t2.update(formulas=["D = vectorize_cubic_func(C)"])
end = time.time()
print("cubic polynomial - Execution time (vectorize) = %s" % (end - start))
```

How much either decorator speeds up a [formula](./formulas.md) depends on the workload, so benchmark your own queries before you decide to use Numba.

### Use `@guvectorize` in a query

The following example calls a `@guvectorize` function in an [`update`](../reference/table-operations/select/update.md) operation. The plain Python function `array_from_val` builds the array column `Y`, and `g`, the function from [`@vectorize` vs `@guvectorize`](#vectorize-vs-guvectorize), adds each row's `X` to it.

This version of `g` also has the return type hint `-> npt.NDArray[np.int64]`. Deephaven uses that [type hint](./python-functions.md#type-hints) for the type of column `Z` when Numba was installed in the running session rather than at startup.

```python order=result,source docker-config=numba
import os

os.system("pip install numba")
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

Each row of `Z` is the array in `Y` with that row's `X` added to every element.

## Related documentation

- [Table operations overview](../conceptual/table-operations-overview.md)
- [The Python-Java boundary and how it affects query efficiency](../conceptual/python-java-boundary.md)
- [Vectorization and the recipe paradigm](../conceptual/vectorization-and-recipes.md)
- [The table update model](../conceptual/table-update-model.md)
- [Select and create columns](./use-select-view-update.md)
- [Python functions in query strings](./python-functions.md)
- [Query string overview](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Install and use Python packages](./install-and-use-python-packages.md)
- [Use NumPy in Deephaven queries](./use-numpy.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`empty_table` Pydoc](/core/pydoc/code/deephaven.table_factory.html#deephaven.table_factory.empty_table)
- [`update` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table.update)
