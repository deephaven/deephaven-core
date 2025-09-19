---
title: The Python-Java boundary and how it affects query efficiency
sidebar_label: Python-Java boundary
---

This guide discusses the relationship between Python and Java in Deephaven Python queries and it how it affects query efficiency.

## From Python to Java

Deephaven, on the backend, is written almost entirely in Java.

![A breakdown of languages used in Deephaven Community Core](../assets/conceptual/dhc-languages.png)

Deephaven's Python API allows users to perform tasks by wrapping the Java operations in Python. This is accomplished via [jpy](https://pypi.org/project/jpy/), a bi-directional Python-Java bridge that can be used to embed Java code in Python programs. This guide will cover Python -> Java.

[jpy](https://pypi.org/project/jpy/) is a powerful tool taken full advantage of in Deephaven. However, Python developers typically don't want to have to write Java code. Moreover, they don't want to use Python just to invoke Java. So, the Deephaven Python API has wrapped Deephaven's powerful table operations in Python to make them PEP-compliant and easy for Python developers.

## Why does this matter?

The Python-Java boundary can be crossed numerous times throughout a single query. Crossing this boundary takes time. So, the number of times this boundary is crossed will affect the query's efficiency. This concept becomes very important when working with real-time and big data, where speed can make or break them.

Here's a screenshot showing execution times between the built-in query language [`sin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function, and [`numpy.sin`](https://numpy.org/doc/stable/reference/generated/numpy.sin.html):

![Print statements. The built-in sine function took 0.045 sect=onds, while Numpy's sine function took 1.341 seconds](../assets/conceptual/sine-timed.png)

That's a pretty significant difference in execution time. Below is the code that produces it:

```python skip-test
from deephaven import empty_table
import numpy as np
from time import time

n_tries = 100

source = empty_table(100_000).update(["X = 0.1 * i"])

start = time()
for idx in range(n_tries):
    result_builtin = source.update(["Y = sin(X)"])
end = time()
elapsed = (end - start) / n_tries

print(f"Built-in sine function - {(elapsed):.3f} seconds.")

start = time()
for idx in range(n_tries):
    result_numpy = source.update(["Y = (double)np.sin(X)"])
end = time()
elapsed = (end - start) / n_tries

print(f"NumPy sine function - {(elapsed):.3f} seconds.")
```

Why is [NumPy](https://numpy.org/) so much slower?

- When creating `result_builtin`, the query string uses built-in methods. This requires no crossings of the Python-Java boundary.
  - The [`sin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) method is built into the query language.
- When creating `result_numpy`, the query engine uses NumPy's [`sin`](https://numpy.org/doc/stable/reference/generated/numpy.sin.html) method. So, it has to cross the Python-Java boundary twice on each iteration.
  - The first time, it goes from Java to Python to calculate the sine of `X`.
  - The second time, it converts the result from Python to Java.
  - Deephaven handles data in chunks, so each of these boundary crossings happen for every chunk. There are multiple chunks in 100,000 rows.

## What's built into the query language?

Deephaven is written in Java under the hood, so all Java built-in classes are available. A list can be found [here](https://docs.oracle.com/en/java/javase/17/docs/api/allclasses-index.html). Some of the most useful classes (and their subclasses) in queries are given below:

- [java.lang](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/package-summary.html)
  - [java.lang.Math](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html) - Contains useful math functions.
  - [java.lang.Number](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Number.html) - Useful for converting `BigDecimal` and `BigInteger` to primitive types.
  - [java.lang.String](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) - The Java String. This is the data type of any Deephaven string column.
- [java.math](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/package-summary.html)
  - [java.math.BigDecimal](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) - Arbitrary precision floating point value.
  - [java.math.BigInteger](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigInteger.html) - Arbitrary precision integer value.
- [java.util](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/package-summary.html)
  - [java.util.Arrays](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Arrays.html) - Routines for searching and manipulating arrays.
  - [java.util.Collections](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Collections.html) - Routines that operate on or return collections.
  - [java.util.Random](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Random.html) - Routines to generate pseudorandom numbers.

Not only that, but Deephaven has its own classes and methods built into the query language.

<!--TODO:

## Deephaven date-times

Deephaven uses the Java date-time class. Their proper and efficient use deserves its own guide. See new guide to learn more.

-->

## How to minimize the number of boundary crossings

Efficient queries typically make minimal Python-Java boundary crossings. They all have one thing in common:

**They use methods and variables built-in to the query language.**

## The Python API under the hood

Deephaven's Python API wraps its engine, written in Java, in Python. It adds small amounts of initialization overhead all for the sake of ease of use. For example, the [`update`](../reference/table-operations/select/update.md) table operation creates a new table containing new, in-memory columns for each operation given in a list. Below is a simplified snippet of the Python source code for a Deephaven table with the `update` method.

```python skip-test
import jpy

_J_Table = jpy.get_type("io.deephaven.engine.table.Table")

class Table:
    def __init__(self, j_table: jpy.JType):
        self.j_table = jpy.cast(j_table, _J_Table)

     def update(self, formulas: Sequence[str]) -> Table:
         return Table(j_table=self.j_table.update(*formulas))
```

- The Python class `deephaven.table.Table` is a wrapper around the Java class, `io.deephaven.engine.table.Table`. This allows for a Pythonic interface for a Java method.

- `_J_Table` is jpy's reference to the Java class.
- `jpy.JType` is jpy's type for Java objects.
- `jpy.cast` ensures that `j_table` is the correct type, and ensures that jpy has the correct signatures for making calls into Java.
- `j_table.update(*formulas)` is how jpy can call into [`update`](https://deephaven.io/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...)).
- The returned result is a Python-wrapped Deephaven table.

Much of Deephaven's Python API looks like this under the hood. It serves two primary purposes: convenience and speed. The Python wrappers make using Deephaven feel Pythonic without losing the concurrency and efficiency offered by the query engine.

## Related documentation

- [Time in Deephaven](../conceptual/time-in-deephaven.md)
