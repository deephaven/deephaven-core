---
title: The Python-Java boundary and how it affects query efficiency
sidebar_label: Python-Java boundary
---

This guide discusses the relationship between Python and Java in Deephaven Python queries and how it affects query efficiency.

## From Python to Java

Deephaven, on the backend, is written almost entirely in Java.

![A breakdown of languages used in Deephaven Community Core](../assets/conceptual/dhc-languages.png)

Deephaven's Python API allows users to perform tasks by wrapping the Java operations in Python. This is accomplished via [jpy](https://pypi.org/project/jpy/), a bi-directional Python-Java bridge that can be used to embed Java code in Python programs. This guide will cover Python -> Java.

[jpy](https://pypi.org/project/jpy/) is a powerful tool taken full advantage of in Deephaven. However, Python developers typically don't want to have to write Java code. Moreover, they don't want to use Python just to invoke Java. So, the Deephaven Python API has wrapped Deephaven's powerful table operations in Python to make them PEP-compliant and easy for Python developers.

## Why does this matter?

The Python-Java boundary can be crossed numerous times throughout a single query. Crossing this boundary takes time. So, the number of times this boundary is crossed will affect the query's efficiency. This concept becomes very important when working with real-time and big data, where speed can make or break them.

Here's a screenshot showing execution times between the built-in query language [`sin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function, and [`numpy.sin`](https://numpy.org/doc/stable/reference/generated/numpy.sin.html):

![Print statements. The built-in sin function took 0.045 seconds, while NumPy's `sin` function took 1.341 seconds](../assets/conceptual/sine-timed.png)

That's a pretty significant difference in execution time. Below is the code that produces it:

```python skip-test
from deephaven import empty_table
import numpy as np
from time import time

n_tries = 100

source = empty_table(100_000).update(["X = 0.1 * ii"])

start = time()
for idx in range(n_tries):
    result_builtin = source.update(["Y = sin(X)"])
end = time()
elapsed = (end - start) / n_tries

print(f"Built-in sin function - {(elapsed):.3f} seconds.")

start = time()
for idx in range(n_tries):
    result_numpy = source.update(["Y = (double)np.sin(X)"])
end = time()
elapsed = (end - start) / n_tries

print(f"NumPy sin function - {(elapsed):.3f} seconds.")
```

Why is [NumPy](https://numpy.org/) so much slower?

- When creating `result_builtin`, the query string uses built-in methods. This requires no crossings of the Python-Java boundary.
  - The [`sin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) method is built into the query language.
- When creating `result_numpy`, the query engine uses NumPy's [`sin`](https://numpy.org/doc/stable/reference/generated/numpy.sin.html) method. So, it has to cross the Python-Java boundary twice on each iteration.
  - The first time, it goes from Java to Python to calculate the sine of `X`.
  - The second time, it converts the result from Python to Java.
  - Deephaven handles data in chunks, so each of these boundary crossings happens for every chunk. There are multiple chunks in 100,000 rows.

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

Not only that, but Deephaven has its own [classes and methods built into the query language](../how-to-guides/built-in-functions.md).

For guidance on using date-time types efficiently, see [Time in Deephaven](./time-in-deephaven.md).

## How to minimize the number of boundary crossings

Efficient queries typically make minimal Python-Java boundary crossings. They all have one thing in common:

**They use Java methods and variables built-in to the query language.**

When there is a one-to-one translation for a function, you should prefer to use the Java equivalent. If there is no Java equivalent, simply be aware of the size of your data and the number of boundary crossings. In the above example, processing 100,000 rows took an additional 13 milliseconds. If as in this case your data is not large, then 13ms is very likely an acceptable trade-off for simple development.

## Memory considerations

When using [Python user-defined functions (UDFs)](../how-to-guides/python-functions.md) in Deephaven query strings, Python memory is allocated outside the Java heap. This has important implications:

- **OOM risk** — When total process memory grows too large, the Linux Out-Of-Memory (OOM) killer may terminate processes. This can happen when the combined Java heap and Python memory exceed the available system or container memory.
- **Unbounded memory growth** — Python objects allocated during UDF execution can accumulate over time, especially in long-running or high-throughput queries. This can lead to resident memory far exceeding the configured Java heap size.

### Recommendations

To minimize memory risks when using Python UDFs:

- **Convert performance-critical UDFs to Java** — For frequently called functions, consider implementing them in Java or using built-in query language functions instead.
- **Avoid returning large Python objects in UDFs** — They can remain in Python memory for extended periods, and if not freed in a timely manner, may cause a Python `MemoryError` and crash the worker process. Instead, when possible, have UDFs return only the data needed for table columns, which are typically primitive types and text. In situations where Java is not actively garbage collecting unused table columns that store Python objects, `deephaven.gc_collect()` can be used to attempt to trigger Java GC, but keep in mind that it is advisory only.
- **Monitor resident memory** — Track total process memory, not just Java heap usage, for queries using Python UDFs.

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

- [Query string overview](../how-to-guides/query-string-overview.md)
- [Python functions in query strings](../how-to-guides/python-functions.md)
- [Built-in query language functions](../how-to-guides/built-in-functions.md)
- [Time in Deephaven](../conceptual/time-in-deephaven.md)
