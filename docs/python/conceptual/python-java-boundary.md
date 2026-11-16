---
title: The Python-Java boundary and how it affects query efficiency
---

This guide discusses the relationship between Python and Java in Deephaven Python queries and how it affects query efficiency. Calls pass between the two languages in both directions:

- Python code calls into the Java engine when you run a table operation.
- The engine calls back into Python when a [query string](../how-to-guides/query-string-overview.md) uses a Python function.

The short version: table operations you call from Python are inexpensive. A Python function inside a query string, however, can run once for every row. So prefer [built-in functions](../how-to-guides/built-in-functions.md), and write the Python calls you keep so that the engine can vectorize them.

The first section shows how the Python API wraps Java table operations. [Why crossing the boundary affects performance](#why-crossing-the-boundary-affects-performance) covers the other direction and explains why it can dominate query cost. The remaining sections cover how to reduce boundary crossings and the memory that Python functions in query strings use.

## From Python to Java

Deephaven's backend is written almost entirely in Java. Over 90% of the code in the Deephaven Community Core repository is Java.

Deephaven's Python API wraps the engine's powerful Java table operations in idiomatic Python that follows [PEP 8](https://peps.python.org/pep-0008/) conventions. Python developers don't have to write Java or call Java classes directly. The wrappers rely on [jpy](../how-to-guides/use-jpy.md), a powerful bidirectional Python-Java bridge.

The Python-Java boundary is the point where a call passes from the Python interpreter to the Java engine, or back. Each crossing takes time, because [jpy](../how-to-guides/use-jpy.md) has to convert the arguments and the return value between Python and Java types.

The [`update`](../reference/table-operations/select/update.md) table operation shows the pattern. It creates a new table containing a new, in-memory column for each formula given in a list. Below is a simplified version of the Python [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) class and its [`update`](../reference/table-operations/select/update.md) method.

```python skip-test
from __future__ import annotations

from collections.abc import Sequence

import jpy

_JTable = jpy.get_type("io.deephaven.engine.table.Table")


class Table:
    def __init__(self, j_table: jpy.JType):
        self.j_table = jpy.cast(j_table, _JTable)

    def update(self, formulas: Sequence[str]) -> Table:
        return Table(j_table=self.j_table.update(*formulas))
```

- [`deephaven.table.Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table) is the Python class that wraps the Java class [`io.deephaven.engine.table.Table`](/core/javadoc/io/deephaven/engine/table/Table.html) in a Pythonic interface.
- `_JTable` is [jpy](../how-to-guides/use-jpy.md)'s reference to the Java class.
- `jpy.JType` is the type hint the Deephaven Python API uses for a Java object.
- `jpy.cast` casts `j_table` to the Java [`Table`](/core/javadoc/io/deephaven/engine/table/Table.html) type so that [jpy](../how-to-guides/use-jpy.md) has the correct signatures for making calls into Java.
- `j_table.update(*formulas)` calls the Java [`update`](/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...)) method through [jpy](../how-to-guides/use-jpy.md).
- `Table(j_table=...)` wraps the Java result in a new Python [`Table`](/core/pydoc/code/deephaven.table.html#deephaven.table.Table), which [`update`](../reference/table-operations/select/update.md) returns.

Much of Deephaven's Python API looks like this under the hood. This thin-wrapper design serves two purposes: convenience and speed. The thin wrappers make the API feel Pythonic and add only a small overhead for each operation. The Java query engine still does the actual work, so table operations keep the concurrency and efficiency it offers.

## Why crossing the boundary affects performance

A table operation called from Python, like [`update`](../reference/table-operations/select/update.md) in the wrapper above, crosses the boundary only a few times, however many rows the table has. A [Python function called inside a query string](../how-to-guides/python-functions.md) is different. The engine calls it from Java once per row. When the engine can [vectorize](#vectorize-the-python-calls-you-keep) the call, it calls it once per chunk (a block of rows the engine processes together) instead. Either way, a single query can cross the boundary many times.

Each crossing takes time, so the number of boundary crossings affects the query's efficiency. This matters most with real-time and large data, where speed can make or break an application.

Calling Python also costs parallelism. Unless the Python build is free-threaded, the engine doesn't split a column or filter that calls Python across threads. [Parallelization](./query-engine/parallelization.md) explains how the engine parallelizes queries.

The following code times the [built-in](../how-to-guides/built-in-functions.md) [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function against [`numpy.sin`](https://numpy.org/doc/stable/reference/generated/numpy.sin.html) on 100,000 rows:

```python skip-test
from deephaven import empty_table
import numpy as np
from time import time

n_tries = 100

source = empty_table(100_000).update(["X = 0.1 * ii"])

start = time()
for _ in range(n_tries):
    result_builtin = source.update(["Y = sin(X)"])
end = time()
elapsed = (end - start) / n_tries

print(f"Built-in sine function - {elapsed:.3f} seconds.")

start = time()
for _ in range(n_tries):
    result_numpy = source.update(["Y = (double)np.sin(X)"])
end = time()
elapsed = (end - start) / n_tries

print(f"NumPy sine function - {elapsed:.3f} seconds.")
```

One run printed the following times:

```text
Built-in sine function - 0.045 seconds.
NumPy sine function - 1.341 seconds.
```

That's a significant difference: the NumPy version takes about 30 times as long.

Why is the [NumPy](https://numpy.org/) version so much slower?

- The [query string](../how-to-guides/query-string-overview.md) that creates `result_builtin` uses only the [built-in](../how-to-guides/built-in-functions.md) [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function, so evaluating it never crosses the Python-Java boundary.
- The [query string](../how-to-guides/query-string-overview.md) that creates `result_numpy` calls into Python at least twice for every row:
  - It looks up `sin` on the `np` module.
  - It calls `sin` on the value of `X`, and [jpy](../how-to-guides/use-jpy.md) converts the argument to Python and the result back to Java.

The engine also can't vectorize this call. [Vectorize the Python calls you keep](#vectorize-the-python-calls-you-keep) explains why.

## How to minimize the number of boundary crossings

Efficient queries make as few Python-Java boundary crossings as possible. There are two ways to get there:

- **Use Java methods and variables built into the query language.** When a function has a Java equivalent, use it, and that call never crosses the boundary. [What's built into the query language?](#whats-built-into-the-query-language) lists where to find them.
- **Vectorize the Python calls you keep.** When a function has no Java equivalent, write the call so the engine can run it once per chunk of rows instead of once per row. See [Vectorize the Python calls you keep](#vectorize-the-python-calls-you-keep).

### What's built into the query language?

Start with Deephaven's own [built-in query language functions](../how-to-guides/built-in-functions.md), which include the [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function from the timing example.

Not every function in the Deephaven Python API is built into the query language. The date-time conversion helpers in [`deephaven.time`](/core/pydoc/code/deephaven.time.html#module-deephaven.time) are Python functions, so calling them in a [query string](../how-to-guides/query-string-overview.md) crosses the boundary on every call. For built-in alternatives, see [Time in Deephaven](./time-in-deephaven.md#performance-and-the-python-java-boundary).

Because Deephaven is written in Java, a [query string](../how-to-guides/query-string-overview.md) can also use any public Java class on the server's classpath. See [Java objects and classes in query strings](../how-to-guides/java-classes.md).

- [Query strings](../how-to-guides/query-string-overview.md) can use classes in `java.lang` and `java.util` without importing them.
- To use a class from another package, such as `java.math.BigDecimal`, write its fully qualified name or import it with [`query_library.import_class`](/core/pydoc/code/deephaven.query_library.html#deephaven.query_library.import_class).

The [Java SE API class index](https://docs.oracle.com/en/java/javase/17/docs/api/allclasses-index.html) lists every class in the Java SE API. These packages and classes are especially useful in queries:

- [`java.lang`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/package-summary.html)
  - [`java.lang.Math`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html) — Standard math functions. Most of them, including `cbrt`, `hypot`, and `log10`, are also [built-in query language functions](../reference/query-language/query-library/auto-imported/math.md) that handle Deephaven null values, so prefer the built-in. Call `Math` directly only for a method with no built-in, such as `fma`.
  - [`java.lang.Number`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Number.html) — Methods for converting [`BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) and [`BigInteger`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigInteger.html) values to primitive types.
  - [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) — Methods such as `contains`, `startsWith`, and `substring` that you can call directly on a non-null `String` column value.
- [`java.math`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/package-summary.html)
  - [`java.math.BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) — Arbitrary-precision signed decimal values.
  - [`java.math.BigInteger`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigInteger.html) — Arbitrary-precision integer values.
- [`java.util`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/package-summary.html)
  - [`java.util.Arrays`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Arrays.html) — Methods for searching and manipulating arrays.
  - [`java.util.Collections`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Collections.html) — Methods that operate on or return collections.
  - [`java.util.Random`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/Random.html) — A seeded pseudorandom generator. For unseeded values, [built-in functions](../reference/query-language/query-library/auto-imported/math.md) such as `random` and `randomInt` need no setup.

### Vectorize the Python calls you keep

When a query must call a [Python function](../how-to-guides/python-functions.md), write the call so the engine can vectorize it. A vectorized call crosses into Python once per chunk of rows instead of once per row.

This is not NumPy vectorization:

- Inside each chunk, Deephaven still calls the Python function once per value, even when the function is a [NumPy ufunc](https://numpy.org/doc/stable/reference/ufuncs.html) (a function that NumPy applies element by element to an array) such as `numpy.sin`.
- Vectorizing changes only how many times the engine crosses the boundary to make those calls.
- A [Numba](../how-to-guides/use-numba.md) `@vectorize` function is the exception: Deephaven passes it the whole chunk as arrays.

To be vectorized, a call must meet several requirements, including these:

- It is a plain function call, not one made through a module or object.
- Its result is not explicitly cast.
- Its result is not used inside a larger expression.
- Each argument is a column, a variable, or a constant, not an expression such as `X + 1`.
- It passes exactly as many arguments as the function has parameters, so it doesn't use `*args` or leave out parameters that have defaults.

The `np.sin(X)` call in the timing example fails the first two requirements: it goes through the `np` module, and its result is cast to `double`.

To make the call vectorizable, wrap `np.sin` in a plain Python function with type hints. The `float` return type hint tells Deephaven the result type, so the query string doesn't need a cast:

```python order=result_vectorized,result_numpy
from deephaven import empty_table
import numpy as np

source = empty_table(100_000).update(["X = 0.1 * ii"])


def np_sin(x: float) -> float:
    return np.sin(x)


# Not vectorized: the call goes through the np module, and its result is cast.
result_numpy = source.update(["Y = (double)np.sin(X)"])

# Vectorized: a plain function call with a column argument and no cast.
result_vectorized = source.update(["Y = np_sin(X)"])
```

Both tables hold the same values. The engine crosses into Python far fewer times for `result_vectorized`. Inside each chunk, `np_sin` still runs once per row, so it remains slower than the built-in [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)).

Vectorizing reduces the number of boundary crossings, but it doesn't remove them. Whether the overhead of the Python calls you keep is acceptable depends on your data size and update frequency.

## Memory considerations

[Python user-defined functions (UDFs)](../how-to-guides/python-functions.md) are Python functions called in [query strings](../how-to-guides/query-string-overview.md). They cost memory as well as time:

- Each call runs in Python, so the objects it creates live in Python's memory, outside the Java heap.
- When a UDF returns a Python object that the engine stores in a column, the Java side holds a reference to it. Python can't free that object until the JVM garbage collects the reference.

Resident memory is the physical memory the whole process occupies, including both the Java heap and Python's memory. Python UDFs can make resident memory grow, and that growth has a cost:

- **Memory growth beyond the Java heap** — Python objects allocated during [UDF](../how-to-guides/python-functions.md) execution can accumulate over time, especially in long-running or high-throughput queries, so resident memory can grow far beyond the configured Java heap size.
- **Out-of-memory (OOM) risk** — When resident memory exceeds the available system or container memory, the Linux OOM killer may terminate processes.

### Recommendations

To minimize memory risks when using [Python UDFs](../how-to-guides/python-functions.md):

- **Convert performance-critical [UDFs](../how-to-guides/python-functions.md) to Java** — Implement frequently called functions in Java, or use [built-in query language functions](../how-to-guides/built-in-functions.md) instead. To call your own Java code, [install it as a Java package](../how-to-guides/install-and-use-java-packages.md) and [call its classes in query strings](../how-to-guides/java-classes.md). See also [How to minimize the number of boundary crossings](#how-to-minimize-the-number-of-boundary-crossings).
- **Avoid returning large Python objects in [UDFs](../how-to-guides/python-functions.md)** — Large Python objects can stay in Python memory for a long time. If Python doesn't free them soon enough, Python can raise a [`MemoryError`](https://docs.python.org/3/library/exceptions.html#MemoryError) that fails the query, or resident memory can grow until the OOM killer terminates the Deephaven server process. Where possible, have UDFs return only the data the table columns need, which is typically primitive values and strings.
- **Trigger garbage collection when needed** — If the JVM hasn't yet freed unused table columns that hold Python objects, call [`deephaven.garbage_collect`](../reference/garbage-collect.md) to run Python [garbage collection](../how-to-guides/performance/garbage-collection.md) and request a JVM garbage collection. The JVM treats the request as advisory, so the effect is not guaranteed.
- **Monitor resident memory** — Track total process memory, not just Java heap usage, for queries using [Python UDFs](../how-to-guides/python-functions.md).

## Related documentation

- [Time in Deephaven](./time-in-deephaven.md)
- [Parallelization](./query-engine/parallelization.md)
- [Query string overview](../how-to-guides/query-string-overview.md)
- [Python functions in query strings](../how-to-guides/python-functions.md)
- [Built-in query language functions](../how-to-guides/built-in-functions.md)
- [Java objects and classes in query strings](../how-to-guides/java-classes.md)
- [Install and use Java packages](../how-to-guides/install-and-use-java-packages.md)
- [Use jpy](../how-to-guides/use-jpy.md)
- [Use Numba in Deephaven queries](../how-to-guides/use-numba.md)
- [Run garbage collection](../how-to-guides/performance/garbage-collection.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`garbage_collect`](../reference/garbage-collect.md)
- [`update`](../reference/table-operations/select/update.md)
- [`query_library.import_class` Pydoc](/core/pydoc/code/deephaven.query_library.html#deephaven.query_library.import_class)
- [`Table` Pydoc](/core/pydoc/code/deephaven.table.html#deephaven.table.Table)
- [`deephaven.time` Pydoc](/core/pydoc/code/deephaven.time.html#module-deephaven.time)
- [`jpy` Pydoc](/core/pydoc/code/jpy.html)
