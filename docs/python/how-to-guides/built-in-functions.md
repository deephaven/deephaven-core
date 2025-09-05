---
title: Built-in query language functions
sidebar_label: Built-in functions
---

Like [constants](./built-in-constants.md) and [variables](./built-in-variables.md), there are many built-in functions that can be called from the query language with no additional imports or setup. These built-in functions should be used over Python or other user-defined functions in query strings for two reasons:

- They are almost always more performant.
- They handle null values gracefully.

For more on why, see [The Python-Java boundary](../conceptual/python-java-boundary.md).

## Built-in functions

Built-in functions range from simple mathematical functions to date-time arithmetic, string manipulation, and more.

The following query calculates the absolute value of a column using the built-in [`abs`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#abs(long)) function:

```python order=source
from deephaven import empty_table

source = empty_table(10).update(["X = -ii", "AbsX = abs(X)"])
```

The following query parses a column of strings containing integers into Java primitive `int` values using [`parseInt`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Parse.html#parseInt(java.lang.String)):

```python order=result,source
from deephaven import empty_table
from random import choice


def rand_string_number() -> str:
    return choice([str(item) for item in list(range(10))])


source = empty_table(10).update("StringColumn = rand_string_number()")
result = source.update("IntegerColumn = parseInt(StringColumn)")
```

The following query uses the built-in [`and`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Logic.html#and(boolean...)) function to evaluate whether or not three different columns are all `true`:

```python order=source,result
from deephaven import new_table
from deephaven.column import bool_col

source = new_table(
    [
        bool_col("A", [True, True, True]),
        bool_col("B", [True, True, False]),
        bool_col("C", [True, False, True]),
    ]
)
result = source.update(["IsOk = and(A, B, C)"])
```

Built-in functions gracefully handle null values as input. For example, the following example calls the built-in [`sin`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function on a column that contains null values:

```python order=source
from deephaven import empty_table

source = empty_table(40).update(
    ["X = (ii % 3 != 0) ? 0.1 * ii : NULL_DOUBLE", "SinX = sin(X)"]
)
```

This page does not provide a comprehensive list of all built-ins. For the complete list of functions built into the query language, see [Auto-imported functions](../reference/query-language/query-library/auto-imported-functions.md).

## Add Java classes to the query library

To use a non-default Java class in a Deephaven Query Language (DQL) query string, you must first import the class into DQL using Python. For this, Deephaven offers the [`query_library`](https://docs.deephaven.io/core/pydoc/code/deephaven.query_library.html) module.

The following example adds the [`java.net.URL`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/URL.html) class to the query library then uses it in table operations.

```python order=source
from deephaven import empty_table
from deephaven import query_library

query_library.import_class("java.net.URL")

source = empty_table(1).update(
    [
        "DeephavenUrl = new URL(`https://deephaven.io/`)",
        "Protocol = DeephavenUrl.getProtocol()",
        "Host = DeephavenUrl.getHost()",
        "Path = DeephavenUrl.getPath()",
    ]
)
```

You can also import static methods from a class into the query language to use them in table operations. The following example imports static methods from the [`java.lang.StrictMath`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/StrictMath.html) class and uses them in table operations.

> [!NOTE]
> [`import_static`](https://docs.deephaven.io/core/pydoc/code/deephaven.query_library.html#deephaven.query_library.import_static) allows you to call methods without the class name. The class name is included in the examples below because methods of the same name also exist in the [built-in](./built-in-functions.md) [`io.deephaven.function.Numeric`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html) class.

```python order=source
from deephaven import empty_table
from deephaven import query_library

query_library.import_static("java.lang.StrictMath")

source = empty_table(10).update(
    [
        "X = randomDouble(-1.67, 1.67)",
        "AcosX = StrictMath.acos(X)",
        "ExpX = StrictMath.exp(X)",
        "ExponentX = StrictMath.getExponent(X)",
    ]
)
```

## Related documentation

- [Query string overview](./query-string-overview.md)
- [Built-in constants](./built-in-constants.md)
- [Built-in variables](./built-in-variables.md)
- [Ternary conditional operator](./ternary-if-how-to.md)
- [Java objects in query strings](./java-classes.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported-functions.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
