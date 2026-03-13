---
title: Auto-imported functions
sidebar_label: Auto-imported functions
---

This guide lists the Java methods that are automatically imported into a Deephaven session upon startup.

These imports, which include classes, methods, and constants, are available to use inside query strings without specifying the classpath. In the following example, the query string calls [`abs`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#abs(int)), which is automatically imported upon server startup.

```python order=result,source
from deephaven import new_table
from deephaven.column import int_col

source = new_table([int_col("IntegerColumn", [1, 2, -2, -1])])

result = source.update(["Abs = abs(IntegerColumn)"])
```

![The above `source` and `result` tables displayed side-by-side](../../../../assets/reference/query-lang-demo.png)

We recommend using built-in Java methods whenever possible. Java tends to be faster than Python and using pure Java avoids [language boundary crossings](../../../../conceptual/python-java-boundary.md), resulting in better performance. Whenever a Python method has a query language equivalent, always use the query language method.

## Function categories

Here is a complete list of everything that is imported automatically into the query language when a new instance of the Python IDE is started, organized by category:

| Category                      | Description                                                                |
| ----------------------------- | -------------------------------------------------------------------------- |
| [Basic](./basic.md)           | Array manipulation, counting, null handling, and utility functions         |
| [Math](./math.md)             | Mathematical operations: abs, sum, avg, min, max, trigonometry, statistics |
| [Time](./time.md)             | Date and time utilities: parsing, formatting, arithmetic, time zones       |
| [Logic](./logic.md)           | Boolean operations: and, or, not                                           |
| [Parse](./parse.md)           | String-to-primitive parsing functions                                      |
| [Sort](./sort.md)             | Sorting functions for arrays and vectors                                   |
| [Search](./search.md)         | String matching and searching utilities                                    |
| [GUI](./gui.md)               | Color utilities and constants for table formatting                         |
| [Constants](./constants.md)   | Null values, infinity, and numeric limits                                  |
| [Java](./java.md)             | Java standard library classes (String, Integer, List, Map, etc.)           |
| [Data types](./data-types.md) | Type casting and data structure utilities                                  |

## Related documentation

- [Query language functions](../../../../how-to-guides/built-in-functions.md)
- [QueryLibraryImportsDefaults Java class](https://github.com/deephaven/deephaven-core/blob/main/engine/table/src/main/java/io/deephaven/engine/table/lang/impl/QueryLibraryImportsDefaults.java)
