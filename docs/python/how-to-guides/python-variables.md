---
title: Python variables in query strings
sidebar_label: Variables
---

The ability to use your own custom Python variables, [functions](./python-functions.md), and [classes](./python-classes.md) in Deephaven query strings is one of its most powerful features. The use of Python variables in query strings follows some basic rules, which are outlined in this guide.

## Python variables in query strings

There are three types of Python variables supported in the Deephaven Query Language (DQL). The use of variables in queries follows [query scope](./query-scope.md) rules similar to Python's [LEGB](https://realpython.com/python-scope-legb-rule/) ordering. Additionally, since the Deephaven engine is built on Java, the [Python-Java boundary](../conceptual/python-java-boundary.md) is an important consideration when using Python variables in query strings.

### Scalars

Scalars are single values, such as numbers or booleans. They can be used directly in query strings without special syntax. The following example shows the basic use of Python variables in query strings to create new columns in a table:

```python order=source,source_meta
from deephaven import empty_table

a = 4
b = 3.14
c = -1.91e7

source = empty_table(1).update(["A = a", "B = b", "C = c + 1"])
source_meta = source.meta_table
```

### Strings

Like [scalars](#scalars), Python strings can be used in [query strings](./query-string-overview.md) with no special syntax. The following example creates a table with two string columns:

```python order=source,source_meta
from deephaven import empty_table

my_first_string = "Hello, world!"
my_second_string = "Coding is fun."

source = empty_table(1).update(
    ["FirstString = my_first_string", "SecondString = my_second_string"]
)
source_meta = source.meta_table
```

### Sequences

In Python, sequences include lists, NumPy arrays, and more. These can be used in [query strings](./query-string-overview.md) as well.

```python order=source_list,source_list_meta
from deephaven import empty_table

my_list = [1, 2, 3]

source_list = empty_table(1).update("ListColumn = my_list")
source_list_meta = source_list.meta_table
```

Extracting Python sequence elements in the query language is also supported. The Deephaven engine is not able to infer data types from extracted list elements, so be sure to use [type casts](./casting.md#type-casts) to ensure the correct resultant column types.

```python order=source,source_meta
from deephaven import empty_table

my_list = [1, 2, 3]

source = empty_table(1).update(
    [
        "FirstElement = (int)my_list[0]",
        "SecondElement = (int)my_list[1]",
        "ThirdElement = (int)my_list[2]",
    ]
)
source_meta = source.meta_table
```

It's common to [use jpy](./use-jpy.md) to convert Python sequence variables to [Java primitive arrays](./java-classes.md) before using them in [query strings](./query-string-overview.md). This is often more efficient than using the Python sequence directly.

```python order=source_java_array,source_java_array_meta
from deephaven import empty_table
import jpy

my_list = [1, 2, 3]
j_my_list = jpy.array("int", my_list)

source_java_array = empty_table(1).update("ArrayColumn = j_my_list")
source_java_array_meta = source_java_array.meta_table
```

## Related documentation

- [Query scope](./query-scope.md)
- [Python functions in query strings](./python-functions.md)
- [Python classes and objects in query strings](./python-classes.md)
- [Handle PyObjects in tables](./pyobjects.md)
- [Use jpy](./use-jpy.md)
- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`meta_table`](../reference/table-operations/metadata/meta_table.md)
