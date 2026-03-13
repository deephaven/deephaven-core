---
id: groovy-variables
title: Groovy variables in query strings
sidebar_label: Variables
---

The ability to use your own custom Groovy variables, [closures](./groovy-closures.md), and [classes](./groovy-classes.md) in Deephaven query strings is one of its most powerful features. The use of Groovy variables in query strings follows some basic rules, which are outlined in this guide.

There are three types of Groovy variables supported in the Deephaven Query Language (DQL). The use of variables in queries follows Groovy's scoping rules and Deephaven's [query scope](./query-scope.md) mechanism.

## Scalars

Scalars are single values, such as numbers or booleans. They can be used directly in query strings without special syntax. The following example shows the basic use of Groovy variables in query strings to create new columns in a table:

```groovy order=source,sourceMeta
a = 4
b = 3.14
c = -1.91e7

source = emptyTable(1).update("A = a", "B = b", "C = c + 1")
sourceMeta = source.meta()
```

## Strings

Like [scalars](#scalars), Groovy strings can be used in [query strings](./query-string-overview.md) with no special syntax. The following example creates a table with two string columns:

```groovy order=source,sourceMeta
myFirstString = "Hello, world!"
mySecondString = "Coding is fun."

source = emptyTable(1).update("FirstString = myFirstString", "SecondString = mySecondString")
sourceMeta = source.meta()
```

## Sequences

In Groovy, sequences include lists, arrays, and more. These can be used in [query strings](./query-string-overview.md) as well.

```groovy order=sourceList,sourceListMeta
myList = [1, 2, 3]

sourceList = emptyTable(1).update("ListColumn = myList")
sourceListMeta = sourceList.meta()
```

Extracting Groovy sequence elements in the query language is also supported. The Deephaven engine is not able to infer data types from extracted list elements, so be sure to use [type casts](./casting.md#type-casts) to ensure the correct resultant column types.

```groovy order=source,sourceMeta
myList = [1, 2, 3]

source = emptyTable(1).update(
    "FirstElement = (int)myList[0]",
    "SecondElement = (int)myList[1]",
    "ThirdElement = (int)myList[2]",
)
sourceMeta = source.meta()
```

## Related documentation

- [Query scope](./query-scope.md)
- [Python functions in query strings](./groovy-closures.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/context/QueryScope.html)
