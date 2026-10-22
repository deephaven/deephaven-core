---
title: Javadocs for Python users
---

This guide covers best practices for reading Javadocs as a Python user. Deephaven's query engine is written in Java, and Deephaven [query strings](./query-string-overview.md) can call Java, so knowing how to read Javadocs helps you find the Java methods you can call in your queries and get the most out of them.

## What are Javadocs?

[Javadoc](https://en.wikipedia.org/wiki/Javadoc) generates API-level documentation called Javadocs from Java source code. Javadocs are the primary source of documentation for Java libraries. [Deephaven's Javadocs](/core/javadoc/) document Deephaven's Java classes and methods. When you first land on Deephaven's Javadocs, you see this:

![Deephaven Javadoc landing page with search bar and package list](../assets/how-to/javadoc-landingpage.png)

The landing page contains a top menu bar, a search bar, and a list of packages. Most Python users only need the search bar. [Calculate a median](#calculate-a-median) shows how to search for a method with it. The package list is useful for browsing, and [Packages, interfaces, classes, and methods](#packages-interfaces-classes-and-methods) explains what it holds.

## Why would a Python user need to use Javadocs?

You don't need the Javadocs for ordinary Python API calls such as [`update`](../reference/table-operations/select/update.md) or [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md). You need them when you write [query strings](./query-string-overview.md): the engine compiles them to Java _regardless_ of which API you use, so the functions built into them are Java methods.

Those built-in methods are also the fast choice. A query performs best when it keeps Java in Java and Python in Python, so prefer a [built-in method](./built-in-functions.md) over a [Python function](./python-functions.md) in a [query string](./query-string-overview.md). That matters most on real-time and large tables, where a formula runs again for every row that's added or whose inputs change. For why, see [The Python-Java boundary](../conceptual/python-java-boundary.md).

Deephaven's query library includes many [built-in methods](./built-in-functions.md) and fields that you can and should take full advantage of, and the Javadocs document all of them. A field is Java's term for a variable that belongs to a class or object, like a Python attribute.

## Java concepts for Python users

Each of the following subsections explains a Java concept you meet when reading a Javadoc page. This is not a comprehensive list. The first subsection covers packages, interfaces, classes, and methods, which organize Javadoc pages much as modules and classes organize Python code. The other four concepts differ most from Python, and the following table summarizes them:

| Concept            | Python                                 | Java                                                                                       |
| ------------------ | -------------------------------------- | ------------------------------------------------------------------------------------------ |
| Naming conventions | `snake_case`                           | `camelCase`                                                                                |
| Data types         | Dynamically typed                      | Statically typed. Every variable and parameter has a declared type, fixed at compile time. |
| Method overloading | Optional arguments with default values | Method overloading                                                                         |
| Varargs            | `*args`                                | Varargs (`...`)                                                                            |

### Packages, interfaces, classes, and methods

The [Javadoc landing page](/core/javadoc/) contains a list of Java packages. But what is a Java package? It's not much different from a Python package in that it contains Java code that's part of an API. Java uses packages to group similar classes and interfaces, much as a file system uses folders to group similar files.

For instance, the [`io.deephaven.api.agg`](/core/javadoc/io/deephaven/api/agg/package-summary.html) package contains the classes and interfaces of Deephaven's Java API for [aggregations](../conceptual/aggregations.md). Deephaven's Python analogue, [`deephaven.agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg), wraps that Java code, so it offers many of the same [aggregations](../conceptual/aggregations.md).

A Java package typically contains one or more interfaces and one or more classes. A Java class is similar to a Python class: a blueprint for building a certain type of object.

A Java [interface](https://docs.oracle.com/javase/tutorial/java/concepts/interface.html) mostly declares methods without implementing them. Classes that implement the interface supply the method bodies. Interfaces are a way to achieve [abstraction](https://en.wikipedia.org/wiki/Abstraction_(computer_science)), which hides implementation details from users.

Interfaces can also define static methods directly. A static method belongs to the class or interface itself rather than to an object, like a Python `@staticmethod`, so you call it without creating an object first. For instance, the Deephaven Java interface [`io.deephaven.api.agg.Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) defines a static factory method for every [aggregation](../conceptual/aggregations.md) type Deephaven offers.

Java methods live within classes and interfaces. Continuing the [`Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) example, [`io.deephaven.api.agg.Aggregation.AggAbsSum`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggAbsSum(java.lang.String...)) is a static method that creates and returns an [absolute sum aggregation](../reference/table-operations/group-and-aggregate/AggAbsSum.md). It's a member of an interface, which is a member of a package.

Javadocs follow the same hierarchy: each package page lists its interfaces and classes, and each interface or class page lists its fields, constructors, and methods.

### Naming conventions

Deephaven's Python API follows standard Python conventions outlined in [PEP 8](https://peps.python.org/pep-0008/). Deephaven's Java code, including Java code in [query strings](./query-string-overview.md), follows [standard Java naming conventions](https://www.oracle.com/java/technologies/javase/codeconventions-namingconventions.html). The naming rules that matter most when reading Javadocs are:

- In Python, functions and variables use `snake_case`. In Java, they use `camelCase`.
- Java classes and interfaces use `PascalCase`, which capitalizes the first letter of every word (for example, [`Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) or [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html)). Python classes follow the same rule.
- Some Deephaven factory methods break this convention. The [static methods](#packages-interfaces-classes-and-methods) on [`Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html), such as [`AggPct`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,boolean,java.lang.String...)) and [`AggAbsSum`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggAbsSum(java.lang.String...)), start with a capital letter.

Because of these exceptions, don't rely on case alone to tell a method from a class. Javadoc search lists methods under **Members**, and each class page lists its methods in a method summary.

### Data types

Java is statically typed, whereas Python is dynamically typed. In Java, every variable and method parameter has a declared type that is fixed at compile time, which is why every parameter in a Javadoc method signature lists its type.

Java data types fall into two categories, [primitives](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) and [objects](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html). A type name in a Javadoc signature usually tells them apart by case:

- Primitives are basic types with no fields or methods. Their names start with a lowercase letter, such as `boolean`, `int`, `long`, and `double`.
- Objects, like Python objects, have fields and methods. Their class names start with an uppercase letter, such as `Integer` and `Boolean`.

Arrays are the exception: they are objects even when the element type is a primitive, as in `double[]` or the [varargs](#varargs) `double...`.

Each primitive has a matching wrapper class that holds the value as an object, such as `Integer` for `int` and `Boolean` for `boolean`. Javadoc pages often list a separate method for each. For example, [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html) has both [`median(double...)`](/core/javadoc/io/deephaven/function/Numeric.html#median(double...)), which takes `double` values, and [`median(Double[])`](/core/javadoc/io/deephaven/function/Numeric.html#median(java.lang.Double%5B%5D)), which takes an array of `Double` objects. The [median example](#calculate-a-median) uses the first. [Method overloading](#method-overloading) explains how Java tells them apart.

Python has only a few numeric types, such as `int` and `float`. Python's `int` has no fixed size, and its `float` is always 64 bits, like Java's `double`. [NumPy](https://numpy.org/) adds more sized types, such as `int32` and `float32`, that line up with the other Java primitives. For how Deephaven maps Java types to Python and NumPy types, see [data types in Deephaven](./data-types.md).

### Method overloading

Java supports method overloading: two methods can share a name if they accept different parameters. Java has no optional arguments, so overloading fills the role that optional arguments with defaults play in Python.

Deephaven's Javadocs contain many overloaded methods. For instance, the [`Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) interface has four overloads of [`AggPct`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,boolean,java.lang.String...)), which builds the same kind of percentile aggregation as Python's [`agg.pct`](../reference/table-operations/group-and-aggregate/AggPct.md). You don't call [`AggPct`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,boolean,java.lang.String...)) from Python, but its signatures show the patterns you see on any Javadoc page:

```java skip-test
AggPct(double percentile, boolean average, String... pairs)
AggPct(double percentile, String... pairs)
AggPct(String inputColumn, boolean average, PercentileOutput... percentileOutputs)
AggPct(String inputColumn, PercentileOutput... percentileOutputs)
```

Each [`AggPct`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,boolean,java.lang.String...)) in the block above is a different method, despite having the same name. Java picks which method to call from the number and types of the arguments.

### Varargs

Java calls a parameter that accepts a variable number of arguments of one type a varargs parameter, short for variable arguments. Its type ends in `...`, as in the `String... pairs` parameter of the [`AggPct`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,java.lang.String...)) overloads shown in [Method overloading](#method-overloading). When reading a varargs signature, keep these rules in mind:

- The `...` means "zero or more".
- A varargs parameter must be the method's last parameter.
- Varargs correspond to Python's `*args`. Java has no equivalent of `**kwargs`.

For the overload [`AggPct(double percentile, String... pairs)`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,java.lang.String...)), each of the following Java calls is valid:

```java skip-test
Aggregation.AggPct(0.5, "Col2", "AnotherColumn", "SomeOtherColumn = Col1");
Aggregation.AggPct(0.2, "ColNew = ColOld");
Aggregation.AggPct(0.9, "A = B", "C = D", "E = F", "G = H");
```

Java's varargs syntax allows a call with no values, but a method can still throw an error at runtime when it receives none. [`AggPct`](/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,java.lang.String...)) throws an `IllegalArgumentException` if you pass no column name pairs.

The signatures of methods you call in [query strings](./query-string-overview.md) read the same way. The [median example](#calculate-a-median) calls the varargs overload [`median(double...)`](/core/javadoc/io/deephaven/function/Numeric.html#median(double...)).

## Examples

Each example starts from a goal, reads the signatures of the Javadoc method that meets it, and calls that method in a [query string](./query-string-overview.md). The first example shows how to find the method with the Javadoc search bar. The others start from a class that query strings import by default.

### Calculate a median

Let's assume you have a table of [OHLC](https://en.wikipedia.org/wiki/Open-high-low-close_chart) data for a particular stock. Your column names are `Open`, `High`, `Low`, and `Close`, and they all contain `double` values. You want to calculate the median of the four values in each row. Start by searching for `median` in the search bar:

![Javadoc search results after searching for 'median'](../assets/how-to/median-javadoc-search.png)

That's a lot of results. Classes and interfaces aren't what you want here. Methods, along with fields, appear under the **Members** heading in the search results. To narrow the search further, include the parameter type. The columns hold `double` values, so search for `median(double`:

![Refined Javadoc search results for 'median(double'](../assets/how-to/median-double-javadoc-search.png)

The first result, [`median(double...)`](/core/javadoc/io/deephaven/function/Numeric.html#median(double...)) in the [`io.deephaven.function.Numeric`](/core/javadoc/io/deephaven/function/Numeric.html) class, is the one you want: it takes any number of `double` values. The [`Double[]`](/core/javadoc/io/deephaven/function/Numeric.html#median(java.lang.Double%5B%5D)) and [`DoubleVector`](/core/javadoc/io/deephaven/vector/DoubleVector.html) [overloads](#method-overloading) further down each take a single array or vector, which isn't what this table holds.

Each [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html) row is followed by a matching [`io.deephaven.libs.GroovyStaticImports`](/core/javadoc/io/deephaven/libs/GroovyStaticImports.html) entry. That class is a generated wrapper that Groovy scripts use, and its entries only link back to [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html), so open the [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html) entry to read the documentation.

[Query strings](./query-string-overview.md) import [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html) by default, so you can call its methods without an import statement.

When you pass column names to a method in a [query string](./query-string-overview.md), the engine passes the values from each row and picks the [overload](#method-overloading) that matches their types. With this knowledge in hand, computing the median of OHLC data is easy. The following example creates the data with [`new_table`](../reference/table-operations/create/newTable.md) and adds a `MedianOHLC` column with [`update`](../reference/table-operations/select/update.md):

```python order=source,result
from deephaven.column import double_col
from deephaven import new_table

source = new_table(
    [
        double_col("Open", [18.5, 18.9, 17.4, 19.0, 18.8]),
        double_col("High", [19.0, 19.7, 18.0, 19.5, 19.1]),
        double_col("Low", [18.0, 18.2, 17.0, 18.5, 18.3]),
        double_col("Close", [18.6, 19.1, 17.1, 19.3, 18.4]),
    ]
)

result = source.update(["MedianOHLC = median(Open, High, Low, Close)"])
```

### Generate pseudorandom numbers

Say you want to test your query against some random data. Deephaven's [`io.deephaven.function.Random`](/core/javadoc/io/deephaven/function/Random.html) class contains many pseudorandom number generators. [Query strings](./query-string-overview.md) import it by default, so you can call its methods without an import statement. The Javadocs list two [overloads](#method-overloading) of [`randomDouble`](/core/javadoc/io/deephaven/function/Random.html#randomDouble(double,double)):

```java skip-test
randomDouble(double min, double max)
randomDouble(double min, double max, int size)
```

The first method generates a single random value that is at least `min` and less than `max`. The second generates an array of `size` random values in the same range. The following example calls both in an [`update`](../reference/table-operations/select/update.md) on an [`empty_table`](../reference/table-operations/create/emptyTable.md):

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Doubles = randomDouble(0.0, 10.0)",
        "DoubleArrays = randomDouble(-50.0, -25.0, 5)",
    ]
)
```

### Apply math functions to an integer column

Say you want the absolute value and sign of an `int` column, and the square root of its absolute value. [`io.deephaven.function.Numeric`](/core/javadoc/io/deephaven/function/Numeric.html), the class from the [median example](#calculate-a-median), lists these and many more common math functions, all available in [query strings](./query-string-overview.md). Most have a separate [overload](#method-overloading) for each numeric input type. For example, [`sqrt(int)`](/core/javadoc/io/deephaven/function/Numeric.html#sqrt(int)) takes an `int` and returns a `double`, while [`abs(int)`](/core/javadoc/io/deephaven/function/Numeric.html#abs(int)) and [`signum(int)`](/core/javadoc/io/deephaven/function/Numeric.html#signum(int)) both return an `int`:

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    ["X = i - 5", "AbsX = abs(X)", "SqrtAbsX = sqrt(AbsX)", "SignumX = signum(X)"]
)
```

## Key takeaways

Keep the following tips in mind when using Javadocs:

- Be specific when using the search bar to limit the number of results you get.
  - If you're looking for a method, look under **Members** in results.
  - Use data types when searching for methods.
- In a [query string](./query-string-overview.md), you can pass a column name where a Java method expects a primitive such as `double` or `int`. The engine passes the column's value from each row and picks the [overload](#method-overloading) that matches its type.
- For how Java's naming, typing, overloading, and varargs differ from Python's, see the table in [Java concepts for Python users](#java-concepts-for-python-users).

## Related documentation

- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [Aggregations in Deephaven](../conceptual/aggregations.md)
- [Built-in query language functions](./built-in-functions.md)
- [Data types in Deephaven and Python](./data-types.md)
- [Java objects and classes in query strings](./java-classes.md)
- [Python functions in query strings](./python-functions.md)
- [Query string overview](./query-string-overview.md)
- [`double_col`](../reference/table-operations/create/doubleCol.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`update`](../reference/table-operations/select/update.md)
- [`agg_by`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`agg.pct`](../reference/table-operations/group-and-aggregate/AggPct.md)
- [`agg.abs_sum`](../reference/table-operations/group-and-aggregate/AggAbsSum.md)
- [`deephaven.agg` Pydoc](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg)
- [`Aggregation` Javadoc](/core/javadoc/io/deephaven/api/agg/Aggregation.html)
- [`Numeric` Javadoc](/core/javadoc/io/deephaven/function/Numeric.html)
- [`Random` Javadoc](/core/javadoc/io/deephaven/function/Random.html)
