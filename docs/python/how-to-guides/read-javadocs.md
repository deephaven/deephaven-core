---
title: Javadocs for Python users
sidebar_label: Read Javadocs
---

This guide will cover best practices for reading Javadocs as a Python user. Deephaven's query engine is written in Java, and Deephaven query strings can call Java, so a basic understanding of Javadocs and how to effectively use them can greatly benefit any Python user who wants to maximize the return from their queries.

## What are Javadocs?

[Javadoc](https://en.wikipedia.org/wiki/Javadoc) generates API-level documentation called Javadocs from Java source code. Javadocs are the primary source of documentation for Java libraries. Deephaven is no different. [Deephaven's Javadocs](/core/javadoc/) are an important resource and contain comprehensive reference material for all our core methods. When you first land on Deephaven's Javadocs, you see this:

![Deephaven Javadoc landing page with search bar and package list](../assets/how-to/javadoc-landingpage.png)

The landing page for Deephaven's Javadocs contains a top menu bar, search bar, and a list of packages. The two most important items for Deephaven Python users are (in order):

- The search bar
- The package list

Most Python developers using Deephaven will only need to use the search bar, but the package list can be helpful as well.

## Why would a Python user need to use Javadocs?

Most Deephaven Python users won't need to use Javadocs very often. However, query strings in Deephaven implement Java code _regardless_ of which API is being used. Python queries benefit greatly from a "keep Java in Java and Python in Python as often as possible" approach. Thus, users should try to minimize the amount of Python objects they use in query strings. For more information on why, see [The Python-Java boundary](../conceptual/python-java-boundary.md). Deephaven's query library has a large variety of [built-in methods](./built-in-functions.md) and attributes that users can and should take full advantage of. All of these methods can be found in the Javadocs.

## Java concepts for Python users

Each of the following subsections explores a Java concept that differs from Python. This is not a comprehensive list. For a TL;DR, see [Java vs Python](#java-vs-python).

### Naming conventions

Deephaven's Python API follows standard Python conventions outlined in [PEP 8](https://peps.python.org/pep-0008/). Deephaven's Java code, including Java code in query strings, follows [standard Java naming conventions](https://www.oracle.com/java/technologies/javase/codeconventions-namingconventions.html). The biggest key differences between the two are:

- In Python, functions and variables use `snake_case`. In Java, they use `camelCase`.
- Java also has [interfaces](https://docs.oracle.com/javase/tutorial/java/concepts/interface.html), which contain empty elements and methods. Classes can implement interfaces, populating these empty elements and methods with substance.

### Packages, interfaces, classes, and methods

The landing page contains a laundry list of Java packages. But what is a Java package? It's not much different from a Python package in that it contains Java code that that's part of an API. Packages in Java are used to group similar classes and interfaces in the same way a folder is typically used in a file system to group similar files. For instance, the [`io.deephaven.api.agg`](/core/javadoc/io/deephaven/api/agg/package-summary.html) package contains classes and interfaces that form the Java implementation of Deephaven's powerful aggregations. Deephaven's Python analogue, [`deephaven.agg`](/core/pydoc/code/deephaven.agg.html#module-deephaven.agg), is a wrapper around Java source code, so it contains many of the same functionalities.

A Java package will typically contain one or more interfaces and one or more classes. A Java class is similar to a Python class - it's a blueprint for how to build certain types of objects. For instance, the Deephaven Java class [`io.deephaven.api.agg.Aggregation`](/core/javadoc/io/deephaven/api/agg/Aggregation.html) contains the blueprints for implementing all of the different types of aggregations Deephaven has to offer. A Java interface is a bit different. For the sake of this document, just think of an interface as a Java mechanism to achieve [abstraction](https://en.wikipedia.org/wiki/Abstraction_(computer_science)), which hides implementation details from users.

Java methods live within classes. Following the previous path, [`io.deephaven.api.agg.Aggregation.AggAbsSum`](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggAbsSum(java.lang.String...)) is the blueprint for building an absolute sum aggregation. It's a method which is a member of a class, which is a member of a package.

Javadocs organize the packages, classes, interfaces, methods, and attributes in a hierarchical fashion.

### Data types

On its own, Python offers very few data types. It's one of the things that makes Python so accessible to new users - but it comes at the cost of performance. Thankfully, Python has a large variety of modules, some of which help alleviate the problem (such as [NumPy](https://numpy.org/) and its data types). For more information on this topic, see [data types in Deephaven](./data-types.md).

Java offers a variety of data types. For our purposes, they fall into two categories: [primitives](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) and [objects](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html). Primitives are what their names suggest: basic data types with no attributes. Objects, like in Python, have attributes. Objects come in an incredible variety of shapes and sizes.

In Java, primitive data types all start with a lowercase letter (e.g. `boolean`, `int`, `long`, `double`, etc.), whereas objects start with an uppercase letter (e.g. `Integer`, `Boolean`, etc.). The Java `Boolean` object simply wraps the primitive type in an object so that it has other attributes and methods.

### Method overloading

Java enables method overloading, where two distinct methods can have the same name, provided that they accept different input parameters. In Python, optional input arguments are typically used to govern a method's behavior based on different inputs. Optional inputs are not possible in Java. Deephaven's [Javadocs](https://deephaven.io/core/javadoc/io/deephaven/api/agg/Aggregation.html#AggPct(double,boolean,java.lang.String...)) have many cases of method overloading. For instance:

```java skip-test
AggPct(double percentile, boolean average, String... pairs)
AggPct(double percentile, String... pairs)
AggPct(String inputColumn, boolean average, PercentileOutput... percentileOutputs)
AggPct(String inputColumn, PercentileOutput... percentileOutputs)
```

Each `AggPct` in the block above is a different method, despite having the same name. The method that gets invoked is dependent on the number of input parameters given, as well as their type.

### Varargs

Note how, in the previous section, each overloaded method uses `...`. This is called `varargs` (short for variable arguments), and it means that an input parameter can take an arbitrary number of values. For instance, `String...` is an input data type to many of Deephaven's methods. The `...` means "zero or more". Any varargs input parameter must _always_ be the final input parameter if there is more than one input to a method. For the second overload above (`AggPct(double percentile, String... pairs)`), any of the following method calls are valid:

```java skip-test
AggPct(0.5, "Col2", "AnotherColumn", "SomeOtherColumn = Col1")
AggPct(0.2, "ColNew = ColOld")
AggPct(0.9, "A = B", "C = D", "E = F", "G = H")
AggPct(0.1)
```

## Java vs Python

To summarize all of the previous subsections, the following list provides the most important differences between Java and Python for Deephaven Python users who wish to use Javadocs:

- Java uses `camelCase` for variable and method names. Python uses `snake_case` for variable and function names.
- Java is strongly typed, whereas Python is dynamically typed. This means that, in Java, variable types are explicitly given, and cannot change without a typecast.
- Java has overloaded methods, whereas Python does not. Python makes up for this with optional arguments with default values.
- Varargs in Java are denoted by `...`, whereas in Python they're denoted by `*args` or `**kwargs`.

## Examples

### Calculate a median

Let's assume you have a table of [OHLC](https://en.wikipedia.org/wiki/Open-high-low-close_chart) data for a particular stock. Your column names are `Open`, `High`, `Low`, and `Close`, and they all contain double values. You want to calculate the median of those values. Start by searching for `median` in the search bar:

![Javadoc search results after searching for 'median'](../assets/how-to/median-javadoc-search.png)

That's a lot of results. Firstly, a class or interface isn't what we're looking for. We want a method, which is a member in a search. This search can be narrowed down further by searching instead for `median(double`, since the columns are of `double` data type:

![Refined Javadoc search results for 'median(double'](../assets/how-to/median-double-javadoc-search.png)

This search yields the correct method as the first result. It takes a variable amount of `double` values (denoted by `...`) as input. The third result, which takes a `double[]` input, is meant for array columns, which is not applicable. Deephaven's [`io.deephaven.function.Numeric`](/core/javadoc/io/deephaven/function/Numeric.html) class is automatically imported into the query language upon server startup, which means two things:

- It shows up twice in Javadoc searches, once in `io.deephaven.function.Numeric` and once in `io.deephaven.libs.GroovyStaticImports`.
- It can be used without any import statements.

Deephaven knows to use the numeric values in columns when column names are passed as input. The query engine will _automatically_ know which method to use based on the input arguments you provide. So, with this knowledge in hand, computing the median of OHLC data is easy:

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

Say, for instance, you want to test your query against some random data. Deephaven's [`io.deephaven.function.Random`](/core/javadoc/io/deephaven/function/Random.html) class contains a wide array of pseudorandom number generators, and is automatically imported into the DQL upon server startup. Looking at the Javadocs, there are two overloads for [`randomDouble`](https://deephaven.io/core/javadoc/io/deephaven/function/Random.html#randomDouble(double,double)):

```java skip-test
randomDouble(double min, double max)
randomDouble(double min, double max, int size)
```

The first method generates a single random value between `min` and `max`. The second generates `size` random values in an array between `min` and `max`. Both can be used in query strings:

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    [
        "Doubles = randomDouble(0.0, 10.0)",
        "DoubleArrays = randomDouble(-50.0, -25.0, 5)",
    ]
)
```

### Math functions

The [`io.deephaven.function.Numeric`](/core/javadoc/io/deephaven/function/Numeric.html) class contains a set of commonly used numeric functions. The link to the class's Javadoc page shows a large variety of methods that are available in query strings.

```python order=source
from deephaven import empty_table

source = empty_table(10).update(
    ["X = i - 5", "AbsX = abs(X)", "SqrtAbsX = pow(AbsX, 0.5)", "SignumX = signum(X)"]
)
```

## Key takeaways

To summarize this article, keep the following tips in mind when using Javadocs:

- Be specific when using the search bar to limit the number of results you get.
  - If you're looking for a method, look under `Members` in results.
  - Use data types when searching for methods.
- Java uses `camelCase` for methods and `PascalCase` for classes and interfaces.
- Java uses method overloading, where different methods can have the same name, but take different input parameters.
- Java is strongly typed. All input parameters to methods must be of a specific data type.
- Java `varargs` use the syntax `...`, which means zero or more inputs of a given type. This input type _must_ come last in a Java method.
- Java methods that take primitive types as input (e.g. `double`, `int`, etc.) can take columns of that type as input.

## Related documentation

- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [Javadoc](/core/javadoc/)
