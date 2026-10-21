---
title: Groovy closures in queries
---

You can use Groovy closures in Deephaven queries. This guide explains:

- What a closure is
- How to call one in a [query string](./query-string-overview.md)
- When to use a [built-in function](./built-in-functions.md) or a direct Java method call instead
- How to pass tables to a closure
- How to import closures from Groovy files on the classpath

A [closure](https://docs.groovy-lang.org/latest/html/api/groovy/lang/Closure.html) is Groovy's way to write a user-defined function. It lets you package a calculation once and reuse it. In a [query string](./query-string-overview.md), Deephaven calls the closure for every row. In a script, you can call it on whole tables.

On a [ticking table](../conceptual/table-types.md), the column stays current as new data arrives. In each update cycle, the engine calls the closure again for added rows and for rows whose input columns changed.

In Groovy, you write a closure inside curly braces (`{}`). The following closure, `f`, adds two integer values:

```groovy test-set=1
f = { int a, int b -> a + b }
```

A closure can take any number of parameters but returns only one value. To return several values, wrap them in a single object, such as a list or an array.

## Call a closure in a query string

You can call closures in [query strings](./query-string-overview.md). A [query string](./query-string-overview.md) can call `f`, the closure we defined above, because `f` is assigned at the top level of the script without `def`. That puts `f` in the [query scope](./query-scope.md). A method declared with `def myMethod(int a) { ... }` is not in the [query scope](./query-scope.md#script-level-functions), so assign a closure to a variable instead.

The query below calls `f` twice. It computes `Z` with a plain call and `ZInt` with an `(int)` [cast](./casting.md#groovy-closures):

```groovy test-set=1 order=source,sourceMeta
source = emptyTable(10).update("X = i", "Y = 2 * i", "Z = f(X, Y)", "ZInt = (int)f(X, Y)")
sourceMeta = source.meta()
```

A closure called in a [query string](./query-string-overview.md) returns [`Object`](../reference/query-language/types/objects.md), whatever types the closure declares. The reason is how the engine makes the call:

- In a [query string](./query-string-overview.md), `f(X, Y)` is shorthand for `f.call(X, Y)`.
- [`call`](https://docs.groovy-lang.org/latest/html/api/groovy/lang/Closure.html) is the generic method every closure has, and it returns [`Object`](../reference/query-language/types/objects.md).

In the example above, `sourceMeta` shows that `Z` is an [`Object`](../reference/query-language/types/objects.md) column, even though `f` adds two integers. The `(int)` [cast](./casting.md#groovy-closures) makes `ZInt` an `int` column.

Cast the result whenever you need a typed column, for example to do arithmetic on it or [aggregate](./dedicated-aggregations.md) it. For more about casts, see [Casting](./casting.md#groovy-closures).

> [!TIP]
> Declare the expected argument types when defining a closure that [query strings](./query-string-overview.md) call. This documents the input the closure expects. For instance, `f` declares `int a, int b` rather than `a, b`.

## Prefer built-in functions when available

One thing to consider before you write a closure to call from a [query string](./query-string-overview.md) is:

> Can I perform the same operation using a [built-in query language function](./built-in-functions.md)?

For instance, the following closure calls [`Math.sin`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html#sin(double)) from Java's [`java.lang.Math`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html) class. The first row of `X` holds a null value, [`NULL_DOUBLE`](../reference/query-language/types/nulls.md):

```groovy test-set=2 order=source
sine = { double x -> Math.sin(x) }

source = emptyTable(100).update("X = (ii == 0) ? NULL_DOUBLE : 0.1 * ii", "Y = (double)sine(X)")
```

Deephaven has a built-in [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function that you can call from [query strings](./query-string-overview.md) with no imports. It handles `null` values. For example, [`sin(NULL_DOUBLE)`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) returns [`NULL_DOUBLE`](../reference/query-language/types/nulls.md). By contrast, [`Math.sin`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html#sin(double)) returns `-0.004961954789184062` for the same input, which is not what we're looking for.

The following query adds a column that uses the built-in [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)). In the first row, `YBuiltIn` is null, while `Y` holds the value that [`Math.sin`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html#sin(double)) returned:

```groovy test-set=2 order=result
result = source.update("YBuiltIn = sin(X)")
```

> [!NOTE]
> Closures used in [query strings](./query-string-overview.md) should perform null checks on their arguments, for example, with [`isNull`](../reference/query-language/types/nulls.md). A closure that doesn't check for nulls treats a [Deephaven null](../reference/query-language/types/nulls.md) as an ordinary value and returns a meaningless result, as the [`Math.sin`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html#sin(double)) example above shows.

The following closure checks for nulls first and returns [`NULL_DOUBLE`](../reference/query-language/types/nulls.md) for a null input:

```groovy test-set=2 order=resultSafe
safeSine = { double x -> isNull(x) ? NULL_DOUBLE : Math.sin(x) }

resultSafe = result.update("YSafe = (double)safeSine(X)")
```

In the first row, `YSafe` is null, like `YBuiltIn`.

Familiarity with [Deephaven's built-in functions](./built-in-functions.md) can speed up your workflow. You can call a large number of them from [query strings](./query-string-overview.md) without adding imports or changing the classpath. For more information and a complete list of what's available, see [auto-imported functions in Deephaven](../reference/query-language/query-library/auto-imported/index.md) and [built-in query language functions](./built-in-functions.md).

## Call Java methods without a closure

When no [built-in function](./built-in-functions.md) fits, a [query string](./query-string-overview.md) can still call a public Java method directly, without wrapping it in a closure.

For example, the `sine` closure above only wraps [`Math.sin`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html#sin(double)), so a [query string](./query-string-overview.md) could call `Math.sin(X)` directly. The direct call has one advantage and one limit:

- The result is a `double` column with no [cast](./casting.md), because the engine sees the declared return type of [`Math.sin`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html#sin(double)).
- Null inputs still give the same meaningless value, so the built-in [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) is still the better choice there.

How a [query string](./query-string-overview.md) names the class depends on whether it is auto-imported:

- For an auto-imported class, such as [`Math`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Math.html) from the `java.lang` package, use the class's simple name.
- For any other class, use its fully qualified name, or first add it to the [query library](./java-classes.md#import-java-classes).

[Java objects and classes in query strings](./java-classes.md#import-java-classes) describes which classes are auto-imported and shows how to add others.

Neither [`java.net.URLEncoder`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/URLEncoder.html) nor [`java.nio.charset.StandardCharsets`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/nio/charset/StandardCharsets.html) is auto-imported. The following example therefore writes out both class names in full:

```groovy order=result
result = emptyTable(2).update("Encoded = java.net.URLEncoder.encode(`Deephaven docs`, java.nio.charset.StandardCharsets.UTF_8)")
```

Both rows of `Encoded` hold `Deephaven+docs`, the URL-encoded form of the string.

To call methods from a Java library that Deephaven does not include, add its JAR to the classpath first. For details, see [Install and use Java packages](./install-and-use-java-packages.md).

## Pass tables to a closure

Sometimes you need to apply the same operations to many tables, for example the same [sums](../reference/table-operations/group-and-aggregate/AggSum.md) and [averages](../reference/table-operations/group-and-aggregate/AggAvg.md) grouped by a key column. Groovy closures can take tables as input and return tables as output, so you can define those operations once and call the closure on each table. Some Deephaven operations, such as the [partitioned table](./partitioned-tables.md) operations described at the end of this section, require this pattern.

The following example uses the closure `doAgg` to perform [multiple aggregations](./combined-aggregations.md) on a table. The same closure can be called on any table that has a `Letter` column and numeric `X` and `Y` columns.

```groovy order=result,result2,source,source2
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggAvg

source = emptyTable(20).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomDouble(0.0, 10.0)")

doAgg = { t -> t.aggBy([AggSum("SumX = X", "SumY = Y"), AggAvg("AvgX = X", "AvgY = Y")], "Letter") }

result = doAgg(source)

source2 = emptyTable(10).update("Letter = (i % 3 == 0) ? `A` : `B`", "X = 10 * i", "Y = randomDouble(0.0, 1.0)")

result2 = doAgg(source2)
```

`doAgg` runs the same [aggregations](./combined-aggregations.md) on `source` and `source2`, so `result` and `result2` have the same columns.

> [!NOTE]
> Table operations return new tables rather than modifying the source table. A closure that applies operations to a table has no effect unless it returns the resulting table. A Groovy closure returns the value of its last expression, so `doAgg` returns the result of [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md) without a `return` statement.

[Partitioned table](./partitioned-tables.md) operations such as [`transform`](../reference/table-operations/partitioned-tables/transform.md) and [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md) take a closure that receives tables and returns a table. When that closure uses operations that compile [formulas](./formulas.md) or use the [query scope](./query-scope.md), it must run them inside an [execution context](../conceptual/execution-context.md). The [Transform section of the partitioned tables guide](./partitioned-tables.md#transform) shows how.

## Import closures from Groovy files

It's common in Groovy to keep custom closures in separate Groovy files, organized in package folders. To use them in Deephaven, put the top folder of that package tree on the server's classpath. You do that with the [`EXTRA_CLASSPATH`](./configuration/configure-production-application.md#environment-variables) environment variable in your [`docker-compose.yml`](../getting-started/docker-install.md) file.

The classpath entry must be a path inside the container, so the compose file below mounts the host's `./data` directory at `/data`. For more about mounts, see [Docker volumes](../conceptual/docker-data-volumes.md).

For example, say we want to use closures from a class in `/data/groovyscripts/com/example/MyClass.groovy`:

```groovy skip-test
package com.example

class MyClass {
    static Closure addOne = { int x -> x + 1 }
}
```

A Groovy file loaded from the classpath doesn't get the imports that the Deephaven console adds automatically. Import every Deephaven class or static method the file uses, as the `doAgg` example does with [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md) and [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md).

To make that class available:

- Add `/data/groovyscripts` to [`EXTRA_CLASSPATH`](./configuration/configure-production-application.md#environment-variables).
- Keep the default entry, `/apps/libs/*`. The [Deephaven Docker images](../getting-started/docker-install.md) set it so that they load JARs mounted at `/apps/libs`.

The resulting `docker-compose.yml` looks like this:

```yaml
services:
  deephaven:
    image: ghcr.io/deephaven/server-slim:latest
    ports:
      - 10000:10000
    volumes:
      - ./data:/data
    environment:
      - EXTRA_CLASSPATH=/data/groovyscripts:/apps/libs/*
      - START_OPTS=-Xmx4g
```

Now, from the IDE, we can import the class that contains the closure we want to call:

```groovy skip-test
import com.example.MyClass
```

The `import` statement makes `MyClass` available to your Groovy script, but not to [query strings](./query-string-overview.md), because the query engine compiles them separately.

To call one of its closures in a [query string](./query-string-overview.md), assign the closure to a top-level variable without `def`. The variable is then in the [query scope](./query-scope.md), as `f` is in [Call a closure in a query string](#call-a-closure-in-a-query-string):

```groovy skip-test
addOne = MyClass.addOne

result = emptyTable(5).update("X = i", "Y = (int)addOne(X)")
```

To use `MyClass` itself in a [query string](./query-string-overview.md), rather than one of its closures, see [Groovy classes and objects in query strings](./groovy-classes.md#make-a-groovy-class-available-to-query-strings).

## Related documentation

- [Docker volumes](../conceptual/docker-data-volumes.md)
- [Execution context](../conceptual/execution-context.md)
- [Docker install](../getting-started/docker-install.md)
- [Query string overview](./query-string-overview.md)
- [Formulas](./formulas.md)
- [Dedicated aggregations](./dedicated-aggregations.md)
- [Create a new table](./new-and-empty-table.md#emptytable)
- [Query scope](./query-scope.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [Configure the production application](./configuration/configure-production-application.md)
- [Built-in query language functions](./built-in-functions.md)
- [Java objects and classes in query strings](./java-classes.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Casting](./casting.md)
- [Multi-aggregation](./combined-aggregations.md)
- [Create and use partitioned tables](./partitioned-tables.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [Null values](../reference/query-language/types/nulls.md)
- [`aggBy`](../reference/table-operations/group-and-aggregate/aggBy.md)
- [`AggAvg`](../reference/table-operations/group-and-aggregate/AggAvg.md)
- [`AggSum`](../reference/table-operations/group-and-aggregate/AggSum.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`partitionedTransform`](../reference/table-operations/partitioned-tables/partitionedTransform.md)
- [`transform`](../reference/table-operations/partitioned-tables/transform.md)
- [`update`](../reference/table-operations/select/update.md)
- [`Numeric` Javadoc](/core/javadoc/io/deephaven/function/Numeric.html)
- [`ExecutionContext` Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
- [`QueryLibrary` Javadoc](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html)
