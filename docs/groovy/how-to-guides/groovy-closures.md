---
title: User-defined functions
---

Groovy closures can be used in Deephaven queries. This guide explains what a closure is and how it is used in Deephaven. These closures can be from libraries, or they can be user-defined.

> [!NOTE]
> The list of default Java imports can be reviewed in the [deephaven-core repository on GitHub](https://github.com/deephaven/deephaven-core/blob/main/engine/table/src/main/java/io/deephaven/engine/table/lang/impl/QueryLibraryImportsDefaults.java).

In cases where you need to apply the same operations to many tables in a query - for example, something like ("filter to where Y < 5, then sum by Letter") - it can be helpful to define those operations as a single function. Then, you can call that function multiple times for each table you want to transform, rather than duplicating the operations many times in the query. The simplest way to define a function in Groovy is with a [closure](https://docs.groovy-lang.org/latest/html/api/groovy/lang/Closure.html).

In Groovy, a [closure](https://docs.groovy-lang.org/latest/html/api/groovy/lang/Closure.html) is enclosed with curly braces (`{}`). The following closure, `f`, adds two integer values:

```groovy test-set=1
f = { int a, int b -> a + b }
```

Closures can take an arbitrary number of input parameters but must only have one return value. Multiple returns must be wrapped in a single object, such as a list, array, etc.

## User-defined functions in table operations

Closures can be called in query strings. The query below calls the `f` closure that we defined above.

> [!NOTE]
> Calling `f` in the query string below is shorthand for `f.call(X, Y)`.

```groovy test-set=1
source = emptyTable(10).update("X = i", "Y = 2 * i", "Z = f(X, Y)")
```

## Query language methods

One thing to consider before creating and calling a closure from the query language is:

> Can I perform the same operation using a built-in query language method?

For instance, the function `sin` from the `java.lang.Math` library is imported and called in the query below:

```groovy test-set=2
import java.lang.Math

sine = { Double X -> Math.sin(X) }

source = emptyTable(100).update("X = 0.1 * ii", "Y = sine(X)")
```

> [!IMPORTANT]
> Always define the expected argument types when defining a closure that will be called from the query language. This allows errors to be caught at compile time rather than at runtime, and often improves performance. For instance, if we define the `sine` function above as `sine = { X -> Math.sin(X) }`, omitting the `Double` specification, the query will take 285 milliseconds to complete instead of 7.

> [!NOTE]
> Closures used in query strings should perform null checks on their arguments, e.g., with `isNull()`. If null values are not handled, closures may yield undesirable results when nulls are encountered.

Deephaven has a built-in [`sin`](https://deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) method that can be used from the query language with no imports. It handles `null` values by default: a call to `sin(NULL_DOUBLE)` will return `NULL_DOUBLE`; Math.sin will return `-0.004961954789184062` -- not what we're looking for.

```groovy test-set=2
source = emptyTable(100).update("X = 0.1 * ii", "Y = sin(X)")
```

Familiarity with Deephaven's built-in methods can speed up your workflow. Deephaven has a large number of built-in methods that can be called from query strings without any imports or classpaths. For more information and a complete list of what's available, see [auto-imported functions in Deephaven](../reference/query-language/query-library/auto-imported/index.md).

## Passing tables to functions

Groovy closures can take tables as input and return tables as output. Some Deephaven operations specifically require this usage pattern.

The following example uses the closure `doAgg` to perform [multiple aggregations](./combined-aggregations.md) on a table.

```groovy order=result,source
import static io.deephaven.api.agg.Aggregation.AggSum
import static io.deephaven.api.agg.Aggregation.AggAvg

source = emptyTable(20).update("Letter = (i % 2 == 0) ? `A` : `B`", "X = i", "Y = randomDouble(0.0, 10.0)")

doAgg = {t -> t.aggBy([AggSum("SumX = X", "SumY = Y"), AggAvg("AvgX = X", "AvgY = Y")], "Letter")}

result = doAgg(source)
```

### Partitioned tables

Some partitioned table operations require the use of Groovy functions that modify tables:

- `transform`
- `partitionedTransform`

## Function from a library

In the following example, a function from a library is called.

```groovy order=result
result = emptyTable(2).update("Home = java.lang.System.getenv(`HOME`)")
```

> [!NOTE]
> Deephaven tables are immutable. A function or closure that modifies a table will have no effect unless a table is returned.

### Imported Java methods

In the following example, imported Java methods are used inside the query string.

> [!NOTE]
> For details on how to install Java packages, see [this guide](../how-to-guides/install-and-use-java-packages.md).

```groovy order=source,result
source = newTable(
    intCol("X", 2, 4, 6)
)

result = source.update("Z = sqrt(X)")
```

<!--TODO: add links when reference docs go in

Partitioned tables are beyond the scope of this guide. See the links above for more information on closures and partitioned tables. -->

## Import packages with user-defined functions

It's common in Groovy to use custom closures from a separate Groovy file or package. To make these closures available for import in Deephaven, you can add files or folders to the classpath by modifying your `docker-compose.yml` file. For example, say we want to import a closure from a file called `MyClass.groovy`, located at `/data/groovyscripts/com/example/MyClass.groovy`.

We can add the whole `groovyscripts` folder to our classpath by adding the `EXTRA_CLASSPATH` environment variable to our `docker-compose.yml` file, like so:

```yml
services:
  deephaven:
    image: ghcr.io/deephaven/server:latest
    ports:
      - 10000:10000
    volumes:
      - ./data:/data
    environment:
      - EXTRA_CLASSPATH=/data/groovyscripts
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

Now, from the IDE, we can import the class containing the closure we want to call:

```groovy skip-test
import com.example.MyClass
```

> [!NOTE]
> When running [Deephaven from Docker](../getting-started/docker-install.md), the path must be visible inside the Docker container. For more information, see [Docker volumes](../conceptual/docker-data-volumes.md).

## Related documentation

- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [Create a new table](../how-to-guides/new-and-empty-table.md#newtable)
- [Docker volumes](../conceptual/docker-data-volumes.md)
- [How to use variables and functions in query strings](../how-to-guides/query-scope.md)
- [How to install Java packages](../how-to-guides/install-and-use-java-packages.md)
- [How to use Deephaven's built-in query language functions](../how-to-guides/built-in-functions.md)
- [Query language functions](../reference/query-language/query-library/auto-imported/index.md)
- [update](../reference/table-operations/select/update.md)
- [view](../reference/table-operations/select/view.md)
