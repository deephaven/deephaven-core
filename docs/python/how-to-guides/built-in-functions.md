---
title: Built-in query language functions
---

Like [constants](./built-in-constants.md) and [variables](./built-in-variables.md), the query language has many built-in functions that you can call without any import or setup. Use these built-in functions instead of [Python functions](./python-functions.md) or other user-defined functions in [query strings](./query-string-overview.md) for these reasons:

- They are faster than [Python functions](./python-functions.md).
- Most of them return [a null result for null input](../reference/query-language/types/nulls.md) instead of throwing an error.
- They need no extra code: you call them by name.

For more on the performance difference, see [The Python-Java boundary](../conceptual/python-java-boundary.md).

## Use built-in functions in query strings

Built-in functions range from simple mathematical functions to date-time arithmetic, string parsing, and more. The examples below show a few of them. For the complete list, see [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md).

The following query calculates the absolute value of a column using the built-in [`abs`](/core/javadoc/io/deephaven/function/Numeric.html#abs(long)) function:

```python order=source
from deephaven import empty_table

source = empty_table(10).update(["X = -ii", "AbsX = abs(X)"])
```

The following query parses a column of [strings](../reference/query-language/types/strings.md) containing integers into [Java primitive](./data-types.md) `int` values using [`parseInt`](/core/javadoc/io/deephaven/function/Parse.html#parseInt(java.lang.String)):

```python order=result,source
from deephaven import empty_table

source = empty_table(10).update("StringColumn = Integer.toString(randomInt(0, 10))")
result = source.update("IntegerColumn = parseInt(StringColumn)")
```

The following query uses the built-in [`and`](/core/javadoc/io/deephaven/function/Logic.html#and(java.lang.Boolean...)) function to evaluate whether three columns are all `true`:

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

Unlike most built-in functions, [`and`](/core/javadoc/io/deephaven/function/Logic.html#and(java.lang.Boolean...)) throws an error if any argument is [null](../reference/query-language/types/nulls.md). To treat [nulls](../reference/query-language/types/nulls.md) as a fixed value, use the [`and`](/core/javadoc/io/deephaven/function/Logic.html#and(java.lang.Boolean[],java.lang.Boolean)) overload that takes a replacement value for [nulls](../reference/query-language/types/nulls.md).

Most other built-in functions return [a null result for null input](../reference/query-language/types/nulls.md). For example, the following query calls the built-in [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) function on a column that contains [null values](../reference/query-language/types/nulls.md). `SinX` is [null](../reference/query-language/types/nulls.md) in those rows:

```python order=source
from deephaven import empty_table

source = empty_table(40).update(
    ["X = (ii % 3 != 0) ? 0.1 * ii : NULL_DOUBLE", "SinX = sin(X)"]
)
```

## Add Java classes to the query library

Built-in functions cover common needs, and [query strings](./query-string-overview.md) can also use [other Java classes](./java-classes.md). The [query library](./java-classes.md#import-java-classes) holds the imports that [query strings](./query-string-overview.md) can use. By default, it includes:

- The `java.lang` and `java.util` packages
- Specific classes from other packages
- The static methods of the built-in function classes

That is why you can call [`abs`](/core/javadoc/io/deephaven/function/Numeric.html#abs(long)) or [`parseInt`](/core/javadoc/io/deephaven/function/Parse.html#parseInt(java.lang.String)) without a class name, and refer to a class such as `Integer` by its simple name, without its package. For the complete list of default imports, see [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md).

To use a class from any other package, you have these options:

- Use its fully qualified name, such as `java.net.URI`.
- Import it with [`import_class`](/core/pydoc/code/deephaven.query_library.html#deephaven.query_library.import_class) from the [`query_library`](/core/pydoc/code/deephaven.query_library.html) module, then use its simple name, such as `URI`.
- Import its whole package with [`import_package`](/core/pydoc/code/deephaven.query_library.html#deephaven.query_library.import_package), then use the simple name of any public class in it.

For an example that imports a class, see [Import Java classes](./java-classes.md#import-java-classes).

### Import static methods

You can also import a class's static methods into the [query library](./java-classes.md#import-java-classes) with [`import_static`](/core/pydoc/code/deephaven.query_library.html#deephaven.query_library.import_static), so [query strings](./query-string-overview.md) can call them without the class name. The following example imports the static methods of [`java.lang.StrictMath`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/StrictMath.html) and calls `fma` without a class name.

> [!NOTE]
> [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html), a built-in class, also defines [`acos`](/core/javadoc/io/deephaven/function/Numeric.html#acos(double)), [`exp`](/core/javadoc/io/deephaven/function/Numeric.html#exp(double)), and [`getExponent`](/core/javadoc/io/deephaven/function/Numeric.html#getExponent(double)). After [`import_static`](/core/pydoc/code/deephaven.query_library.html#deephaven.query_library.import_static) imports `StrictMath`, a call to any of those names without a class name is ambiguous. The example prefixes each one with a class name:
>
> - `StrictMath.acos` and `StrictMath.getExponent` work without an import, because the [query library](./java-classes.md#import-java-classes) includes `java.lang` by default.
> - [`exp`](/core/javadoc/io/deephaven/function/Numeric.html#exp(double)) needs the fully qualified name `io.deephaven.function.Numeric.exp`. The [query library](./java-classes.md#import-java-classes) imports the static methods of [`Numeric`](/core/javadoc/io/deephaven/function/Numeric.html), but not the class name `Numeric`.
>
> The import lasts for the rest of the [execution context](../conceptual/execution-context.md), not just this query. After it, a call without a class name to any method that both classes define for the same argument type is ambiguous in every [query string](./query-string-overview.md). That includes the earlier examples on this page, such as [`abs`](/core/javadoc/io/deephaven/function/Numeric.html#abs(long)) on a `long` or [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)) on a `double`. Prefix those calls with a class name too.
>
> Other names switch to the `StrictMath` method with no error. After the import, `min(A, B)` on two `double` or `int` values calls `StrictMath.min`. That method treats a Deephaven [null](../reference/query-language/types/nulls.md) as an ordinary number, so when `A` is [null](../reference/query-language/types/nulls.md), `min(A, B)` returns [null](../reference/query-language/types/nulls.md) instead of `B`. To keep Deephaven's [null handling](../reference/query-language/types/nulls.md), write `io.deephaven.function.Numeric.min(A, B)`.

```python order=source reset
from deephaven import empty_table
from deephaven import query_library

query_library.import_static("java.lang.StrictMath")

source = empty_table(10).update(
    [
        "X = randomDouble(-1.0, 1.0)",
        "FmaX = fma(X, 2.0, 1.0)",
        "AcosX = StrictMath.acos(X)",
        "ExpX = io.deephaven.function.Numeric.exp(X)",
        "ExponentX = StrictMath.getExponent(X)",
    ]
)
```

## Related documentation

- [The Python-Java boundary](../conceptual/python-java-boundary.md)
- [Execution context](../conceptual/execution-context.md)
- [Query string overview](./query-string-overview.md)
- [Built-in constants](./built-in-constants.md)
- [Built-in variables](./built-in-variables.md)
- [Ternary conditional operator](./ternary-if-how-to.md)
- [Python functions in query strings](./python-functions.md)
- [Data types in Deephaven and Python](./data-types.md)
- [Java objects in query strings](./java-classes.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [Math functions](../reference/query-language/query-library/auto-imported/math.md)
- [Logic functions](../reference/query-language/query-library/auto-imported/logic.md)
- [Parse functions](../reference/query-language/query-library/auto-imported/parse.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [Strings](../reference/query-language/types/strings.md)
- [`empty_table`](../reference/table-operations/create/emptyTable.md)
- [`new_table`](../reference/table-operations/create/newTable.md)
- [`bool_col`](../reference/table-operations/create/boolCol.md)
- [`update`](../reference/table-operations/select/update.md)
- [`query_library` Pydoc](/core/pydoc/code/deephaven.query_library.html)
