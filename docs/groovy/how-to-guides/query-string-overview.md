---
title: An overview of query strings
---

Query strings are the main way you tell Deephaven's query engine what to compute. Almost every query uses them, so a conceptual understanding of the Deephaven Query Language (DQL) and query strings is essential for writing correct, performant, and effective queries. The guides in this section teach you what you need to know about query strings and DQL. This page gives a high-level overview of the most common query string features, with links to the pages that cover each one in depth.

## What is a query string?

A query string is a string that you pass as input to a table operation such as [`select`](../reference/table-operations/select/select.md), [`update`](../reference/table-operations/select/update.md), or [`where`](../reference/table-operations/filter/where.md).

Query strings are written in DQL, which is simple and natural to use. Its syntax is based on Java expressions, with a few Deephaven additions such as [backtick string literals](./string-char-literals.md) and the [underscore operator (`_`)](#arrays). Don't worry: you don't need to know Java to write query strings. Query strings can use [Groovy variables, closures, and objects](#groovy) as well as [Java classes and objects](./java-classes.md).

The same query string works on static tables and on [refreshing (real-time) tables](../conceptual/table-types.md), with the few restrictions this page notes.

The following code block uses a simple query string to create a new column called `NewColumn` in a table called `source`. The string passed into [`update`](../reference/table-operations/select/update.md) is the query string. The query string `NewColumn = 1` is a [formula](./formulas.md) that creates `NewColumn` and sets each of its values to 1.

```groovy order=source
source = emptyTable(10).update("NewColumn = 1")
```

## Formulas and filters

Query strings that you pass to selection operations such as [`select`](../reference/table-operations/select/select.md) and [`update`](../reference/table-operations/select/update.md) are [formulas](./formulas.md), which create columns. Query strings that you pass to filtering operations such as [`where`](../reference/table-operations/filter/where.md) are [filters](./filters.md), which keep or drop rows.

### Formulas

[Formulas](./formulas.md) contain assignments, usually in the form `LHS = RHS`. The left-hand side, or `LHS`, names the new column. The right-hand side, or `RHS`, defines the values that go in that column.

The following example uses five [formulas](./formulas.md) to create a table with five columns:

```groovy test-set=1 order=source
source = emptyTable(20).update(
    "X = 0.2 * ii",
    "Y = sin(X)",
    "Z = (ii % 2 == 0) ? `Even` : `Odd`",
    "RandomNumber = randomDouble(-1, 1)",
    "Timestamp = now() + ii * SECOND",
)
```

The `Z` [formula](./formulas.md) uses the [ternary conditional operator](./ternary-if-how-to.md) (`condition ? a : b`) to choose between two values. Query strings support many other [operators](./operators.md) as well.

All of the [formulas](./formulas.md) above use query language built-ins, which the [Query language built-ins](#query-language-built-ins) section explains.

### Filters

[Filters](./filters.md) evaluate to `true` or `false` and determine which rows the result table includes. Consider the following example, which uses the table created in the [previous section](#formulas):

```groovy test-set=1 order=resultLessThan,resultEven,resultRng,resultConjunctive,resultDisjunctive
resultLessThan = source.where("X <= 1.2")
resultEven = source.where("Z == `Even`")
resultRng = source.where("RandomNumber >= 0.21")
resultConjunctive = source.where("X <= 2.4", "Z == `Even`")
resultDisjunctive = source.where("X > 1.6 || Z == `Even`")
```

Passing several [filters](./filters.md) to one [`where`](../reference/table-operations/filter/where.md) call keeps only rows that match all of them, as in `resultConjunctive`. Use `||` inside a single [filter](./filters.md) to keep rows that match either condition, as in `resultDisjunctive`.

For more on filtering table data, see [Filter table data](./use-filters.md).

## What a query string can contain

Both [formulas](./formulas.md) and [filters](./filters.md) can use [literals](#literals), [built-in constants, variables, and functions](#query-language-built-ins), [Groovy variables, closures, and objects](#groovy), and [array access](#arrays). They can also call [Java classes and objects](./java-classes.md) directly.

### Literals

A literal is a fixed value written directly in a query string, rather than defined outside it and passed in. The earlier [formula](#formulas) and [filter](#filters) examples already use several, such as `0.2` and `` `Even` ``.

The following example creates a table with one column for each of several common literal types. The `literalsMeta` table, created with [`meta`](../reference/table-operations/metadata/meta.md), shows each column's data type.

```groovy order=literals,literalsMeta
literals = emptyTable(1).update(
    "BooleanLiteral = true",  // Boolean literal - note the lowercase `true`
    "IntLiteral = 42",  // 32-bit integer literal
    "LongLiteral = 42L",  // 64-bit integer literal
    "DoubleLiteral = 3.14",  // 64-bit floating point literal
    "StringLiteral = `Hello, world!`",  // String literal, in backticks (`)
    "DateTimeLiteral = '2023-01-01T00:00:00Z'",  // Date-time literal, single quotes
)
literalsMeta = literals.meta()
```

For more on literals, see:

- [Boolean and numeric literals in query strings](./boolean-numeric-literals.md)
- [String and char literals in query strings](./string-char-literals.md)
- [Date-time literals in query strings](./date-time-literals.md)

### Query language built-ins

The query language offers many [constants](./built-in-constants.md), [variables](./built-in-variables.md), and [functions](./built-in-functions.md) that you can use in a query string without importing anything. These built-ins are generally the most performant way to accomplish tasks in a query string. There are too many to list here.

The [formulas example](#formulas) already uses five of them. The following query adds two built-in constants to that example's `source` table:

```groovy test-set=1 order=result
result = source.update("MaxLong = MAX_LONG", "NullDouble = NULL_DOUBLE")
```

The [formulas example](#formulas) and the query above use the following built-ins:

- [`ii`](../reference/query-language/variables/special-variables.md): A built-in variable representing the current row number as a 64-bit integer.
- [`sin`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)): A function that returns the sine of its argument.
- [`randomDouble`](/core/javadoc/io/deephaven/function/Random.html#randomDouble(double,double)): A function that returns a random `double` value between its two arguments.
- [`now`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()): A function that returns the current date-time as an [`Instant`](../reference/query-language/types/date-time.md#instant).
- [`SECOND`](/core/javadoc/io/deephaven/time/DateTimeUtils.html#SECOND): A constant that equals one second in nanoseconds, used here to space the timestamps one second apart.
- [`MAX_LONG`](/core/javadoc/io/deephaven/util/QueryConstants.html#MAX_LONG): A constant that equals the maximum value of a Java primitive `long`.
- [`NULL_DOUBLE`](/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_DOUBLE): A constant that Deephaven uses to represent null for a Java primitive `double`.

> [!CAUTION]
> A [refreshing table](../conceptual/table-types.md) is one whose rows change as new data arrives. By default, a [formula](./formulas.md) or [filter](./filters.md) that uses [`ii`](../reference/query-language/variables/special-variables.md) on a [refreshing table](../conceptual/table-types.md) throws an error, unless the table is [append-only](../conceptual/table-types.md#specialization-1-append-only) or [blink](../conceptual/table-types.md#specialization-3-blink). The exception is [`ii`](../reference/query-language/variables/special-variables.md) used as a constant offset inside a [column array](./work-with-arrays.md#using-the-underscore-operator), such as `X_[ii - 1]`, which works on every table, as [Arrays](#arrays) explains. For the rules for [`i`](../reference/query-language/variables/special-variables.md), [`k`](../reference/query-language/variables/special-variables.md), and [column arrays](./work-with-arrays.md#using-the-underscore-operator) such as `X_`, see [refreshing table restrictions](../reference/query-language/variables/special-variables.md#refreshing-table-restrictions).

For more on query language built-in constants, variables, and functions, see:

- [Query language built-in constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)

### Groovy

Deephaven lets you use Groovy code in query strings. Query strings can reference Groovy [variables](./groovy-variables.md), [closures](./groovy-closures.md), and [objects](./groovy-classes.md) from the [query scope](./query-scope.md). To use the name of a class your script defines, such as `MyClass`, first import the class into the [query library](../conceptual/execution-context.md). [Groovy classes and objects in query strings](./groovy-classes.md#make-a-groovy-class-available-to-query-strings) shows how.

The following example uses a Groovy [variable](./groovy-variables.md), a [closure](./groovy-closures.md) (`randName`), and an [object](./groovy-classes.md) (`myObj`) of a Groovy class in a query string.

```groovy order=source
namesList = ["James", "Jessica", "Albert", "Ophelia", "Sophia", "Mark"]
groovyVar = 3

randName = { -> namesList[new Random().nextInt(namesList.size())] }

class MyClass {
    public int a = 1
    public static int myValue = 3

    public int b

    MyClass(int b) {
        this.b = b
    }

    int getA() {
        return this.a
    }

    static int scaledValue(int factor) {
        return myValue * factor
    }
}

myObj = new MyClass(2)

source = emptyTable(10).update(
    "GroovyVariable = groovyVar",
    "Name = randName()",
    "X = myObj.getA()",
    "Y = myObj.b",
    "Z = myObj.scaledValue(5)",
)
```

A query string can read a public field directly, as in `myObj.b`, or call a method on the object, as in `myObj.getA()`. Calling a static method through the object, as in `myObj.scaledValue(5)`, works without importing `MyClass`. Each of these columns takes its type from the Groovy declaration, so `X`, `Y`, and `Z` are `int` columns.

A query string calls a [closure](./groovy-closures.md) with the same syntax as a [built-in function](./built-in-functions.md): `randName()` looks like [`sin(X)`](/core/javadoc/io/deephaven/function/Numeric.html#sin(double)). Unlike a built-in, a [closure](./groovy-closures.md) returns `Object`, so `Name` is an `Object` column. [Cast](./casting.md) the call, as in `(String)randName()`, when you need a typed column. See [Groovy closures in queries](./groovy-closures.md).

For more on using Groovy in query strings, see:

- [Query scope](./query-scope.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)

### Arrays

Columns can hold [arrays](./work-with-arrays.md). The [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) table operation is the most common way to make an array column. It stores each group as a [vector](./work-with-arrays.md#vector-columns), Deephaven's own array type, which is distinct from a [Java array](./work-with-arrays.md#java-array-columns). Deephaven provides a suite of built-in methods for working with [vectors](./work-with-arrays.md#vector-columns), including methods that access their elements.

The following example creates a [vector column](./work-with-arrays.md#vector-columns), then uses some of these methods to access elements and subsets of elements in the vector.

```groovy order=source,result,resultMeta
source = emptyTable(10).update("X = ii").groupBy()
result = source.update(
    "SubsetX = X.subVector(3, 8)",
    "StringX = X.toString(10)",
    "FifthElement = X.get(4)",
)
resultMeta = result.meta()
```

In the result:

- `SubsetX` holds elements 3 through 7, because the end index of [`subVector`](/core/javadoc/io/deephaven/vector/Vector.html#subVector(long,long)) is exclusive.
- `StringX` shows up to the first 10 elements as a string.
- `FifthElement` is `4`, because vector indexes start at 0.

Additionally, the [underscore operator](./work-with-arrays.md#using-the-underscore-operator) (`_`) lets a query string access any column as an array. For example, the following code grabs previous and next elements from a column:

```groovy order=source,result
source = emptyTable(10).update("X = ii")
result = source.update(
    "PreviousElement = X_[ii - 1]",
    "NextElement = X_[ii + 1]",
)
```

The first row's `PreviousElement` and the last row's `NextElement` are null, because those offsets fall outside the table.

On a [refreshing table](../conceptual/table-types.md), [column arrays](./work-with-arrays.md#using-the-underscore-operator) follow these rules by default:

- An index of [`i`](../reference/query-language/variables/special-variables.md) or [`ii`](../reference/query-language/variables/special-variables.md) works on every table type, alone or with an [integer literal](./boolean-numeric-literals.md) added or subtracted, such as `X_[ii]` or `X_[ii - 1]`.
- Any other use throws an error unless the table is a [blink table](../conceptual/table-types.md#specialization-3-blink). An [append-only table](../conceptual/table-types.md#specialization-1-append-only) is not exempt. This includes the whole array (`X_`), a fixed index (`X_[0]`), and other index expressions (`X_[ii * 2]`).

For the full rules, see [refreshing table restrictions](../reference/query-language/variables/special-variables.md#refreshing-table-restrictions).

For full coverage of arrays in tables, see the [Arrays guide](./work-with-arrays.md).

## Related documentation

- [Deephaven's table types](../conceptual/table-types.md)
- [Execution Context](../conceptual/execution-context.md)
- [Formulas](./formulas.md)
- [Filters](./filters.md)
- [Operators](./operators.md)
- [Casting](./casting.md)
- [Use the ternary conditional operator in query strings](./ternary-if-how-to.md)
- [Data types in Deephaven](./data-types.md)
- [Boolean and numeric literals in query strings](./boolean-numeric-literals.md)
- [String and char literals in query strings](./string-char-literals.md)
- [Date-time literals in query strings](./date-time-literals.md)
- [Query language built-in constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Null, NaN, and infinity values](./null-inf-nan.md)
- [Query scope](./query-scope.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)
- [Java objects and classes in query strings](./java-classes.md)
- [Arrays](./work-with-arrays.md)
- [Strings](./work-with-strings.md)
- [Programmatically generate query strings with Groovy](./generate-query-strings.md)
- [Select and create columns](./use-select-view-update.md)
- [Filter table data](./use-filters.md)
- [Special variables and constants](../reference/query-language/variables/special-variables.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`select`](../reference/table-operations/select/select.md)
- [`update`](../reference/table-operations/select/update.md)
- [`where`](../reference/table-operations/filter/where.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html)
