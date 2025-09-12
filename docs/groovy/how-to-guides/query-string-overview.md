---
id: query-string-overview
title: An overview of query strings
sidebar_label: Overview
---

This page provides a succinct overview of query strings in Deephaven. Query strings are how you interact with Deephaven's query engine. They are used in almost every query written, so a conceptual understanding of the Deephaven Query Language (DQL) and query strings is essential for writing correct, performant, and effective queries. The guides are organized to help you learn everything you need to know about query strings and the DQL. This page provides a high-level overview of each item in the sidebar. For a more in-depth explanation of each topic, refer to the corresponding page.

## What is a query string?

A query string is a string that gets passed into a table operation as input. More specifically, a query string gets passed to certain table operations such as [`select`](../reference/table-operations/select/select.md), [`where`](../reference/table-operations/filter/where.md), and more. A single query string is used to either create a new column or filter data in a table.

Query strings utilize the "Deephaven Query Language," or DQL for short. DQL is simple and natural to use. It combines features of Java and Groovy to allow complex and powerful queries. Don't worry; you don't need to be proficient in Java to write query strings. Query strings can use Groovy variables, functions, and classes as well as Java variables, functions, and classes.

The following code block provides an example of a simple query string that creates a new column called `NewColumn` in a table called `source`. The string passed into [`update`](../reference/table-operations/select/update.md) is the query string. The query string `NewColumn = 1` defines a [formula](./formulas.md), where the left-hand side (LHS) is the name of the column to be created and the right-hand side (RHS) defines the values that will be assigned to that column. In this case, each value in `NewColumn` is 1.

```groovy order=source
source = emptyTable(10).update("NewColumn = 1")
```

## Query string examples

A query string defines a [formula](./formulas.md) or a [filter](./filters.md):

- [Formulas](./formulas.md) contain assignments.
- [Filters](./filters.md) evaluate to True/False.

Query strings can contain a wide array of expressions. Different types of expressions are explored via examples in the following subsections.

### Formulas

Formulas contain assignments. These assignments usually come in the form of `LHS = RHS`, where the left-hand-side (`LHS`) contains the name of the new column that will be created, and the right-hand-side (`RHS`) defines the values that will be assigned to that column.

The following example creates a table with five columns. Each column is created via a formula:

```groovy test-set=1 order=source
source = emptyTable(20).update(
    "X = 0.2 * ii",
    "Y = sin(X)",
    "Z = (ii % 2 == 0) ? `Even` : `Odd`",
    "RandomNumber = randomDouble(-1, 1)",
    "Timestamp = now() + ii * SECOND",
)
```

The formulas used above all leverage query language built-ins including [built-in constants](./built-in-constants.md), [built-in variables](./built-in-variables.md), and [built-in functions](./built-in-functions.md).

### Filters

Filters evaluate to True/False and are used to determine which rows are included in the result table. Consider the following example, which uses the table created in the [previous section](#formulas):

```groovy test-set=1 order=resultLessThan,resultEven,resultRng,resultConjunctive,resultDisjunctive
resultLessThan = source.where("X <= 1.2")
resultEven = source.where("Z == `Even`")
resultRng = source.where("RandomNumber >= 0.21")
resultConjunctive = source.where("X <= 2.4", "Z == `Even`")
resultDisjunctive = source.where("X > 1.6 || Z == `Even`")
```

For more on filtering table data, see [Use filters](./filters.md).

### Literals

A literal is a fixed value that's used in a query string. The [previous section](#filters) used numeric literals in some of the filter statements. The value is defined in the query string itself, not outside of it and then passed in as a parameter. The following example creates a table with several columns, each containing a literal value. The table metadata is included to show the resultant column data types.

```groovy order=literals,literalsMeta
literals = emptyTable(1).update(
    "BooleanLiteral = true",  // Boolean literal - note the lowercase `true`
    "IntLiteral = 42",  // 32-bit integer literal
    "LongLiteral = 42L",  // 64-bit integer literal
    "DoubleLiteral = 3.14",  // 64-bit floating point literal
    "StringLiteral = `Hello, world!`",  // String literal, enclosed in backticks (`)
    "DateTimeLiteral = '2023-01-01T00:00:00Z'",  // Date-time (Instant) literal, enclosed in single quotes (')
)
literalsMeta = literals.meta()
```

For more on literals, see any of the following links:

- [Boolean and numeric literals](./boolean-numeric-literals.md)
- [String literals](./string-char-literals.md)
- [Date-time literals](./date-time-literals.md)

### Query language built-ins

The query language offers many constants, variables, and functions that can be called with no additional import statements or setup. These built-ins are generally the most performant way to accomplish tasks in a query string. There are too many built-ins to list here; the following query uses just a few of them:

```groovy order=source
source = emptyTable(10).update(
    "X = 0.1 * ii",
    "Y = sin(X)",
    "NoisyY = Y + randomDouble(-0.2, 0.2)",
    "MaxLong = MAX_LONG",
    "NullDouble = NULL_DOUBLE",
    "Letter = (ii % 2 == 0) ? `A` : `B`",
    "Timestamp = now() + ii * SECOND",
)
```

The above example uses the following built-ins:

- [`ii`](../reference/query-language/variables/special-variables.md): A built-in variable representing the current row number as a 64-bit integer.
- [`sin`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Numeric.html#sin(double)): The sine function.
- [`randomDouble`](https://docs.deephaven.io/core/javadoc/io/deephaven/function/Random.html#randomDouble(double,double)): A function that generates a random double floating-point precision value between the two input arguments.
- [`MAX_LONG`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/QueryConstants.html#MAX_LONG): A constant that equals the maximum value of a Java primitive `long`.
- [`NULL_DOUBLE`](https://docs.deephaven.io/core/javadoc/io/deephaven/util/QueryConstants.html#NULL_DOUBLE): A constant that equals null for a Java primitive `double`.
- [`now()`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#now()): A function that returns the current date-time as an Instant.

For more on query language built-in constants, variables, and functions, see:

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)

### Groovy

Deephaven allows you to use Groovy code in query strings.

The following example uses a Groovy variable, function, and class in a query string.

```groovy order=source
namesList = ["James", "Jessica", "Albert", "Ophelia", "Sophia", "Mark"]
groovyVar = 3


randName = { -> namesList[new Random().nextInt(namesList.size())] }

class MyClass {
    public int a = 1
    public static BigDecimal myValue = 3.14

    public int b

    MyClass(int b) {
        this.b = b
    }

    int getA() {
        return this.a
    }
    int getB() {
        return this.b
    }

    static int changeValue(int newValue) {
        myValue = newValue
        return newValue
    }
}

myClass = new MyClass(2)

source = emptyTable(10).update(
    "GroovyVariable = groovyVar",
    "Name = randName()",
    "X = myClass.getA()",
    "Y = myClass.b",
    "Z = (int)myClass.changeValue(5)",
)
```

### Arrays

Columns can contain array data. These array columns are most commonly created with the [`groupBy`](../reference/table-operations/group-and-aggregate/groupBy.md) table operation. Deephaven supports the manipulation of array columns, and provides a suite of built-in array methods for working with them.

The following example creates an array column, then uses some built-in array methods to access elements and subsets of elements in the array.

```groovy order=source,result,resultMeta
source = emptyTable(10).update("X = ii").groupBy()
result = source.update(
    "SubsetX = X.subVector(3, 8)",
    "StringX = X.toString(10)",
    "FifthElement = X.get(5)",
)
resultMeta = result.meta()
```

Additionally, all columns in tables are backed by arrays. You can leverage this with the underscore operator (`_`). For example, the following code grabs previous and next elements from a column:

:::caution
The special row variable [`ii`](../reference/query-language/variables/special-variables.md) is not safe in ticking tables.
:::

```groovy order=source
source = emptyTable(10).update("X = ii")
result = source.update(
    "PreviousElement = X_[ii - 1]",
    "NextElement = X_[ii + 1]",
)
resultMeta = result.meta()
```

For full coverage on arrays in tables, see [Arrays in Deephaven](./arrays.md).

## Related documentation

- [Formulas in query strings](./formulas.md)
- [Filters in query strings](./filters.md)
- [Operators in query strings](./operators.md)
- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Handle infinity, null, and NaN](./null-inf-nan.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in query strings](./groovy-closures.md)
- [Java objects in query strings](./java-classes.md)
- [Work with arrays](./arrays.md)
- [Work with strings](./strings.md)
- [Select and update columns](./use-select-view-update.md)
- [Filter table data](./filters.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
