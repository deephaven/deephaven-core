---
title: Formulas and filters
---

This guide will cover formulas in Deephaven, including how to use them to filter data and assign data to columns. Formulas are used in query strings, which are used in a wide variety of table operations.

Formulas can be used in two contexts:

- To filter data in tables via any of the following table operations:
  - [`where`](../reference/table-operations/filter/where.md)
  - [`whereIn`](../reference/table-operations/filter/where-in.md)
  - [`whereNotIn`](../reference/table-operations/filter/where-not-in.md)
- To assign data to columns via any of the following table operations:
  - [`select`](../reference/table-operations/select/select.md)
  - [`view`](../reference/table-operations/select/view.md)
  - [`update`](../reference/table-operations/select/update.md)
  - [`updateView`](../reference/table-operations/select/update-view.md)
  - [`lazyUpdate`](../reference/table-operations/select/lazy-update.md)

## What is a formula?

Regardless of how a formula is used, it is constructed in the same way and follows the same structure. A formula expresses a relationship between a left and right-hand side by one or more [operators](#appendix-operators). The left side contains a single variable - the resultant column name. The right side contains a Java expression, which can include any of the following:

- [operators](#appendix-operators)
- [methods](../reference/query-language/formulas/auto-imported-functions.md)
- [functions](../how-to-guides/user-defined-functions.md)
- [objects](../reference/query-language/types/objects.md)
- [variables](../reference/query-language/variables/scope.md)
- [literals](./work-with-strings.md#string-literals-in-query-strings)
- [special variables](../reference/query-language/variables/special-variables.md)
- [type casts](../reference/query-language/control-flow/casting.md)

## Operators

Operators can be used to construct formulas. Deephaven gives users access to [all of Java's operators](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/operators.html). For a complete list, see the [appendix](#appendix-operators) at the bottom of this guide.

The following code block implements a variety of operators to create new columns of values.

```groovy
class MyObj {
    public int a, b, c

    MyObj(int a, int b, int c) {
        this.a = a
        this.b = b
        this.c = c
    }

    int compute(int value1){
        return  value1
    }
}

obj = new MyObj(1, 2, 3)

result = emptyTable(10).update(
	            "A = i",
	            "B = A * A",
	            "C = A / 2",
	            "D = A % 3",
	            "E = (int) C",
	            "F = A_[i-2]",
	            "G = obj.a",
	            "H = obj.compute(A)",
	            "I = sqrt(A)"
	        )
```

In this example, comparison operators are used to filter a table.

```groovy order=source,greaterThan,greaterThanOrEqual,lessThan,lessThanOrEqual
source = newTable(
    intCol("Value", 0, 1, 2, 3, 4, 5, 6)
)

greaterThan = source.where("Value > 3")
greaterThanOrEqual = source.where("Value >= 3")
lessThan = source.where("Value < 3")
lessThanOrEqual = source.where("Value <= 3")
```

### Methods

Methods in formulas are those that are [built into the query language](../reference/query-language/formulas/auto-imported-functions.md). The following code block uses the built-in [`lowerBin`](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#lowerBin(java.time.Instant,java.time.Duration)) method to bin timestamp data into 5-minute buckets.

```groovy order=result,source
source = emptyTable(20).update("Timestamp = '2024-04-04T08:00:00 ET' + i * MINUTE")
result = source.update("FiveMinBin = lowerBin(Timestamp, 'PT5m')")
```

### Functions (Groovy closures)

Closures are [user-defined Groovy code blocks](../how-to-guides/user-defined-functions.md) that can be called in formulas, like so:

```groovy order=result,source
f = { a, b -> a * b }

source = emptyTable(10).update("X1 = 0.1 * i", "X2 = 0.2 * i")
result = source.update("Y = f(X1, X2)")
```

### Objects

Formulas can use [objects](../reference/query-language/types/objects.md). The following code block uses a Groovy class method in a formula.

```groovy order=result,source
f = { int a, int b -> a * b }

class MyObj {
    public int a, b, c

    MyObj(int a, int b, int c) {
        this.a = a
        this.b = b
        this.c = c
    }

    int compute(int value1){
        return  value1
    }
}

obj = new MyObj(1, 2, 3)

source = emptyTable(10).update("X = i")
result = source.update("Y = obj.compute(X)")
```

### Variables

Variables in formulas can either be a column name or a Java variable. The following code block adds two variables by using a variable defined in Java to a column:

```groovy order=source
var = 5

source = emptyTable(10).update("X = i", "Y = X + var")
```

### Literals

Literals in formulas are encapsulated in single quotes (`'`). They get inferred as a different data type. The following code block uses a string literal in the formula that gets inferred as a [Java Instant](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html):

```groovy order=source
source = emptyTable(10).update("X = '2024-01-01T00:00:00 ET' + i * SECOND")
```

### Special variables

Deephaven offers three special variables, one of which that has already been used in several code blocks in this document. They are `i`, `ii`, and `k`, which represent row indices. Only `i` and `ii` represent traditional row indices as `int` and `long`, respectively, while `k` should only be used in limited circumstances such as advanced operations or debugging.

> [!CAUTION]
> Note that these variables are only useable in append-only tables and can be unreliable in ticking tables.

The following code block uses `i` and `ii` to get row indices in a formula:

```groovy order=source
source = emptyTable(10).update("X = i", "Y = ii")
```

### Type casts

Type casts are used in formulas when the formula would otherwise produce a column with an undesired data type. Groovy functions, for example, will produce object columns without a type hint or a type cast in the formula in which they are called. The following code block uses a type cast to ensure that the column produced by the Groovy function `f` is of the correct data type.

> [!NOTE]
> Type hints are advised for Groovy functions, as they make query strings and formulas more readable.

```groovy order=source
f = { int a, int b -> (int) a / b }

source = emptyTable(10).update("X = i", "Y = f(X, 2)")

result = emptyTable(10).update("X = i", "Y = (double)f(X, 2)")
```

## Boolean (filter) formulas

Formulas filter data from tables based on whether the data meets a given condition. These formulas return a boolean `True` or `False` value that determines whether or not the data is kept in the resultant table.

Filter formulas either:

- Compare the left-hand side to the right-hand side using one or more [comparison operators](#comparison-operators)
- Use one or more [logical operators](#logical-operators) to enforce a condition.

The following example creates a table with some data and then filters it using formulas that use comparison and logical operators.

```groovy order=resultComparison,resultLogical,source
source = emptyTable(10).update("X = i", "TrueFalse = randomBool()")

resultComparison = source.where("X > 5")
resultLogical = source.where("!TrueFalse")
```

Filter formulas can use more than one condition to filter data. Filters can be [conjunctive](./use-filters.md#conjunctive), where both conditions must be met, or [disjunctive](./use-filters.md#disjunctive), where only one condition must be met.

```groovy order=resultLogicalAndComparison,resultLogicalOrComparison,source
source = emptyTable(10).update("X = i", "TrueFalse = randomBool()")

resultLogicalAndComparison = source.where("X <= 7", "TrueFalse")
resultLogicalOrComparison = source.where("X < 7 || X = 7", "TrueFalse")
```

> [!NOTE]
> As in the above example, using two formulas in a table operation requires encapsulating them in a list.

## Assignment formulas

Formulas that assign data to columns do so by equating the left-hand side of the formula to its right-hand side. The left-hand side defines the column's name, while the right-hand side defines the data in that column.

Assignment formulas use the [assignment operator](#assignment-operators) `=` to relate the left-hand and right-hand sides and return data of any type. In the [filter formulas](#boolean-filter-formulas) section, assignment formulas were used to assign data to columns before filtering it. The following example demonstrates several different assignment formulas used in an [`update`](../reference/table-operations/select/update.md) operation.

```groovy order=result
class MyObj {
    public int a, b, c

    MyObj(int a, int b, int c) {
        this.a = a
        this.b = b
        this.c = c
    }

    int compute(int value1){
        return  value1
    }
}

obj = new MyObj(1, 2, 3)

result = emptyTable(10).update(
        "A = i",
        "B = A * A",
        "C = A / 2",
        "D = A % 3",
        "E = (int) C",
        "F = A_[i-2]",
        "G = obj.a",
        "H = obj.compute(A)",
        "I = sqrt(A)"
)
```

## Null values

Null values in tables are represented by the following constants:

- `byte`: `NULL_BYTE`
- `short`: `NULL_SHORT`
- `int`: `NULL_INT`
- `long`: `NULL_LONG`
- `float`: `NULL_FLOAT`
- `double`: `NULL_DOUBLE`
- `char`: `NULL_CHAR`

These constants are available in two places:

- The query language.
- The [`io.deephaven.util.QueryConstants`](/core/javadoc/io/deephaven/util/QueryConstants.html) Groovy package.

The following example uses null values in formulas with both the built-in constants and the Groovy package.

```groovy order=source
import io.deephaven.util.QueryConstants

f = { index ->
    if (index % 4 == 0) {
        return NULL_INT
    } else {
        return index
    }
}

source = emptyTable(10).update("X = (i % 3 == 0) ? NULL_INT : i", "Y = f(ii)")
```

## NaN and infinity values

NaN values and infinity are different than [null values](#null-values), and are used exclusively for floating point data types. Where null represents an absence of data, NaN and infinity typically represent an incorrect calculation or undefined value. They can be found built into the query language or in the [`io.deephaven.util.QueryConstants`](/core/javadoc/io/deephaven/util/QueryConstants.html) Groovy package. The following example uses NaN values in formulas where a normal calculation would divide by 0, which is undefined.

```groovy order=result,source
import io.deephaven.util.QueryConstants

f = { index ->
    if (index == 0) {
        return NAN_DOUBLE
    } else {
        return 1 / index
    }
}

source = emptyTable(10).update("X = i")

result = source.update("Y = (i == 0) ? NAN_DOUBLE : 1 / i", "Z = f(i)")
```

## Appendix: Operators

### Arithmetic operators

| Symbol | Name           | Description                                                          |
| ------ | -------------- | -------------------------------------------------------------------- |
| `+`    | Addition       | Adds values.                                                         |
| `-`    | Subtraction    | Subtracts the right value from the left value.                       |
| `*`    | Multiplication | Multiplies the left and right values.                                |
| `/`    | Division       | Divides the left value by the right value.                           |
| `%`    | Modulus        | Divides the left value by the right value and returns the remainder. |

### Access operators

| Symbol | Name       | Description                                        |
| ------ | ---------- | -------------------------------------------------- |
| `_`    | Underscore | Accesses an array of all values within the column. |
| `[]`   | Index      | Indexes array elements.                            |
| `.`    | Dot        | Accesses members of a package or a class.          |

### Comparison operators

| Symbol | Name                  | Description                                                                               |
| ------ | --------------------- | ----------------------------------------------------------------------------------------- |
| `==`   | Equal to              | Compares two values to see if they are equal.                                             |
| `!=`   | Not equal to          | Compares two values to see if they are not equal.                                         |
| `>`    | Greater than          | Compares two values to see if the left value is greater than the right value.             |
| `>=`   | Greater than or equal | Compares two values to see if the left value is greater than or equal to the right value. |
| `<`    | Less than             | Compares two values to see if the left value is less than the right value.                |
| `<=`   | Less than or equal    | Compares two values to see if the left value is less than or equal to the right value.    |

### Assignment operators

| Symbol | Name                      | Description                                                                                   |
| ------ | ------------------------- | --------------------------------------------------------------------------------------------- |
| `=`    | Assignment                | Assigns a value to a variable.                                                                |
| `+=`   | Addition assignment       | Adds the right operand to the left operand and assigns the result to the left operand.        |
| `-=`   | Subtraction assignment    | Subtracts the right operand from the left operand and assigns the result to the left operand. |
| `++`   | Increment                 | Adds one to the value of the operand.                                                         |
| `--`   | Decrement                 | Subtracts one from the value of the operand.                                                  |
| `*=`   | Multiplication assignment | Multiplies the left operand by the right operand and assigns the result to the left operand.  |
| `/=`   | Division assignment       | Divides the left operand by the right operand and assigns the result to the left operand.     |
| `%=`   | Modulus assignment        | Divides the left operand by the right operand and assigns the remainder to the left operand.  |

### Logical operators

| Symbol            | Name        | Description                             |
| ----------------- | ----------- | --------------------------------------- |
| `!`               | Logical NOT | Inverts the value of a boolean.         |
| `&&`              | Logical AND | Returns true if both operands are true. |
| <code>\|\|</code> | Logical OR  | Returns true if either operand is true. |

### Bitwise operators

| Symbol | Name                   | Description                                                                                                                                                                                                      |
| ------ | ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `~`    | Bitwise complement     | A unary operator that 'flips' bits.                                                                                                                                                                              |
| `&`    | Bitwise AND            | Compares each bit of the first operand to the corresponding bit of the second operand. If both bits are 1, the corresponding result bit is set to 1. Otherwise, the corresponding result bit is set to 0.        |
| `<<`   | Left shift             | The left operand's value is shifted left by the number of bits set by the right operand.                                                                                                                         |
| `>>`   | Right shift            | The left operand's value is shifted right by the number of bits set by the right operand.                                                                                                                        |
| `^`    | Bitwise XOR            | Compares each bit of the first operand to the corresponding bit of the second operand. If the bits are different, the corresponding result bit is set to 1. Otherwise, the corresponding result bit is set to 0. |
| `&=`   | Bitwise AND assignment | Performs a bitwise AND on the left and right operands and assigns the result to the left operand.                                                                                                                |
| `^=`   | Bitwise XOR assignment | Performs a bitwise XOR on the left and right operands and assigns the result to the left operand.                                                                                                                |

### Other Java operators

| Symbol       | Name                | Description                                                               |
| ------------ | ------------------- | ------------------------------------------------------------------------- |
| `(type)`     | Casting             | Casts from one type to another.                                           |
| `()`         | Function call       | Calls a function.                                                         |
| `->`         | Java Lambda         | Defines a Java lambda function.                                           |
| `?:`         | Ternary conditional | Returns one of two values depending on the value of a boolean expression. |
| `instanceof` | Instance of         | Returns true if the object is an instance of the class.                   |

## Related documentation

- [Create new and empty tables](./new-and-empty-table.md)
- [How to use filters](./use-filters.md)
- [How to use select, view, and update](./use-select-view-update.md)
- [User-Defined Functions](../how-to-guides/user-defined-functions.md)
