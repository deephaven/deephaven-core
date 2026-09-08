---
title: Operators
---

An operator is a symbol, like `+` or `-`, that tells a program to perform a specific action on values or variables. Operators can be used to construct [filters](./filters.md) and [formulas](./formulas.md). Deephaven provides access to [most of Java's operators](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/operators.html). A complete list of supported operators is provided [below](#available-operators).

The following code block uses a variety of operators to create new columns of values.

```groovy
class MyObj {
    int a, b, c
    
    MyObj(int a, int b, int c) {
        this.a = a
        this.b = b
        this.c = c
    }

    int getA() {
        return a
    }
    
    int compute(int value1) {
        return this.a + value1
    }
}

obj = new MyObj(1, 2, 3)

result = emptyTable(10).update(
    "A = i",
    "B = A * A",
    "C = A / 2",
    "D = A % 3",
    "E = (int)C",
    "F = A_[ii-2]",
    "G = obj.getA()",
    "H = (int)obj.compute(A)",
    "I = sqrt(A)",
)
```

In the example below, comparison operators are used to filter data from the `source` table.

```groovy order=source,greaterThan,greaterThanOrEqual,lessThan,lessThanOrEqual
source = newTable(intCol("Value", 0, 1, 2, 3, 4, 5, 6))

greaterThan = source.where("Value > 3")
greaterThanOrEqual = source.where("Value >= 3")
lessThan = source.where("Value < 3")
lessThanOrEqual = source.where("Value <= 3")
```

## Available operators

There are many operators available in the Deephaven Query Language (DQL). They are organized by category in the subsections below.

### Arithmetic operators

| Symbol | Name           | Description                                                          |
| ------ | -------------- | -------------------------------------------------------------------- |
| `+`    | Addition       | Adds values.                                                         |
| `-`    | Subtraction    | Subtracts the right value from the left value.                       |
| `*`    | Multiplication | Multiplies the left and right values.                                |
| `/`    | Division       | Divides the left value by the right value.                           |
| `%`    | Modulus        | Divides the left value by the right value and returns the remainder. |

### Access operators

| Symbol | Name       | Description                                                                 |
| ------ | ---------- | --------------------------------------------------------------------------- |
| `_`    | Underscore | Accesses an [array](./work-with-arrays.md) of all values within the column. |
| `[]`   | Index      | Indexes array elements.                                                     |
| `.`    | Dot        | Accesses members of a package or a class.                                   |

> [!NOTE]
> The [`_`](./query-string-overview.md#arrays) operator is Deephaven-specific; it is not part of standard Java syntax.

### Comparison operators

| Symbol | Name                  | Description                                                                               |
| ------ | --------------------- | ----------------------------------------------------------------------------------------- |
| `==`   | Equal to              | Compares two values to see if they are equal.                                             |
| `!=`   | Not equal to          | Compares two values to see if they are not equal.                                         |
| `>`    | Greater than          | Compares two values to see if the left value is greater than the right value.             |
| `>=`   | Greater than or equal | Compares two values to see if the left value is greater than or equal to the right value. |
| `<`    | Less than             | Compares two values to see if the left value is less than the right value.                |
| `<=`   | Less than or equal    | Compares two values to see if the left value is less than or equal to the right value.    |

> [!NOTE]
> Null values are considered less than any non-null value for sorting and comparison purposes. Therefore, `<` and `<=` comparisons will always include `null`. To prevent this behavior, you can add an explicit null check; for example: `!isNull(value) && value < 10`.

> [!NOTE]
> Comparison operators on floating-point values follow standard IEEE 754 rules for handling `NaN` values. Any comparison involving `NaN` returns `false`, except for `!=`, which returns `true` for all values. To include `NaN` values in your comparisons, you can use the set inclusion operators ("in"/"not in"). For example: `value in NaN, 10.0` will return true if `value` is `NaN` or `10.0`. Alternatively, use the `isNaN(value)` function to explicitly test for NaN values, such as `isNaN(value) || value < 10.0`.

### Assignment operators

| Symbol | Name       | Description                    |
| ------ | ---------- | ------------------------------ |
| `=`    | Assignment | Assigns a value to a variable. |

### Logical operators

| Symbol            | Name        | Description                                 |
| ----------------- | ----------- | ------------------------------------------- |
| `!`               | Logical NOT | Inverts the value of a boolean.             |
| `&`               | Bitwise AND | Performs a bitwise AND operation.           |
| <code>\|</code>   | Bitwise OR  | Performs a bitwise OR operation.            |
| `&&`              | Logical AND | Returns `true` if both operands are `true`. |
| <code>\|\|</code> | Logical OR  | Returns `true` if either operand is `true`. |

### Bitwise operators

| Symbol | Name               | Description                                                                                                                                                                                                      |
| ------ | ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `~`    | Bitwise complement | A unary operator that "flips" bits.                                                                                                                                                                              |
| `&`    | Bitwise AND        | Compares each bit of the first operand to the corresponding bit of the second operand. If both bits are 1, the corresponding result bit is set to 1. Otherwise, the corresponding result bit is set to 0.        |
| `<<`   | Left shift         | The left operand's value is shifted left by the number of bits set by the right operand.                                                                                                                         |
| `>>`   | Right shift        | The left operand's value is shifted right by the number of bits set by the right operand.                                                                                                                        |
| `^`    | Bitwise XOR        | Compares each bit of the first operand to the corresponding bit of the second operand. If the bits are different, the corresponding result bit is set to 1. Otherwise, the corresponding result bit is set to 0. |

### Other Java operators

| Symbol       | Name                                          | Description                                                               |
| ------------ | --------------------------------------------- | ------------------------------------------------------------------------- |
| `(type)`     | [Casting](./casting.md)                       | Casts from one type to another.                                           |
| `()`         | Function call                                 | Calls a function.                                                         |
| `?:`         | [Ternary conditional](./ternary-if-how-to.md) | Returns one of two values depending on the value of a boolean expression. |
| `instanceof` | Instance of                                   | Returns `true` if the object is an instance of the class.                 |

## Related documentation

- [Query strings](./query-string-overview.md)
- [Filters](./filters.md)
- [Formulas](./formulas.md)
- [Filter table data](./use-filters.md)
- [Select and update columns](./use-select-view-update.md)
- [Groovy classes in query strings](./groovy-classes.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`newTable`](../reference/table-operations/create/newTable.md)
