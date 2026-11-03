---
title: Casting
---

Data type conversion, or casting for short, is the process of changing a value from one data type to another. This guide shows how to use type casts in [query strings](./query-string-overview.md) to control column types. It covers common numeric and string conversions, then casts on values from Groovy variables, closures, and classes.

In tables, casting gives a column the data type that later operations need. Casting to a Java primitive type also stores the values in compact primitive columns instead of `Object` or `BigDecimal` columns.

## Type casts

A type cast is an expression that converts a value from one data type to another:

```groovy order=null
a = 3
b = (double)a
```

In Deephaven, type casts in [query strings](./query-string-overview.md) convert a value or column to another data type. They use [Java syntax](https://www.w3schools.com/java/java_type_casting.asp) and follow these rules:

- **Between primitives**: a cast between two numeric [primitive types](./data-types.md#primitive-types) widens or narrows the value:
  - Widening, such as `(long)` on an `int`, keeps the value.
  - Narrowing, such as `(int)` on a `double`, can lose information. `(int)` drops a `double`'s fractional part.
  - A value outside the target type's range wraps or clamps. A `double` outside the `float` range becomes infinity.
  - A result that equals the target type's [null constant](../reference/query-language/types/nulls.md) reads as null. For `byte`, that constant is `-128`.
- **Boxed to primitive**: a boxed type is a Java object that wraps a primitive value, such as `java.lang.Double`. A cast from a boxed type to a primitive type works only for the matching primitive type or a wider one.
- **Other objects to primitive**: a cast from any other object type, such as `java.lang.Object` or `java.math.BigDecimal`, to a numeric primitive type other than `char` converts the value with the matching `java.lang.Number` method, such as `doubleValue`. It works whenever the value is a number, even where a plain Java cast would fail or not compile.
- **To an object type**: a cast to an object type, such as `(String)`, is a plain Java cast. It sets the column type. It works only when the value is already an instance of that type, or when it boxes a primitive into its own boxed type or a supertype, such as `(Integer)` or `(Object)` on an `int`. `(String)` is short for `(java.lang.String)`.
- **Null safety**: casts to primitive types other than `boolean` are null-safe. A null value becomes the [null constant](../reference/query-language/types/nulls.md) of the target type.

For more casting examples, see the [casting reference](../reference/query-language/control-flow/casting.md).

## Common casting scenarios

### Numeric conversions

The following query applies a numeric cast to each column:

- `ByteVal`: `(byte)128` overflows to `-128`. That value is the `byte` [null constant](../reference/query-language/types/nulls.md), so `ByteVal` reads as null.
- `ShortVal`: `(short)32767` is the largest `short` value, so it fits and is unchanged.
- `IntVal`: `(int)3.99` truncates to `3`.
- `LongVal` and `DoubleVal`: `(long)` and `(double)` widen the `int` column `IntVal`.
- `FloatVal`: `(float)` rounds a `double` literal to `float` precision.

The `sourceMeta` table, from [`meta`](../reference/table-operations/metadata/meta.md), lists each column's resulting type:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "ByteVal = (byte)128",
    "ShortVal = (short)32767",
    "IntVal = (int)3.99",
    "LongVal = (long)IntVal",
    "FloatVal = (float)3.14159265359",
    "DoubleVal = (double)IntVal",
)
sourceMeta = source.meta()
```

### String conversions

A Java cast can't convert between [strings](./work-with-strings.md) and numbers, so call a conversion function instead. [`parseInt`](../reference/query-language/query-library/auto-imported/parse.md) parses a string as an `int`, and `Integer.toString` formats an `int` as a string:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "StringNum = `123`",
    "IntFromString = parseInt(StringNum)",
    "IntToString = Integer.toString(456)",
)
sourceMeta = source.meta()
```

## Casts with Groovy objects

How you cast a value from Groovy depends on where it comes from: a [Groovy variable](./groovy-variables.md), a [Groovy closure](./groovy-closures.md), or a method of a [Groovy class](./groovy-classes.md). The following sections cover each.

### Groovy variables

Groovy decimal literals such as `3.14159` are [`java.math.BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) objects, so a [Groovy variable](./groovy-variables.md) holding one produces a `BigDecimal` column. A type cast converts it to a [Java primitive](./data-types.md#primitive-types), following the **Other objects to primitive** rule under [Type casts](#type-casts).

`BigDecimal` has arbitrary precision and primitives do not, so the cast can round the value. In the following query, `(float)` rounds `a` to `float` precision:

```groovy order=source,sourceMeta
a = 3.14159

source = emptyTable(1).update(
    "BigDecimalA = a",
    "DoubleA = (double)a",
    "FloatA = (float)a",
)
sourceMeta = source.meta()
```

### Groovy closures

[Groovy closures called in query strings](./groovy-closures.md) return `java.lang.Object`, because the query engine calls the closure's generic `call` method and does not see the types the closure declares. Use a type cast when you need a specific column type for the result.

The following example uses a type cast to convert the return value of a [Groovy closure](./groovy-closures.md) to a [`java.lang.String`](../reference/query-language/types/strings.md):

```groovy order=source
randomSymbol = {
    def symbols = ["AAPL", "GOOG", "MSFT"]
    return symbols[new Random().nextInt(symbols.size())]
}

source = emptyTable(10).update("Symbol = (String)randomSymbol()")
```

### Groovy classes

Methods of [Groovy classes](./groovy-classes.md) called in [query strings](./query-string-overview.md) produce columns of their declared return type. A method declared with `def` returns `java.lang.Object`, so use a type cast when you need a specific column type for its result. The **Other objects to primitive** rule under [Type casts](#type-casts) makes a numeric cast such as `(int)` work on that `Object`.

The following example casts the results of the static method `multiplyModulo` and the instance method `plusModulo`. It first imports `MyClass` into the [query library](../conceptual/execution-context.md) so the [query strings](./query-string-overview.md) can call its static methods by class name. [Groovy classes](./groovy-classes.md) explains this import in more detail.

```groovy order=source,sourceMeta
class MyClass {
    public int x
    public int y

    MyClass(int x, int y) {
        this.x = x
        this.y = y
    }

    static int changeValue(int newValue) {
        return newValue
    }

    static def multiplyModulo(int x, int y, int modulo) {
        if (modulo == 0) {
            return x * y
        }
        return (x % modulo) * (y % modulo)
    }

    def plusModulo(int modulo) {
        if (modulo == 0) {
            return this.x + this.y
        }
        return (this.x % modulo) + (this.y % modulo)
    }
}

// Import the class into the query library so query strings can call its static methods by class name
ExecutionContext.getContext().getQueryLibrary().importClass(MyClass.class)

myClass = new MyClass(15, 6)

source = emptyTable(1).update(
    "X = MyClass.changeValue(5)",
    "Q = MyClass.multiplyModulo(11, 16, X)",
    "Y = (double)MyClass.multiplyModulo(11, 16, X)",
    "Z = (int)myClass.plusModulo(X)",
)
sourceMeta = source.meta()
```

Column `X` needs no cast, because `changeValue` declares an `int` return type. Column `Q` has no cast, so `sourceMeta` shows its type as `java.lang.Object`. The casts give `Y` the `double` type and `Z` the `int` type.

## Related documentation

- [Execution context](../conceptual/execution-context.md)
- [Access metadata](./metadata.md)
- [Data types](./data-types.md)
- [Query string overview](./query-string-overview.md)
- [Query scope](./query-scope.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Groovy classes in query strings](./groovy-classes.md)
- [Casting](../reference/query-language/control-flow/casting.md)
- [Nulls](../reference/query-language/types/nulls.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [Parse functions](../reference/query-language/query-library/auto-imported/parse.md)
- [`update`](../reference/table-operations/select/update.md)
- [`emptyTable` Javadoc](/core/javadoc/io/deephaven/engine/util/TableTools.html#emptyTable(long))
- [`ExecutionContext` Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
- [`QueryLibrary` Javadoc](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html)
- [`meta` Javadoc](/core/javadoc/io/deephaven/engine/table/Table.html#meta())
- [`update` Javadoc](/core/javadoc/io/deephaven/api/TableOperations.html#update(java.lang.String...))
