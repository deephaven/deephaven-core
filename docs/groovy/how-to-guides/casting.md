---
title: Casting
sidebar_label: Casting
---

This guide covers casting in Deephaven. Data type conversion, or casting for short, is the process of changing a value from one data type to another.

In tables, casting is essential to ensure that columns are in the correct format for subsequent operations. It's also vital for optimizing performance and managing memory effectively.

## Type casts

A type cast is an expression that converts a value from one data type to another. In Groovy, type casts use the syntax:

```groovy order=null
a = 3
b = (double) a
```

In Deephaven, type casts in [query strings](./query-string-overview.md) ensure that columns convert the data type of a column to another. They use [Java syntax](https://www.w3schools.com/java/java_type_casting.asp).

### Groovy variables

The following query creates a column using a [Groovy variable](./groovy-variables.md). The column is automatically inferred as a `BigDecimal` type. A type cast is used to convert the variable to a Java primitive `double`:

```groovy order=source,sourceMeta
a = 1.24

source = emptyTable(1).update(
    "IntegerA = a",
    "IntA = (double)a",
)
sourceMeta = source.meta()
```

Type casting [Groovy variables in query strings](./groovy-variables.md) has limitations. Unlike Groovy, where numeric types can be automatically promoted, Java primitives have fixed precision. You cannot cast a value to a type with smaller precision without potential data loss. For example, the following query may lose precision:

```groovy order=source,sourceMeta
a = 3.14159

source = emptyTable(1).update(
    "DoubleA = a",
    "FloatA = (float)a",
)
sourceMeta = source.meta()
```

The Groovy `BigDecimal` type (default for decimal literals) corresponds most closely to the Java `double` type in terms of precision. When a Groovy decimal is used in a query string, explicit casting ensures the correct Java primitive type is used.

### Groovy closures

[Groovy closures called in query strings](./groovy-closures.md) should generally use explicit parameter and return type declarations rather than relying solely on type casts. However, type casts can still be used when needed to ensure the correct column data type.

The following example uses a type cast to convert the returned value of a Groovy closure to a [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html):

> [!NOTE]
> The shorthand notation `(String)` is used below. The full notation is `(java.lang.String)`.

```groovy order=source
randomSymbol = {
    def symbols = ["AAPL", "GOOG", "MSFT"]
    return symbols[new Random().nextInt(symbols.size())]
}

source = emptyTable(10).update("Symbol = (String)randomSymbol()")
```

### Groovy classes

[Groovy classes](./groovy-classes.md) used in [query strings](./query-string-overview.md) benefit from explicit type casts to ensure columns have the appropriate data types. The example below demonstrates using type casts with Groovy class methods and attributes in query strings:

```groovy order=source,sourceMeta
class MyClass {
    static int myValue = 3
    
    public int x
    public int y

    MyClass(int x, int y) {
        this.x = x
        this.y = y
    }

    static int changeValue(int newValue) {
        MyClass.myValue = newValue
        return newValue
    }

    static double multiplyModulo(int x, int y, int modulo) {
        if (modulo == 0) {
            return x * y
        }
        return (x % modulo) * (y % modulo)
    }

    int plusModulo(int modulo) {
        if (modulo == 0) {
            return this.x + this.y
        }
        return (this.x % modulo) + (this.y % modulo)
    }
}

// Make the class available in the query scope
getBinding().setVariable("MyClass", MyClass)

myClass = new MyClass(15, 6)

// Access static variable directly, not through class
value1 = MyClass.changeValue(4)
value2 = MyClass.changeValue(5)

source = emptyTable(1).update(
    "Q = value1",
    "X = value2",
    "Y = (double)myClass.multiplyModulo(11, 16, X)",
    "Z = (int)myClass.plusModulo(X)",
)
sourceMeta = source.meta()
```

## Common casting scenarios

### Numeric conversions

When working with numeric data, explicit casting ensures the correct Java primitive type:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "ByteVal = (byte)127",
    "ShortVal = (short)32767",
    "IntVal = (int)2147483647",
    "LongVal = (long)9223372036854775807L",
    "FloatVal = (float)3.14f",
    "DoubleVal = (double)3.14159265359",
)
sourceMeta = source.meta()
```

### String conversions

Converting between strings and other types:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "StringNum = `123`",
    "IntFromString = (int)parseInt(StringNum)",
    "IntToString = (String)Integer.toString(456)",
)
sourceMeta = source.meta()
```

## Related documentation

- [Access metadata](./metadata.md)
- [Query string overview](./query-string-overview.md)
- [Query scope](./queryscope.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in query strings](./groovy-closures.md)
- [Groovy classes in query strings](./groovy-classes.md)
- [Data types](./data-types.md)
