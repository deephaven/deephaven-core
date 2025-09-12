---
title: Java objects and classes in query strings
sidebar_label: Java classes & objects
---

The ability to use Java objects and classes in Deephaven query strings is one of its most powerful features. This guide explains how to use Java objects effectively in your queries, leveraging Groovy's seamless Java interoperability.

## Classes and objects in Java

Java is an object-oriented programming language where classes serve as blueprints for creating objects. Like Groovy, Java distinguishes between primitive types and objects:

1. **Primitive types**: Basic data types like `int`, `double`, and `boolean` that are not objects.
2. **Objects**: Instances of classes that bundle data and methods together.

For performance reasons, Deephaven tables primarily use [primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) for numeric columns, but many operations require object types.

### The root Java object

All objects in Java inherit from the [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) class. Common Java objects used in Deephaven queries include:

- [`java.lang.String`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/String.html) - For text data
- [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) - For timestamp data
- [`java.math.BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html) - For high-precision numeric data

## Variables

Java classes can have two types of variables:

1. **Static variables** (class variables): Belong to the class itself, not to instances. They are shared across all instances of the class.
2. **Instance variables**: Belong to individual object instances created from the class.

In query strings, you access static variables using the class name, while instance variables require an object instance. The following example demonstrates accessing both static and instance variables:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "StaticVariable = Integer.MAX_VALUE",
    "Instance = new int[]{1,2,3}",
    "InstanceVariable = Instance.length",
)
sourceMeta = source.meta()
```

## Methods

Java classes can have three types of methods:

1. **Static methods**: Called on the class itself, not on instances
2. **Instance methods**: Called on object instances
3. **Constructor methods**: Special methods used to create new instances with the `new` keyword

All three are supported in query strings.

The following example demonstrates all three method types by calling static methods, creating instances with constructors, and calling instance methods:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "RandomValueStaticMethod = Math.random()",
    "MaxValueStaticMethod = Math.max(10, 20)",
    "MyUUIDConstructorMethod = new java.util.UUID(123456789, 987654321)",
    "UUIDStringInstanceMethod = MyUUIDConstructorMethod.toString()",
)
sourceMeta = source.meta()
```

## Built-in query language methods

The Deephaven Query Language (DQL) has a large number of [built-in functions](./built-in-functions.md) that take Java objects as input.

For example, [`java.time.Instant`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Instant.html) and [`java.time.Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) are common in tables. Many methods in [`io.deephaven.time.DateTimeUtils`](https://docs.deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html) (built into the query language by default) accept these object types as input.

The following example demonstrates several [built-in methods](./built-in-functions.md) that work with Java objects in [query strings](./query-string-overview.md):

```groovy order=source,sourceMeta
source = emptyTable(10).update(
    "Timestamp = '2025-09-01T09:30:00 ET' + ii * MINUTE",
    "InstantMinusDuration = minus(Timestamp, 'PT5s')",
    "InstantPlusDuration = plus(Timestamp, 'PT10s')",
    "UpperBin = upperBin(Timestamp, 'PT15m')",
    "LowerBin = lowerBin(Timestamp, 'PT8m')",
)
sourceMeta = source.meta()
```

## Groovy's Java interoperability

One of Groovy's greatest strengths is its seamless interoperability with Java. In Groovy query strings, you can:

- Use any Java class without imports (for classes in `java.lang` and other common packages)
- Access Java static methods and variables directly
- Create Java objects using the `new` keyword
- Call Java instance methods on objects

This makes working with Java objects in Groovy query strings very natural and straightforward.

## Related documentation

- [Query string overview](./query-string-overview.md)
- [String and char literals in query strings](./string-char-literals.md)
- [Date-time literals in query strings](./date-time-literals.md)
- [Groovy closures in query strings](./groovy-closures.md)
- [Built-in functions](./built-in-functions.md)
- [Built-in constants](./built-in-constants.md)
- [Work with arrays](./work-with-arrays.md)
- [Work with strings](./strings.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`update`](../reference/table-operations/select/update.md)
