---
title: Java objects and classes in query strings
---

Using Java classes and objects directly in [query strings](./query-string-overview.md) is one of Deephaven's most powerful features. Your queries can use the whole Java standard library and any Java class on the server's classpath, just as Groovy's seamless Java interoperability lets your scripts use them. This guide shows how to:

- Use static and instance variables, methods, and constructors in [query strings](./query-string-overview.md).
- Use Java classes that aren't imported by default.
- Refer to nested classes.
- Pass Java objects to [built-in query language functions](./built-in-functions.md).

You don't need much Java experience to follow it.

## Classes and objects in Java

Java is an object-oriented programming language where classes serve as blueprints for creating objects. Java distinguishes between primitive types and objects:

1. **Primitive types**: Basic data types like `int`, `double`, and `boolean` that are not objects.
2. **Objects**: Instances of classes that bundle data and methods together.

For performance reasons, Deephaven tables primarily use [primitive types](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html) for numeric columns. Other columns, such as text and timestamp columns, hold Java objects. [query strings](./query-string-overview.md) can call those objects' methods, as the rest of this guide shows.

### Common Java objects in Deephaven

All objects in Java inherit from the [`java.lang.Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) class. Common Java objects used in Deephaven queries include:

- [`java.lang.String`](../reference/query-language/types/strings.md): text data
- [`java.time.Instant`](../reference/query-language/types/date-time.md#instant): timestamp data
- [`java.math.BigDecimal`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/math/BigDecimal.html): high-precision numeric data

## Variables

Java classes can have two types of variables:

1. **Static variables** (class variables): Belong to the class itself, not to instances. They are shared across all instances of the class.
2. **Instance variables**: Belong to individual object instances created from the class.

In [query strings](./query-string-overview.md), you access a static variable through its class name and an instance variable through an object. The following example reads the static variable `Integer.MAX_VALUE`, creates an `int` array, and reads the array's `length` instance variable. It creates the array with the `new` keyword, which [Methods and constructors](#methods-and-constructors) explains:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "StaticVariable = Integer.MAX_VALUE",
    "Instance = new int[]{1, 2, 3}",
    "InstanceVariable = Instance.length",
)
sourceMeta = source.meta()
```

The `sourceMeta` table lists each column's type: `Instance` holds an `int[]` object, and `StaticVariable` and `InstanceVariable` are `int` values.

## Methods and constructors

[Query strings](./query-string-overview.md) can use three kinds of Java methods and constructors:

1. **Static methods**: Called on the class itself, not on instances.
2. **Constructors**: Create new instances with the `new` keyword.
3. **Instance methods**: Called on object instances.

The following example calls static methods of `Math`, creates a `UUID` with a constructor, and calls the new object's `toString` instance method. The [query library](#import-java-classes) imports both `Math` and `UUID` by default, so the example needs no import:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "RandomValueStaticMethod = Math.random()",
    "MaxValueStaticMethod = Math.max(10, 20)",
    "UUIDConstructor = new UUID(123456789, 987654321)",
    "UUIDStringInstanceMethod = UUIDConstructor.toString()",
)
sourceMeta = source.meta()
```

In `sourceMeta`, `UUIDConstructor` is a `java.util.UUID` column, and `UUIDStringInstanceMethod` is a [`String`](../reference/query-language/types/strings.md) column.

## Import Java classes

[Query strings](./query-string-overview.md) can use many Java classes by their simple names, with no import step. A simple name is the class name alone, such as `Math`. A fully qualified name also includes the package, such as `java.lang.Math`.

The [query library](../conceptual/execution-context.md) holds the imports that [query strings](./query-string-overview.md) can use. By default, it includes:

- The classes in the `java.lang` and `java.util` packages, such as `Integer`, `Math`, and `UUID`.
- Specific classes from other packages, such as [`java.time.Instant`](../reference/query-language/types/date-time.md#instant) and [`java.time.Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html).
- The static members of classes such as [`io.deephaven.time.DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html). [Query strings](./query-string-overview.md) can call these members without the class name.

For the complete list, see [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md).

A Groovy `import` statement doesn't make a class available in [query strings](./query-string-overview.md), which are compiled separately from your Groovy script.

To use a class that isn't imported by default, refer to it by its fully qualified name. You can also add it to the [query library](../conceptual/execution-context.md) of the current [execution context](../conceptual/execution-context.md) with [`ExecutionContext.getContext().getQueryLibrary().importClass`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importClass(java.lang.Class)) and then use its simple name. Either way, the class must be on the server's classpath. To add your own JAR, see [Install and use Java packages](./install-and-use-java-packages.md).

The following example adds [`java.net.URI`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/URI.html) to the [query library](../conceptual/execution-context.md) and uses its simple name. The `LoopbackName` column uses [`java.net.InetAddress`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/net/InetAddress.html) by its fully qualified name, with no import. The URI is a [string literal](./string-char-literals.md), which [query strings](./query-string-overview.md) delimit with backticks:

```groovy order=source reset
// Add java.net.URI to the query library so query strings can use its simple name
ExecutionContext.getContext().getQueryLibrary().importClass(java.net.URI.class)

source = emptyTable(1).update(
    "Address = new URI(`https://deephaven.io/core/docs/`)",
    "Host = Address.getHost()",
    "Path = Address.getPath()",
    "LoopbackName = java.net.InetAddress.getLoopbackAddress().getHostName()",
)
```

[`QueryLibrary`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html) has two other import methods:

- [`importStatic`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importStatic(java.lang.Class)) imports a class's static members.
- [`importPackage`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importPackage(java.lang.Package)) imports every public class in a package.

For an [`importStatic`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importStatic(java.lang.Class)) example, see [Add Java classes to the query library](./built-in-functions.md#add-java-classes-to-the-query-library).

## Nested classes

Java lets you define a class inside another class. In a [query string](./query-string-overview.md), you refer to a nested class with dot notation, such as `OuterClass.NestedClass`. This is standard Java syntax.

The following example creates a [`java.util.AbstractMap.SimpleEntry`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/AbstractMap.SimpleEntry.html), a class nested inside `java.util.AbstractMap`, and reads its key and value. `AbstractMap` is in `java.util`, so the example needs no import:

```groovy order=source,sourceMeta
source = emptyTable(1).update(
    "Pair = new AbstractMap.SimpleEntry(`Price`, 101.5)",
    "Key = (String) Pair.getKey()",
    "Value = (double) Pair.getValue()",
)
sourceMeta = source.meta()
```

The example uses `SimpleEntry` without type parameters, so `getKey` and `getValue` return [`Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html). The casts make `Key` a [`String`](../reference/query-language/types/strings.md) column and `Value` a `double` column, as `sourceMeta` shows. Without them, both columns would hold [`Object`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Object.html) values.

A [query string](./query-string-overview.md) can also refer to a nested class by its fully qualified name, such as `java.util.AbstractMap.SimpleImmutableEntry`. To use the nested class's simple name alone, add it to the [query library](../conceptual/execution-context.md) with [`importClass`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importClass(java.lang.Class)). The following example imports [`java.util.AbstractMap.SimpleImmutableEntry`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/AbstractMap.SimpleImmutableEntry.html) and then uses both its simple name and its fully qualified name:

```groovy order=source reset
ExecutionContext.getContext().getQueryLibrary().importClass(java.util.AbstractMap.SimpleImmutableEntry.class)

source = emptyTable(1).update(
    "Bid = new SimpleImmutableEntry(`Bid`, 99.5)",
    "Ask = new java.util.AbstractMap.SimpleImmutableEntry(`Ask`, 100.5)",
    "BidKey = (String) Bid.getKey()",
    "AskValue = (double) Ask.getValue()",
)
```

As [Import Java classes](#import-java-classes) explains, a Groovy `import` of a nested class doesn't make the class available in [query strings](./query-string-overview.md).

## Built-in query language functions

The Deephaven Query Language (DQL) has a large number of [built-in functions](./built-in-functions.md) that take Java objects as input.

For example, [`java.time.Instant`](../reference/query-language/types/date-time.md#instant) and [`java.time.Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) are common in tables. Many methods in [`io.deephaven.time.DateTimeUtils`](/core/javadoc/io/deephaven/time/DateTimeUtils.html) accept these object types as input. The [query library](../conceptual/execution-context.md) imports these methods by default, as described in [Import Java classes](#import-java-classes).

The following example passes [`Instant`](../reference/query-language/types/date-time.md#instant) and [`Duration`](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/time/Duration.html) objects to the built-in functions [`minus`](../reference/time/datetime/minus.md), [`plus`](../reference/time/datetime/plus.md), [`upperBin`](../reference/time/datetime/upperBin.md), and [`lowerBin`](../reference/time/datetime/lowerBin.md):

- The `Timestamp` column starts at a [date-time literal](./date-time-literals.md) and adds one minute per row. [`ii`](../reference/query-language/variables/special-variables.md) is the row index, and [`MINUTE`](./built-in-constants.md#date-time-constants) is a [built-in constant](./built-in-constants.md).
- The durations, such as `'PT5s'`, are [duration literals](./date-time-literals.md#duration).
- In the result, `UpperBin` and `LowerBin` round each timestamp up or down to a 15-minute boundary.
- `sourceMeta` shows that every column holds [`Instant`](../reference/query-language/types/date-time.md#instant) values.

```groovy order=source,sourceMeta
source = emptyTable(10).update(
    "Timestamp = '2025-09-01T09:30:00 ET' + ii * MINUTE",
    "InstantMinusDuration = minus(Timestamp, 'PT5s')",
    "InstantPlusDuration = plus(Timestamp, 'PT10s')",
    "UpperBin = upperBin(Timestamp, 'PT15m')",
    "LowerBin = lowerBin(Timestamp, 'PT15m')",
)
sourceMeta = source.meta()
```

## Related documentation

- [Execution context](../conceptual/execution-context.md)
- [Query string overview](./query-string-overview.md)
- [String and char literals in query strings](./string-char-literals.md)
- [Date-time literals in query strings](./date-time-literals.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Groovy classes and objects in query strings](./groovy-classes.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [Built-in functions](./built-in-functions.md)
- [Built-in constants](./built-in-constants.md)
- [Work with arrays](./work-with-arrays.md)
- [Work with strings](./work-with-strings.md)
- [Auto-imported functions](../reference/query-language/query-library/auto-imported/index.md)
- [Strings](../reference/query-language/types/strings.md)
- [date-time](../reference/query-language/types/date-time.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`update`](../reference/table-operations/select/update.md)
- [`lowerBin`](../reference/time/datetime/lowerBin.md)
- [`minus`](../reference/time/datetime/minus.md)
- [`plus`](../reference/time/datetime/plus.md)
- [`upperBin`](../reference/time/datetime/upperBin.md)
- [`DateTimeUtils` Javadoc](/core/javadoc/io/deephaven/time/DateTimeUtils.html)
- [`QueryLibrary` Javadoc](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html)
- [`ExecutionContext` Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
