---
title: Groovy classes and objects in query strings
---

Using your own Groovy [variables](./groovy-variables.md), [closures](./groovy-closures.md), classes, and objects in Deephaven [query strings](./query-string-overview.md) is one of Deephaven's most powerful features. Using Groovy classes in query strings follows a few basic rules. This guide shows how to make a Groovy class available to query strings and how to use its variables and methods there.

## Classes and objects in Groovy

Groovy is an object-oriented programming language. Its design revolves around objects, which bundle data (fields) with the code (methods) that operates on it.

Classes are blueprints for objects. They define the fields and methods of every object created from the class. A member declared `static` belongs to the class itself, so you can use it without creating an object. Other members are instance members. Each object has its own copy of the instance variables, and instance methods run on a particular object.

## Make a Groovy class available to query strings

[Query strings](./query-string-overview.md) can refer by simple name only to the classes, packages, and static members in the [query library](../conceptual/execution-context.md). Deephaven compiles [query strings](./query-string-overview.md) separately from your Groovy script, so a class your script defines, such as `MyClass`, isn't in the [query library](../conceptual/execution-context.md) until you add it.

To use the class name in [query strings](./query-string-overview.md), add the class to the [query library](../conceptual/execution-context.md) of the current [execution context](../conceptual/execution-context.md) with [`ExecutionContext.getContext().getQueryLibrary().importClass`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importClass(java.lang.Class)). The `MyClass` and `Calculator` examples below call it right after the class definition.

[Query strings](./query-string-overview.md) can use a class's members without the import in two cases:

- To use an object created from the class, assign it to a top-level script variable without `def` or a type, such as `myObj = new MyClass(2)`. That puts it in the [query scope](./query-scope.md), so a [query string](./query-string-overview.md) can use the object's public variables and methods. For why `def` and typed variables don't work, see [Groovy variables in query strings](./groovy-variables.md#rules-for-groovy-variables-in-query-strings).
- To use a static value, copy it to a top-level script variable the same way, such as `staticA = MyClass.a`, and use `staticA` in the [query string](./query-string-overview.md).

To reuse classes across scripts, keep them out of the console script:

- Put them in Groovy files on the classpath. See [Import closures from Groovy files](./groovy-closures.md#import-closures-from-groovy-files).
- Package them in a JAR. See [Install and use Java packages](./install-and-use-java-packages.md).

A Groovy `import` of such a class doesn't reach [query strings](./query-string-overview.md). Import it with [`importClass`](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html#importClass(java.lang.Class)) or use its fully qualified name.

## Static and instance variables in query strings

Like methods, variables are either [static or instance members](#classes-and-objects-in-groovy). In the example below, `a` is a static variable, so a [query string](./query-string-overview.md) can use it without an object. `b` is an instance variable, so a [query string](./query-string-overview.md) needs an object to read it.

Because the example imports `MyClass` into the [query library](../conceptual/execution-context.md), the [query string](./query-string-overview.md) can read `a` through the class name. It reads `b` through the object `myObj`, which works without the import:

```groovy order=source,sourceMeta reset
class MyClass {
    public static int a = 1
    public int b

    MyClass(int b) {
        this.b = b
    }
}

// Import the class into the query library so query strings can refer to it by name
ExecutionContext.getContext().getQueryLibrary().importClass(MyClass.class)

myObj = new MyClass(2)

source = emptyTable(1).update("X = MyClass.a", "Y = myObj.b")
sourceMeta = source.meta()
```

The [`meta`](../reference/table-operations/metadata/meta.md) table, `sourceMeta`, shows that `X` and `Y` are `int` columns, matching the declared types of `a` and `b`.

The example declares both variables `public`. A variable declared without an access modifier, such as `int b`, is a Groovy property:

- Groovy makes the field private, so [query strings](./query-string-overview.md) can't read it.
- Groovy generates a public getter for it, named after the property. The getter for `b` is `getB`, and a static property, such as `static int a`, gets a static getter, `getA`.

In a [query string](./query-string-overview.md), call the getter instead: `myObj.getB()` for an instance property or `MyClass.getA()` for a static one. The following example calls `getB`:

```groovy order=propertySource
class PropertyClass {
    int b

    PropertyClass(int b) {
        this.b = b
    }
}

propObj = new PropertyClass(2)

// "Y = propObj.b" fails: Groovy makes the field for property b private
propertySource = emptyTable(1).update("Y = propObj.getB()")
```

## Methods in query strings

Like variables, methods are either [static or instance members](#classes-and-objects-in-groovy). An instance method runs on one object and can read both its instance variables and the class's static variables. [Query strings](./query-string-overview.md) can call both kinds of methods.

The following code block calls two static methods through the imported class `Calculator` and an instance method through the instance `myObj`:

```groovy order=source,sourceMeta
class Calculator {
    public int x
    public int y

    Calculator(int x, int y) {
        this.x = x
        this.y = y
    }

    static int square(int value) {
        return value * value
    }

    static double multiplyModulo(int x, int y, int modulo) {
        if (modulo == 0) {
            return x * y
        }
        return ((x % modulo) * (y % modulo)) % modulo
    }

    int plusModulo(int modulo) {
        if (modulo == 0) {
            return this.x + this.y
        }
        return ((this.x % modulo) + (this.y % modulo)) % modulo
    }
}

// Import the class into the query library so query strings can call its static methods by class name
ExecutionContext.getContext().getQueryLibrary().importClass(Calculator.class)

myObj = new Calculator(15, 6)

source = emptyTable(1).update(
    "X = Calculator.square(3)",
    "Y = Calculator.multiplyModulo(11, 16, X)",
    "Z = myObj.plusModulo(X)"
)
sourceMeta = source.meta()
```

Each column takes its type from the method's declared return type, so the [`meta`](../reference/table-operations/metadata/meta.md) table, `sourceMeta`, shows that `X` and `Z` are `int` columns and `Y` is a `double` column.

A method declared with `def` instead of a return type returns `Object`. To get a specific column type from such a method, [cast](./casting.md#groovy-classes) its result in the [query string](./query-string-overview.md).

## Related documentation

- [Execution context](../conceptual/execution-context.md)
- [Query string overview](./query-string-overview.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in queries](./groovy-closures.md)
- [Java objects and classes in query strings](./java-classes.md)
- [Casting](./casting.md)
- [Query scope](./query-scope.md)
- [Install and use Java packages](./install-and-use-java-packages.md)
- [`emptyTable`](../reference/table-operations/create/emptyTable.md)
- [`meta`](../reference/table-operations/metadata/meta.md)
- [`update`](../reference/table-operations/select/update.md)
- [`ExecutionContext` Javadoc](/core/javadoc/io/deephaven/engine/context/ExecutionContext.html)
- [`QueryLibrary` Javadoc](/core/javadoc/io/deephaven/engine/context/QueryLibrary.html)
