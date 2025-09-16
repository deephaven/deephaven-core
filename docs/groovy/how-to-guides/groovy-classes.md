---
title: Groovy classes and objects in query strings
sidebar_label: Classes & Objects
---

The ability to use your own custom Groovy [variables](./groovy-variables.md), [closures](./groovy-closures.md), classes, and objects in Deephaven query strings is one of its most powerful features. The use of Groovy classes in query strings follows some basic rules, which are outlined in this guide.

## Classes and objects in Groovy

Groovy is an object-oriented programming language. Its design revolves around the concept of "objects", which can contain arbitrary data and code suited to any given task.

In Groovy, everything that is not a primitive value is an object. For example:

```groovy order=:log
myInt = 1

println myInt.getClass()
```

Classes are blueprints for objects. They define the properties and behaviors that the objects created from the class will have.

### Variables

Class variables can be either static or instance variables. In the example below, `a` is a static class variable and `b` is an instance variable. The variable `a` does _not_ require an instance of the class to be used, while `b` does. However, static variables from custom Groovy classes cannot be accessed directly in query strings (like `MyClass.a`), so we extract the static value to a regular variable first:

```groovy order=source,sourceMeta
class MyClass {
    static int a = 1
    int b

    MyClass(int b) {
        this.b = b
    }

    int getB() {
        return b
    }
}

myClass = new MyClass(2)

// Access static variable directly, not through class
staticA = MyClass.a

source = emptyTable(1).update("X = staticA", "Y = myClass.getB()")
sourceMeta = source.meta()
```

### Functions (methods)

In Groovy, classes can have two types of methods:

**Instance methods:** Defined without the static keyword and require an instance of the class to exist in order to be called. They operate on object state and can access both instance variables and static (class) variables.

**Static methods:** Defined with the static keyword and belong to the class itself rather than any particular instance. They can only access static variables and other static methods, and do not require an instance to be called.

Both kinds of methods are supported in query strings.

The following code block calls a static method and an instance method in a query string. Note how the instance method is called on the instance of `MyClass` called `myClass`. Type casts are used to ensure the resultant columns are of an appropriate Java primitive type:

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

myClass = new MyClass(15, 6)

source = emptyTable(1).update(
    "X = (int)myClass.changeValue(5)",
    "Y = (double)myClass.multiplyModulo(11, 16, X)",
    "Z = (int)myClass.plusModulo(X)"
)
sourceMeta = source.meta()
```

## Related documentation

- [Built-in query language constants](./built-in-constants.md)
- [Built-in query language variables](./built-in-variables.md)
- [Built-in query language functions](./built-in-functions.md)
- [Filters in query strings](./filters.md)
- [Formulas in query strings](./formulas.md)
- [Operators in query strings](./operators.md)
- [Groovy variables in query strings](./groovy-variables.md)
- [Groovy closures in query strings](./groovy-closures.md)
