---
title: Data types in Groovy
sidebar_label: Groovy data types
---

This guide discusses data types in Groovy and how they apply to Deephaven. Properly managing data types in queries leads to cleaner, faster, and more reusable code.

## Groovy data types

Groovy uses Java’s underlying data types. Here are some of the common ones:

```groovy order=:log
printType = { obj ->
    println "${obj}: ${obj.getClass()}"
}

printType(3)  // Integer
printType(3.14)  // BigDecimal
printType(true)  // Boolean
printType("Hello world!")  // String
printType([1, 2, 3])  // ArrayList
printType([1, 2, 3] as int[])  // Array
printType(["a": 1, "b": 2, "c": 3])  // LinkedHashMap
```

The data type of an integer literal is inferred from its size:

```groovy skip-test
myInt1 = 3 // Integer
myInt2 = 1_000_000_000_000 // Long
myInt3 = 1_000_000_000_000_000_000_000 // BigInteger
```

As in Java, you can define explicit types for variables:

```groovy skip-test
int myInt1 = 3
long myInt2 = 3
double myDouble = 3.0
BigDecimal myDouble2 = 3.0
```

Variables defined this way are not available in the query scope:

```groovy skip-test
int myInt1 = 3
myInt2 = 5
// myTable = emptyTable(1).update("X = myInt1") fails as myInt1 is not in scope
myTable = emptyTable(1).update("X = myInt2") // ok because myInt2 is in the binding
```

### Boxed vs primitive

Variables in the Groovy binding are always boxed. Same with variables defined with `def` or `var`:

```groovy skip-test
x = 5 //x is an Integer object, not an int primitive
var x2 = 5.0 //x2 is a BigDecimal
```

Square brackets create a Java List. Anything inside the brackets is, therefore, a Java Object:

```groovy skip-test
x = [1,2,3] //x is an ArrayList<Integer>
```

You can convert a List to an array by casting, or with the `as` operator:

```groovy skip-test
x = [1,2,3]  as int[]
x2 = (int[]) [1,2,3]
```

For more information on data types in Groovy, see the [official Groovy documentation](https://docs.groovy-lang.org/latest/html/documentation/core-syntax.html)

### Data types in closures

Deephaven tables don't know the return type of a Groovy closure unless you explicitly typecast the result:

```groovy order=myTable,myTableMetadata
xPlusY = { int x, int y ->
    return x + y
}

myTable = emptyTable(4).update("X = i", "Y = i % 2", "Z = xPlusY(X, Y)")
myTableMetadata = myTable.meta()
```

The type of the column `Z` is a `java.lang.Object`. To get the data type you want, cast the result inside the query string:

```groovy order=myTable,myTableMetadata
xPlusY = { int x, int y ->
    return x + y
}

myTable = emptyTable(4).update("X = i", "Y = i % 2", "Z = (int) xPlusY(X, Y)")
myTableMetadata = myTable.meta()
```

Casting makes it easier to use the column in downstream queries. It can also improve memory usage by letting Deephaven know exactly how much memory to allocate to each cell.

## Performance

It is generally a good idea to declare the data types of the input parameters to a closure if you know them ahead of time.

```groovy skip-test
// best practice
xPlusY = { int x, int y ->
    return x + y
}

// may be less efficient
xPlusY2 = { x, y ->
    return x + y
}
```

In `xPlusY2`, the data types of `x` and `y` are unknown at compile time. Groovy must determine what they are at runtime and what "plus" should mean for those two arguments. If `x` and `y` are numbers, it adds them; if `x` or `y` is a `String`, it concatenates them. Checking the data types and determining how to handle them can be much slower than the addition or concatenation itself. Static typing allows the Groovy compiler to best optimize your closure.

## Related documentation

- [Use variables in query strings](./queryscope.md)
