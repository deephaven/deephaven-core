---
title: Objects
---

The Deephaven Query Language has native support for objects within query strings.

<!-- TODO: https://github.com/deephaven/deephaven-core/issues/1388 and https://github.com/deephaven/deephaven-core/issues/1389
are blocking examples of objects within a table. For now this doc will stick with just objects
within a query string. -->

## Example

The following example shows how to create an object and use one of its methods within a query string.

```groovy order=source,result
class MyStringClass {
    private String strn

    MyStringClass(String strn) {
        this.strn = strn
    }

    String myString() {
        return this.strn
    }
}

obj = new MyStringClass("A")

source = newTable(
    stringCol("Strings", "A", "B", "C")
)

result = source.update("Match = Strings_[i] == obj.myString()")
```

## Related Documentation

- [Create a new table](../../../how-to-guides/new-and-empty-table.md#newtable)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/query-language-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [`update`](../../table-operations/select/update.md)

<!--TODO: XXX from "use-objects"


This guide will show you how to work with [objects](../reference/query-language/types/objects.md) in your query strings.

When performing complex analyses, [objects](../reference/query-language/types/objects.md) are an invaluable tool. [Objects](../reference/query-language/types/objects.md) can contain related data and provide an easy way to access data from one source to make your program more logical or streamlined.

The Deephaven Query Language natively supports [objects](../reference/query-language/types/objects.md) in Python and Groovy, allowing users to pass in [objects](../reference/query-language/types/objects.md), [object's](../reference/query-language/types/objects.md) fields, and [object's](../reference/query-language/types/objects.md) methods into query strings.

## Power of objects in code

[Objects](../reference/query-language/types/objects.md) are designed to hold information or values. In the following example, operators are used with [objects](../reference/query-language/types/objects.md) to assign values.

Here, we have two [objects](../reference/query-language/types/objects.md) and each [object](../reference/query-language/types/objects.md) holds two values and a custom method. When we call that [object](../reference/query-language/types/objects.md), those specific values and methods are utilized without having to pass extra parameters.

```groovy
class SimpleObj {
    public int a, b
    SimpleObj(int a, int b){
        this.a = a
        this.b = b
    }
    int getA() {
        return this.a
    }
    int getB() {
        return this.b
    }
    int compute() {
        return getA() + getB()
    }
}
class OtherObj {
    public int a, b
    OtherObj(int a, int b){
        this.a = a
        this.b = b
    }
    int getA() {
        return this.a
    }
    int getB() {
        return this.b
    }
    int compute() {
        return 2 * getA() + 2 * getB()
    }
}
obj1 = new SimpleObj(1, 2)
obj2 = new OtherObj(3, 4)
result = emptyTable(5).update("X = obj1.getA()", "Y = obj1.compute()", "M = obj2.getA()", "N = obj2.compute()")
```

## Poorly written code

If we didn't use [objects](../reference/query-language/types/objects.md), our code could get confusing and cumbersome.

In the following example, we do a similar operation as above, but without the power of [objects](../reference/query-language/types/objects.md). Notice that the compute methods require us to track the parameters, and pass them in every time we need to perform these operations.

```groovy
compute1 = { int valueA, int valueB  ->  (valueA + valueB) }
compute2 = { int valueA, int valueB  ->  (2 * valueA + 2 * valueB) }

a1 = 1
b1 = 2
a2 = 3
b2 = 4

result = emptyTable(5).update("X = a1", "Y = compute1(a1, b1)", "M = a2", "N = compute2(a2, b2)")
```

Use the power of [objects](../reference/query-language/types/objects.md) in the Deephaven Query Language to make your queries more powerful and concise.

## Related documentation

- [Create an empty table](./new-and-empty-table.md#emptytable)
- [Formulas](../how-to-guides/formulas-how-to.md)
- [Objects](../reference/query-language/types/objects.md)


-->
