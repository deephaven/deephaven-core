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
