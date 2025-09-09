---
title: Strings
---

String values can be represented in Deephaven query strings by using backticks `` ` ``.

## Syntax

```
`string`
```

## Usage

### Filter

The following example shows a query string used to filter data. This query string returns items in the `Value` column that are equal to the string `` `C` ``.

```groovy order=source,result
source = newTable(
    stringCol("Value", "A" , "B", "C", "D", "E")
)

result = source.where("Value = `C`")
```

## Related documentation

- [How to use formulas](../../../how-to-guides/formulas-how-to.md)
- [How to use strings and variables inside your query strings](../../../how-to-guides/queryscope.md)
- [How to use Deephaven's built-in query language functions](../../../how-to-guides/query-language-functions.md)
- [Query language functions](../query-library/query-language-function-reference.md)
- [where](../../table-operations/filter/where.md)
- [update](../../table-operations/select/update.md)
