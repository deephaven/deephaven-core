---
title: Use the ternary conditional operator in query strings
sidebar_label: Ternary-if
---

Groovy's native ternary operator (`condition ? valueIfTrue : valueIfFalse`) works seamlessly in Deephaven query strings, providing a concise way to implement conditional logic.

## Basic usage

```groovy order=woods,result
woods = newTable(
    stringCol("Type", "Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"),
    doubleCol("Price", 1.95, 2.50, 3.25, 3.45, 4.25, 7.95, 4.10, 5.25)
)

result = woods.update("Budget = (Price <= 3.50) ? `yes` : `no`")
```

## Nested ternary operators

Ternary operators can be nested for more complex conditional logic:

```groovy test-set=1 order=woods,result
woods = newTable(
    stringCol("Type", "Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"),
    stringCol("Hardness", "soft", "soft", "soft", "hard", "hard", "hard", "hard", "hard"),
    doubleCol("Price", 1.95, 2.50, 3.25, 3.45, 4.25, 7.95, 4.10, 5.25)
)

result = woods.update("Category = (Hardness == `hard`) ? ((Price <= 3.50) ? `budget` : `expensive`) : `softwood`")
```

## Using custom methods

When using custom methods as conditions, [cast](./casting.md) the result to `(Boolean)`:

```groovy order=woods,result
budget = { price -> price <= 3.50 }

woods = newTable(
    stringCol("Type", "Pine", "Fir", "Cedar", "Oak", "Ash", "Walnut", "Beech", "Cherry"),
    doubleCol("Price", 1.95, 2.50, 3.25, 3.45, 4.25, 7.95, 4.10, 5.25)
)

result = woods.update("Budget = (Boolean)budget(Price) ? `yes` : `no`")
```

## Related documentation

- [Query string overview](./query-string-overview.md)
- [Create a new table](./new-and-empty-table.md#newtable)
