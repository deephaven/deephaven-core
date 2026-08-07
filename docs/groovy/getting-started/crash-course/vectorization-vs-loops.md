---
title: Recipes, not loops!
---

Deephaven works fundamentally differently from traditional data processing. Understanding this difference early will save you countless hours of frustration and help you write better, faster code.

## The fundamental paradigm shift: recipes, not loops

### Traditional approach

In traditional programming, you tell the computer **exactly how** to process each row:

```groovy skip-test
// A loop applied to data — common habit, but wrong for Deephaven!
def values = [0, 1, 2, 3, 4]
def valuesSquared = values.collect { it * it }
```

This loop processes one element at a time. You're giving **step-by-step instructions** for how to process the data.

### The Deephaven way

In Deephaven, you specify **what** you want, not **how** to compute it. You write a **recipe** that describes the transformation, and the engine figures out the optimal way to execute it:

```groovy order=result test-set=groovy-recipe-example
// No loop — just describe what you want
result = emptyTable(5).update("X = i", "XSquared = X * X")
```

Notice:

- **No loops** — you describe the relationship (`XSquared = X * X`) and the engine applies it to every row.
- You specify **what** to compute, not **how** to iterate.
- The engine applies this recipe to all rows automatically.

## Why this matters: real-time data

This is the critical difference. **Loops execute once and stop. Recipes update automatically.**

```groovy ticking-table order=null test-set=groovy-ticking-demo
// This table adds a new row every second
source = timeTable("PT1s").update("X = i", "XSquared = X * X", "XCubed = X * X * X")
```

Watch what happens:

- The table **keeps updating** — new rows appear every second.
- Your **recipe runs automatically** on every new row.
- You wrote it **once**, but it executes **forever**.

With a loop approach, you'd need to build your own subscription and recomputation logic. With recipes, it's automatic.

## The recipe paradigm explained

### Recipes are specifications, not instructions

When you write:

```groovy skip-test
t.update("Y = X * 2")
```

You're **not** saying:

- "Start at row 0"
- "Read X from row 0"
- "Multiply by 2"
- "Store in Y at row 0"
- "Repeat..."

You're saying:

- "For every row, Y should equal X times 2"

The engine decides:

- How to chunk the data for optimal performance.
- Whether to parallelize the operation.
- How to handle updates efficiently.
- What rows need recomputation when data changes.

### The engine is smart about updates

1. **Tracks dependencies** — It knows that `Y` depends on `X`.
2. **Computes incrementally** — Only new or changed rows are processed.
3. **Updates automatically** — Results update without you doing anything.

## When loops ARE appropriate

There are valid uses for loops in Deephaven:

### ✅ Extracting data from Deephaven

```groovy skip-test
source = emptyTable(5).update("X = i", "Y = X * 2")

// This is fine — you're extracting, not transforming
iter = source.columnIterator("X")
while (iter.hasNext()) {
    x = iter.next()
    println "X=${x}"
}
```

### ✅ Control flow in your Groovy code

```groovy order=null test-set=groovy-control-flow
source = emptyTable(100).update(
    "Symbol = (i % 3 == 0) ? `AAPL` : (i % 3 == 1) ? `GOOGL` : `MSFT`",
    "X = i"
)

// Creating multiple similar tables — fine!
tables = [:]
["AAPL", "GOOGL", "MSFT"].each { symbol ->
    tables[symbol] = source.where("Symbol = `${symbol}`")
}
```

### ❌ Transforming table columns

```groovy skip-test
source = emptyTable(10).update("X = i")

// NEVER do this!
resultData = []
for (x in source.getColumnSource("X")) {
    resultData.add(x * 2)  // ❌ Use .update() instead!
}
```

## Common patterns: Wrong vs Right

### Pattern: Create a column from another column

❌ **Wrong** (loop approach):

```groovy skip-test
source = emptyTable(10).update("X = i")

// Don't do this!
values = []
for (x in source.getColumnSource("X")) {
    values.add(x * x)
}
// Now what? How do you get this back into a table?
```

✅ **Right** (recipe approach):

```groovy order=result test-set=groovy-pattern1
result = emptyTable(10).update("X = i", "XSquared = X * X")
```

### Pattern: Conditional logic

✅ **Right** (recipe with ternary operator):

```groovy order=result test-set=groovy-pattern2
result = emptyTable(10).update("X = i", "Label = (X % 2 == 0) ? `Even` : `Odd`")
```

### Pattern: Running calculations

✅ **Right** (use updateBy):

```groovy order=result test-set=groovy-pattern3
import static io.deephaven.api.updateby.UpdateByOperation.CumSum

result = emptyTable(10).update("X = i").updateBy(CumSum("SumX = X"))
```

## Next steps

- Read [Think like a ninja](../../conceptual/ninja.md#looping-dont-do-it) for more examples.
- Learn about [table operations](./table-ops.md) to see recipes in action.
- Understand [the query engine](../../conceptual/vectorization-and-recipes.md) for technical details.
- See [updateBy operations](../../how-to-guides/rolling-aggregations.md) for powerful recipes.
