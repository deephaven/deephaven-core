---
title: Recipes, not loops!
---

If you're accustomed to writing loops to transform data, **stop!** Deephaven works fundamentally differently, and understanding this difference early saves you frustration and helps you write better, faster code.

## The fundamental paradigm shift: recipes, not loops

### How you might think

In traditional programming, you tell the computer **exactly how** to process each element:

```groovy skip-test
// A loop applied to data — common habit, but wrong for Deephaven!
def values = [0, 1, 2, 3, 4]
def valuesSquared = values.collect { it * it }
```

This loop processes one element at a time. You're giving **step-by-step instructions** for how to process the data.

### How to think in Deephaven

In Deephaven, you specify **what** you want, not **how** to compute it. You write a **recipe** that describes the transformation, and the engine figures out the optimal way to execute it. Table operations such as [`update`](../../reference/table-operations/select/update.md) take recipes as [query strings](./query-strings.md):

```groovy order=result test-set=groovy-recipe-example
// No loop — just describe what you want
result = emptyTable(5).update("X = i", "XSquared = X * X")
```

There's **no loop**. You describe the relationship (`XSquared = X * X`), and the engine applies it to every row. This works the same way for arbitrarily complex operations.

When you write `XSquared = X * X`, you're **not** saying:

- "Start at row 0"
- "Read `X` from row 0"
- "Multiply `X` by itself"
- "Store the result in `XSquared` at row 0"
- "Go to row 1"
- "Repeat..."

You're saying:

- "For every row, `XSquared` should equal `X` times `X`"

The engine decides:

- How to split the data into batches of rows for fast processing.
- Whether to parallelize the operation.
- Which rows to recompute when data changes.

## Why this matters

### For static data

Even for static, one-time calculations, the recipe approach has advantages:

1. **Clearer code** — declarative recipes are easier to read than imperative loops.
2. **Faster execution** — the engine evaluates formulas over batches of rows in compiled code.
3. **Less error-prone** — no manual loop management or index tracking.

### For real-time data

This is the critical difference. **Loops execute once and stop. Recipes keep applying to new rows as they arrive.**

```groovy ticking-table order=null test-set=groovy-ticking-demo
// This table adds roughly one new row every second
source = timeTable("PT1s").update("X = ii", "XSquared = X * X", "XCubed = X * X * X")
```

Watch what happens:

- The table **keeps updating** — new rows appear about every second.
- You wrote the recipe **once**, and the engine applies it to every new row.

Compare a loop that uses a [column iterator](../../how-to-guides/extract-table-value.md#tablecolumniterator) to read values one at a time:

```groovy skip-test
// This runs once and never reacts to new rows!
source.longColumnIterator("X").withCloseable { iter ->
    for (x in iter) {
        def xSquared = x * x  // ❌ Where would this even go?
    }
}
```

#### The engine is smart about updates

When the source data changes, the engine:

1. **Tracks dependencies** — it knows that `XSquared` depends on `X`.
2. **Computes incrementally** — each update tells the engine which rows were added, which existing rows changed (modified rows), and which columns changed. The engine computes `XSquared` for the added rows. For modified rows, it recomputes `XSquared` only when `X` is one of the changed columns.
3. **Updates results in place** — it writes the new values into the result table, so you never re-run the query.

With loops, this requires significant additional infrastructure. A loop executes once and stops, so you'd have to write your own code to detect new and changed rows and recompute them. With recipes, it's automatic.

## Common patterns: wrong vs. right

You've already seen the simplest pattern, creating a column from another column, in [How to think in Deephaven](#how-to-think-in-deephaven). Two other common loops have recipe equivalents.

### Pattern: conditional logic

❌ **Wrong** (loop approach):

```groovy order=source test-set=groovy-antipattern-conditional
source = emptyTable(10).update("X = i")

// Don't do this!
results = []
source.integerColumnIterator("X").withCloseable { iter ->
    for (x in iter) {
        if (x % 2 == 0) {
            results.add("Even")
        } else {
            results.add("Odd")
        }
    }
}
```

✅ **Right** (recipe with the [ternary-if operator](../../how-to-guides/ternary-if-how-to.md)). The operator has the form `condition ? valueIfTrue : valueIfFalse`:

```groovy order=result test-set=groovy-pattern2
result = emptyTable(10).update("X = i", "Label = (X % 2 == 0) ? `Even` : `Odd`")
```

### Pattern: running calculations

❌ **Wrong** (loop with accumulator):

```groovy order=source test-set=groovy-antipattern-running
source = emptyTable(10).update("X = i")

// Don't do this!
runningSum = 0
results = []
source.integerColumnIterator("X").withCloseable { iter ->
    for (x in iter) {
        runningSum += x
        results.add(runningSum)
    }
}
```

✅ **Right** (use [`updateBy`](../../reference/table-operations/update-by-operations/updateBy.md)):

```groovy order=result test-set=groovy-pattern3
result = emptyTable(10).update("X = i").updateBy(CumSum("SumX = X"))
```

## When loops are appropriate

Recipes replace loops for transforming data, but loops are still the right tool for extracting data from a table and for control flow in your own code.

### ✅ Extracting data from Deephaven

To read two columns side by side, open one iterator per column and advance them together, closing both when you're done:

```groovy order=source,:log test-set=groovy-valid-loops
source = emptyTable(5).update("X = i", "Y = X * 2")

// This is fine — you're extracting, not transforming
xIter = source.integerColumnIterator("X")
yIter = source.integerColumnIterator("Y")
try {
    while (xIter.hasNext()) {
        println "X=${xIter.nextInt()}, Y=${yIter.nextInt()}"
    }
} finally {
    xIter.close()
    yIter.close()
}
```

See the guide to [extracting table values](../../how-to-guides/extract-table-value.md) for details.

### ✅ Control flow in your Groovy code

A loop over ordinary Groovy values is fine when each iteration builds a query rather than reading table rows. Here, the loop creates one filtered table per symbol, and each table is still a live recipe.

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

## Next steps

- Continue to the next chapter, [Real-time plots](./plots.md), to visualize static and ticking tables.
- Read [Think like a ninja](../../conceptual/ninja.md#looping-dont-do-it) for more examples.
- Read [Vectorization and the recipe paradigm](../../conceptual/vectorization-and-recipes.md) for technical details.
- See [rolling aggregations with `updateBy`](../../how-to-guides/rolling-aggregations.md) for more recipes.
