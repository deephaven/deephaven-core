---
title: Recipes, not loops!
---

If you're accustomed to writing loops to transform data, **stop!** Deephaven works fundamentally differently, and understanding this difference early saves you frustration and helps you write better, faster code.

## The fundamental paradigm shift: recipes, not loops

### How you might be thinking

In traditional Python, you tell the computer **exactly how** to process each element:

```python skip-test
# A loop applied to data — common habit, but wrong for Deephaven!
values = [0, 1, 2, 3, 4]
values_squared = [v * v for v in values]
```

This loop processes one element at a time and builds a new list. You're giving **step-by-step instructions** for how to process the data.

### How to think in Deephaven

In Deephaven, you specify **what** you want, not **how** to compute it. You write a **recipe** that describes the transformation, and the Deephaven engine figures out the optimal way to execute it. Table operations such as [`update`](../../reference/table-operations/select/update.md) take recipes as [query strings](./query-strings.md):

```python order=result test-set=recipe-example
from deephaven import empty_table

# No loop — just describe what you want
result = empty_table(5).update(["X = i", "XSquared = X * X"])
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

```python ticking-table order=null test-set=ticking-demo
from deephaven import time_table

# This table adds roughly one new row every second
source = time_table("PT1s").update(["X = ii", "XSquared = X * X", "XCubed = X * X * X"])
```

Watch what happens:

- The table **keeps updating** — new rows appear about every second.
- You wrote the recipe **once**, and the engine applies it to every new row.

Compare a loop that uses [`iter_tuple`](../../reference/data-import-export/iterate/iter-tuple.md) to read rows one at a time:

```python skip-test
# This runs once, blocks table updates while it iterates, and never reacts to new rows!
for row in source.iter_tuple():
    x = row.X
    x_squared = x * x  # ❌ Where would this even go?
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

```python order=source test-set=antipattern-conditional
from deephaven import empty_table

source = empty_table(10).update("X = i")

# Don't do this!
results = []
for row in source.iter_tuple():
    if row.X % 2 == 0:
        results.append("Even")
    else:
        results.append("Odd")
```

✅ **Right** (recipe with the [ternary-if operator](../../how-to-guides/ternary-if-how-to.md)). The operator has the form `condition ? valueIfTrue : valueIfFalse`:

```python order=result test-set=pattern2
from deephaven import empty_table

result = empty_table(10).update(["X = i", "Label = (X % 2 == 0) ? `Even` : `Odd`"])
```

### Pattern: running calculations

❌ **Wrong** (loop with accumulator):

```python order=source test-set=antipattern-running
from deephaven import empty_table

source = empty_table(10).update("X = i")

# Don't do this!
running_sum = 0
results = []
for row in source.iter_tuple():
    running_sum += row.X
    results.append(running_sum)
```

✅ **Right** (use [`update_by`](../../reference/table-operations/update-by-operations/updateBy.md)):

```python order=result test-set=pattern3
from deephaven import empty_table
from deephaven.updateby import cum_sum

result = empty_table(10).update("X = i").update_by(cum_sum("SumX = X"))
```

## When loops are appropriate

Recipes replace loops for transforming data, but loops are still the right tool for extracting data from a table and for control flow in your own code.

### ✅ Extracting data from Deephaven

```python order=source,:log test-set=valid-loops
from deephaven import empty_table

source = empty_table(5).update(["X = i", "Y = X * 2"])

# This is fine — you're extracting, not transforming
for row in source.iter_tuple():
    print(f"X={row.X}, Y={row.Y}")
```

See the [table iteration guide](../../how-to-guides/iterate-table-data.md) for details.

### ✅ Control flow in your Python code

A loop over ordinary Python values is fine when each iteration builds a query rather than reading table rows. Here, the loop creates one filtered table per symbol, and each table is still a live recipe.

```python order=null test-set=control-flow
from deephaven import empty_table

source = empty_table(100).update(
    ["Symbol = (i % 3 == 0) ? `AAPL` : (i % 3 == 1) ? `GOOGL` : `MSFT`", "X = i"]
)

# Creating multiple similar tables — fine!
tables = []
for symbol in ["AAPL", "GOOGL", "MSFT"]:
    t = source.where(f"Symbol = `{symbol}`")
    tables.append(t)
```

## Bridging pandas and Deephaven

Many users work with both pandas and Deephaven. Here's how to think about the transition. The habit to break is looping over DataFrame rows:

```python skip-test
# pandas habit: loop over rows
doubled = []
for _, row in df.iterrows():
    doubled.append(row["Value"] * 2)
```

Instead, convert the DataFrame to a table once with `to_table` from the `deephaven.pandas` module, and use a recipe. Convert back with `to_pandas` only if you need a DataFrame again. The next chapter, [Python integrations](./py-integrations.md#pandas), covers both functions:

```python order=t,result,df,df2 test-set=pandas-bridge
import pandas as pd
from deephaven import pandas as dhpd

df = pd.DataFrame({"Value": range(5)})

# Convert to Deephaven, then use a recipe instead of a loop
t = dhpd.to_table(df)
result = t.update("DoubleValue = Value * 2")

# Convert back to pandas if needed
df2 = dhpd.to_pandas(result)
```

**Key principle:** Once you're in Deephaven, think in recipes.

### Quick reference: pandas operations as recipes

These are the closest Deephaven recipes for common DataFrame operations:

| pandas pattern                      | Deephaven recipe                                                                                                                      |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `df['Y'] = df['X'].apply(func)`     | [`.update("Y = func(X)")`](../../reference/table-operations/select/update.md)                                                         |
| `df['Y'] = df['X'] * 2`             | [`.update("Y = X * 2")`](../../reference/table-operations/select/update.md)                                                           |
| `df[df['X'] > 5]`                   | [`.where("X > 5")`](../../reference/table-operations/filter/where.md)                                                                 |
| `df['X'].rolling(window=10).mean()` | [`.update_by(rolling_avg_tick("AvgX = X", rev_ticks=10))`](../../reference/table-operations/update-by-operations/rolling-avg-tick.md) |
| `df.groupby('G').sum()`             | [`.sum_by("G")`](../../reference/table-operations/group-and-aggregate/sumBy.md)                                                       |

## Next steps

- Continue to the next chapter, [Python integrations](./py-integrations.md), to convert between Deephaven tables and pandas, NumPy, and other Python libraries.
- Read [Think like a ninja](../../conceptual/ninja.md#looping-dont-do-it) for more examples.
- Read [Vectorization and the recipe paradigm](../../conceptual/vectorization-and-recipes.md) for technical details.
- See [rolling aggregations with `update_by`](../../how-to-guides/rolling-aggregations.md) for more recipes.
