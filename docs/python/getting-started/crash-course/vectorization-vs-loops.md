---
title: Vectorization
sidebar_label: Vectorization
---

If you're coming from pandas, traditional Python, or other data processing tools, you're likely accustomed to writing loops to transform data. **Stop!** Deephaven works fundamentally differently, and understanding this difference early will save you countless hours of frustration and help you write better, faster code.

## The fundamental paradigm shift: recipes, not loops

### How you might be thinking

In pandas or traditional Python, you tell the computer **exactly how** to process each row:

```python skip-test
import pandas as pd

# Pandas approach: explicit loop over rows
time_index = pd.date_range(start="2025-01-01 00:00:00", periods=5, freq="h")
df = pd.DataFrame(
    {
        "time": time_index,
        "value": range(5),
    }
)

# Converting time with list comprehension - WRONG for Deephaven!
from deephaven.column import datetime_col

# This is what you would do in pandas/Python:
datetime_col("TsDT", [_to_jinst_from_ns(r["TsEpochNs"]) for r in rows])
```

This list comprehension loops over every row, processes it, and builds a new list. You're giving **step-by-step instructions** for how to process the data.

### How to think in Deephaven

In Deephaven, you specify **what** you want, not **how** to compute it. You write a **recipe** that describes the transformation, and the Deephaven engine figures out the optimal way to execute it:

```python order=t1,t2,t3 test-set=recipe-example
from deephaven import time_table

# Create a table that ticks every second
t1 = time_table("PT1s").update(["TsEpochNs = epochNanos(Timestamp)"])

# Add a column using a Deephaven recipe - NO LOOP!
t2 = t1.update("TS2 = epochNanosToInstant(TsEpochNs)")

# Do more time operations - still no loops
t3 = t2.update(
    [
        "TS3 = epochNanosToInstant(TsEpochNs + 2*SECOND)",
        "TS4 = Timestamp + 'PT2s'",
        "D3 = TS3-Timestamp",
        "D4 = TS4-Timestamp",
    ]
)
```

Notice:

- **No loops** - You write `.update("TS2 = epochNanosToInstant(TsEpochNs)")`.
- You specify **what** to compute, not **how** to iterate.
- The engine applies this recipe to all rows automatically.

## Why this matters

### For static data

Even for static, one-time calculations, the recipe approach has advantages:

1. **Clearer code** - Declarative recipes are easier to read than imperative loops.
2. **Faster execution** - The engine can optimize vectorized operations.
3. **Less error-prone** - No manual loop management or index tracking.

### For real-time data

This is the critical difference. **Loops execute once and stop. Recipes update automatically.**

```python ticking-table order=null test-set=ticking-demo
from deephaven import time_table

# This table adds a new row every second
source = time_table("PT1s").update(["X = i", "XSquared = X * X", "XCubed = X * X * X"])
```

Watch what happens:

- The table **keeps updating** - new rows appear every second.
- Your **recipe runs automatically** on every new row.
- You wrote it **once**, but it executes **forever**.

With a loop approach:

```python skip-test
# This would only work ONCE and never update!
for row in source.iter_tuple():
    x = row.X
    x_squared = x * x  # ❌ Where would this even go?
```

## The recipe paradigm explained

### Recipes are specifications, not instructions

When you write:

```python skip-test
t.update("Y = X * 2")
```

You're **not** saying:

- "Start at row 0"
- "Read X from row 0"
- "Multiply by 2"
- "Store in Y at row 0"
- "Go to row 1"
- "Repeat..."

You're saying:

- "For every row, Y should equal X times 2"

The engine decides:

- How to chunk the data for optimal performance
- Whether to parallelize the operation
- How to handle updates efficiently
- What rows need recomputation when data changes

### The engine is smart about updates

When data ticks in real-time, the engine:

1. **Tracks dependencies** - It knows that `Y` depends on `X`
2. **Computes incrementally** - Only new or changed rows are processed
3. **Updates automatically** - Results update without you doing anything

This is fundamentally impossible with loops!

## Bridging pandas and Deephaven

Many users need to work with both pandas and Deephaven. Here's how to think about the transition:

```python order=df,t1,m,t2,t3,df2 test-set=pandas-bridge
import pandas as pd
import deephaven.pandas as dhpd

# Create data in pandas
time_index = pd.date_range(start="2025-01-01 00:00:00", periods=5, freq="h")
df = pd.DataFrame(
    {
        "time": time_index,
        "value": range(5),
    }
)

print("Original pandas DataFrame:")
print(df)

# Convert to Deephaven
t1 = dhpd.to_table(df)

# Check the column types
m = t1.meta_table

# Now use Deephaven recipes (NOT loops!)
t2 = t1.update("TsEpochNs = epochNanos(time)")

# More time operations using recipes
t3 = t2.update(
    [
        "TS3 = epochNanosToInstant(TsEpochNs + 2*SECOND)",
        "TS4 = time + 'PT2s'",
        "D3 = TS3-time",
        "D4 = TS4-time",
    ]
)

# Convert back to pandas if needed
df2 = dhpd.to_pandas(t3)
print("Result DataFrame:")
print(df2)
```

**Key principle:** Once you're in Deephaven, think in recipes. Save loops for when you convert back to pandas.

## When loops ARE appropriate

There are valid uses for loops in Deephaven:

### ✅ Extracting data from Deephaven

```python order=source test-set=valid-loops
from deephaven import empty_table

source = empty_table(5).update(["X = i", "Y = X * 2"])

# This is fine - you're extracting, not transforming
for row in source.iter_tuple():
    print(f"X={row.X}, Y={row.Y}")
```

See the [table iteration guide](../../how-to-guides/iterate-table-data.md) for details.

### ✅ Control flow in your Python code

```python skip-test
# Creating multiple similar tables - fine!
tables = []
for symbol in ["AAPL", "GOOGL", "MSFT"]:
    t = source.where(f"Symbol = `{symbol}`")
    tables.append(t)
```

### ❌ Transforming table columns

```python skip-test
# NEVER do this!
result_data = []
for row in source.iter_tuple():
    result_data.append(row.X * 2)  # ❌ Use .update() instead!
```

## Common patterns: Wrong vs Right

### Pattern: Create a column from another column

❌ **Wrong** (loop approach):

```python skip-test
# Don't do this!
values = []
for row in source.iter_tuple():
    values.append(row.X * row.X)
# Now what? How do you get this back into a table?
```

✅ **Right** (recipe approach):

```python order=result test-set=pattern1
from deephaven import empty_table

result = empty_table(10).update(["X = i", "XSquared = X * X"])
```

### Pattern: Conditional logic

❌ **Wrong** (loop approach):

```python skip-test
# Don't do this!
results = []
for row in source.iter_tuple():
    if row.X % 2 == 0:
        results.append("Even")
    else:
        results.append("Odd")
```

✅ **Right** (recipe with ternary operator):

```python order=result test-set=pattern2
from deephaven import empty_table

result = empty_table(10).update(["X = i", "Label = (X % 2 == 0) ? `Even` : `Odd`"])
```

### Pattern: Running calculations

❌ **Wrong** (loop with accumulator):

```python skip-test
# Don't do this!
running_sum = 0
results = []
for row in source.iter_tuple():
    running_sum += row.X
    results.append(running_sum)
```

✅ **Right** (use update_by):

```python order=result test-set=pattern3
from deephaven import empty_table
from deephaven.updateby import cum_sum

result = empty_table(10).update("X = i").update_by(cum_sum("SumX = X"))
```

## Quick reference: Migration guide

| pandas/Python Pattern          | Deephaven Recipe                    |
| ------------------------------ | ----------------------------------- |
| `df.apply(func)`               | `.update("Y = func(X)")`            |
| `for row in df.iterrows():`    | ❌ Don't! Use `.update()`           |
| `df['Y'] = df['X'] * 2`        | `.update("Y = X * 2")`              |
| `df[df['X'] > 5]`              | `.where("X > 5")`                   |
| `df.rolling(window=10).mean()` | `.update_by(rolling_avg_tick(...))` |
| `df.groupby('G').sum()`        | `.sum_by("G")`                      |

## Next steps

- Read [Think like a ninja](../../conceptual/ninja.md#looping-dont-do-it) for more examples.
- Learn about [table operations](./table-ops.md) to see recipes in action.
- Understand [the query engine](../../conceptual/vectorization-and-recipes.md) for technical details.
- See [update_by operations](../../how-to-guides/rolling-aggregations.md) for powerful recipes.

## Related documentation

- [Think like a Deephaven ninja](../../conceptual/ninja.md)
- [Table operations](./table-ops.md)
- [Query strings](./query-strings.md)
- [Table iteration (for extraction only!)](../../how-to-guides/iterate-table-data.md)
- [Update_by for rolling calculations](../../how-to-guides/rolling-aggregations.md)
