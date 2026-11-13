---
title: How parallel formulas work
---

Deephaven runs formulas across rows and across tables. Across rows means the engine splits a table into pieces and computes each piece on its own core; across tables means independent tables tick at the same time.

## Safe formulas

A formula is safe to run in parallel when it only does math on its inputs and doesn't touch anything else. Stateless formulas like these are thread-safe, so the engine runs them across rows automatically:

```python
from deephaven import empty_table

source = empty_table(100).update(["X = i", "Y = X * 2"])
```

A thread-safe function is also stateless, so you can treat the two words as meaning the same thing.

## Unsafe formulas

If a formula updates a global variable, it isn't stateless. Use `.with_serial()` on the column (the engine then skips splitting across rows, controlled by `QueryTable.statelessSelectByDefault`) so that rows are processed one at a time, in order.
