---
title: Formula evaluation and threads
---

The engine has two strategies for spreading work over threads: row fan-out and column fan-out. With row fan-out, one column's rows are divided into ranges that different threads evaluate. With column fan-out, independent columns in the same `update` are evaluated on different threads.

## Which formulas the engine can fan out

The engine can fan out any formula whose output depends only on its arguments and that changes nothing outside itself. Formulas built from column arithmetic, string functions, and the built-in date-time functions all qualify. Because these formulas are thread-safe, row fan-out never changes their results.

Row fan-out only happens once a table is large enough to be worth dividing. You can tune that size with `QueryTable.minimumParallelSelectRows`, which defaults to about 4.2 million rows.

## Formulas that need care

A formula that increments a global counter isn't stateless, so two threads evaluating it at once can both read the same value. Mark it with `with_serial` so the engine evaluates its rows one at a time, in order.
