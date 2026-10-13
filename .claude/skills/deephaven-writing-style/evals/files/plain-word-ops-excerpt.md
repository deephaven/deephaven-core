---
title: Stateless by default
---

## Stateless by default

Starting in Deephaven Core 41, the engine assumes that filter and select operations can be executed in parallel by default. To force sequential execution, mark the filter or the column serial. Sort operations always parallelize by size, and joins parallelize per update cycle.

- Filter operations run in parallel when the input is large enough.
- Select operations run in parallel when columns are independent.

| Operation | Parallelized when                          |
| --------- | ------------------------------------------ |
| Filter    | The input exceeds the segment threshold    |
| Sort      | The row count exceeds the sort threshold   |

Group your settings by purpose, select the ones you need, and view the defaults first.

For more on how to pick between them, see the [query engine guide](./query-engine/parallelization.md).
