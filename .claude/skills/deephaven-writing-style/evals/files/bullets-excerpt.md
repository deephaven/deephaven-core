---
title: Parallelization
---

<!-- Excerpt of docs/python/conceptual/query-engine/parallelization.md at 609b266973 (PR #7457), a Concept guide. Passages are verbatim; gaps are marked. -->

By default, Deephaven parallelizes operations that are **stateless** — meaning each row's result depends only on that row's input values.

**An operation is stateless if it**:

- Doesn't read or modify global variables.
- Doesn't depend on which row is processed first.
- Produces the same output for the same input, regardless of when or how it runs.

<!-- excerpt gap: text between these passages omitted -->

## Controlling execution order

Most queries work correctly with automatic parallelization. Some code doesn't — for example, code that uses a counter or modifies shared state. Deephaven provides two controls for these cases, `with_serial` and barriers, which you apply to these objects:

- **[`Selectable`](../../reference/query-language/types/Selectable.md)**: Represents a column expression, used in `select` or `update` operations.
- **[`Filter`](../../reference/query-language/types/Filter.md)**: Represents a filter condition, used in `where` operations. Concurrency control works the same way for `Filter` as it does for `Selectable`.

**`with_serial` vs. barriers** — these solve different problems:

- **`with_serial`**: Rows _within one column_ are processed sequentially (row 0, then row 1, etc.). Other columns can still run at the same time.
- **Barriers**: _Between columns_, one column finishes all its rows before another column starts. Rows within each column can still be parallelized.

When shared state is involved, you often need both: `with_serial` to protect row-level access to the shared state, and a barrier to ensure one column is completely done before the other starts.

<!-- excerpt gap: text between these passages omitted -->

## Key takeaways

Deephaven automatically parallelizes queries across all available CPU cores. Most code works correctly without changes.

- Deephaven assumes all formulas can run in parallel by default.
- Use [`with_serial`](../../reference/query-language/types/Selectable.md#with_serial) when your code has side effects, depends on rows being processed in a specific order, or calls functions that aren't safe to run from multiple threads.
- Use **barriers** when one operation must complete before another starts.
- `with_serial` keeps one column from running concurrently with itself. When several columns share state, use `with_serial` and barriers together. When several tables share state, make the shared code itself thread-safe (for example, protect it with a lock).

For a quick introduction, see the [Crash Course](../../getting-started/crash-course/parallelization.md).
