---
title: Filter
---

A [`Filter`](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html) represents a filter condition used in [`where`](../../table-operations/filter/where.md) operations. Use `Filter` objects when you need to control how Deephaven evaluates filter conditions — specifically, to force sequential (serial) execution or to coordinate execution order with barriers.

> [!NOTE]
> `Filter` extends [`ConcurrencyControl`](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl), which defines the concurrency methods. You may see these methods documented in multiple places in the Pydoc — use the `Filter` class for filter operations.

## Creating a Filter

### From a condition string

```python syntax
from deephaven.filters import Filter

f = Filter.from_("X > 5")
```

### Using filter functions

```python syntax
from deephaven.filters import is_null, not_, and_, or_

f1 = is_null("X")
f2 = not_(is_null("Y"))
f3 = and_([Filter.from_("X > 5"), Filter.from_("Y < 10")])
f4 = or_([Filter.from_("X > 5"), Filter.from_("Y < 10")])
```

## Methods

### `.with_serial`

Forces the filter to evaluate sequentially on a single core, processing rows one at a time in order. Use this when the filter has side effects or depends on row order.

```python order=source,result
from deephaven.filters import Filter
from deephaven import empty_table

rows_checked = 0


def check_value(x) -> bool:
    global rows_checked
    rows_checked += 1  # Side effect: modifies external state
    return x > 5


source = empty_table(100).update("X = i")

# Use .with_serial because the filter has side effects
f = Filter.from_("(boolean)check_value(X)").with_serial()
result = source.where(f)
```

> [!NOTE]
> Most filters don't need serial execution. Use `.with_serial` only when the filter modifies external state or has side effects.

### `.with_declared_barriers`

Declares that this filter creates a barrier — other filters that respect this barrier will wait until this filter finishes evaluating all rows.

```python syntax
from deephaven.filters import is_null
from deephaven.concurrency_control import Barrier

barrier = Barrier()
filter1 = is_null("X").with_declared_barriers(barrier)
```

### `.with_respected_barriers`

Declares that this filter respects a barrier — it will not begin evaluating until all filters that declare the same barrier have finished.

```python syntax
from deephaven.filters import is_null
from deephaven.concurrency_control import Barrier

barrier = Barrier()
filter2 = is_null("Y").with_respected_barriers(barrier)
```

### Barriers example

Use barriers when one filter must complete before another starts:

```python order=result
from deephaven.filters import is_null
from deephaven.concurrency_control import Barrier
from deephaven import empty_table

barrier = Barrier()

# filter1 declares the barrier (must finish first)
filter1 = is_null("X").with_declared_barriers(barrier)

# filter2 respects the barrier (waits for filter1 to finish)
filter2 = is_null("Y").with_respected_barriers(barrier)

result = (
    empty_table(100)
    .update(["X = i % 5 == 0 ? null : i", "Y = i % 7 == 0 ? null : i"])
    .where([filter1, filter2])
)
```

## Filter functions

The `deephaven.filters` module provides these filter constructors:

| Function                       | Description                                     |
| ------------------------------ | ----------------------------------------------- |
| `Filter.from_(condition)`      | Create from condition string                    |
| `is_null(column)`              | True if column value is null                    |
| `not_(filter)`                 | Logical NOT                                     |
| `and_(filters)`                | Logical AND of multiple filters                 |
| `or_(filters)`                 | Logical OR of multiple filters                  |
| `pattern(column, regex, mode)` | Regex pattern match                             |
| `is_inf(column)`               | True if value is infinite                       |
| `is_nan(column)`               | True if value is NaN                            |
| `is_normal(column)`            | True if value is a normal floating-point number |

## When to use Filter objects

| Scenario                             | Approach                                                |
| ------------------------------------ | ------------------------------------------------------- |
| Simple conditions                    | Use string filters directly - no `Filter` object needed |
| Filter with side effects             | `Filter.from_(...).with_serial()`                       |
| Filter A must finish before Filter B | Use barriers                                            |
| Complex boolean logic                | Use `and_()`, `or_()`, `not_()`                         |

## Related documentation

- [Parallelization](../../../conceptual/query-engine/parallelization.md) - Full guide on controlling parallel execution
- [`where`](../../table-operations/filter/where.md) - Uses Filter objects
- [Selectable](./Selectable.md) - Similar concurrency controls for column calculations
- [Filter Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.filters.html)
- [ConcurrencyControl Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.ConcurrencyControl) - base class defining `with_serial` and barrier methods
- [Barrier Pydoc](https://docs.deephaven.io/core/pydoc/code/deephaven.concurrency_control.html#deephaven.concurrency_control.Barrier)
