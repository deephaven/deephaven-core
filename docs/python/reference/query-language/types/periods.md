---
title: Periods
---

Periods are a special type of string used to represent a period of calendar time (i.e. days, weeks, months, years, etc.).

## Syntax

`[-]PnYnMnWnD`

- `[-]` - An optional sign to indicate that the period is negative. Omitting this makes the period positive.
- `P` - The prefix indicating this is a period string.
- `n` - An integer value
- `Y` - Years
- `M` - Months
- `W` - Weeks
- `D` - Days

Each `#[Y|M|W|D]` value translates to a part of the time period. A valid period string can contain nearly any combination of these values. For example, `P1M1D` (1 month and 1 day), `P1Y3M` (1 year and 3 months), and `P3W2D` (3 weeks and 2 days) are all valid period strings.

## Examples

The following example uses a string literal in a query string to add a Period of 1 day to an Instant:

```python
from deephaven import empty_table

source = empty_table(1).update("Timestamp = now() + 'P1d'")
```

## Related Documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [`date-time`](./date-time.md)
- [Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.Period)
