---
title: Durations
---

Durations are a special type of string used to represent a period of wall clock time (i.e. hours, minutes, seconds, nanoseconds).

## Syntax

`[-]PTnHnMnS` or `[-]PT00:00:00.000000`

- `[-]` - An optional sign to indicate that the period is negative. Omitting this makes the period positive.
- `PT` - The prefix indicating this is a duration string.
- `n` - A numeric value
- `H` - Hours
- `M` - Minutes
- `S` - Seconds

Each `#[H|M|S]` value translates to a part of the duration. A valid duration string can contain nearly any combination of these values. For example, `PT1M1S` (1 minute and 1 second), `PT2H3M` (2 hours and 3 minutes), and `-PT24H30M2.4S` (negative 24 hours, 30 minutes, and 2.4 seconds) are all valid period strings. Alternatively, the `PT00:00:00.000000` format can be used.

## Example

The following example uses `to_j_duration` to convert duration strings to duration objects.

```python order=:log
from deephaven.time import to_j_duration

five_hours = to_j_duration("PT5H")
one_minute_fifteen_seconds = to_j_duration("PT00:01:15")
negative_one_hour = to_j_duration("PT-1H")

print(five_hours)
print(one_minute_fifteen_seconds)
print(negative_one_hour)
```

The following example uses [pandas](https://pandas.pydata.org/) to add durations to a date-time object before converting it to a Java Instant via [`to_j_instant`](../../time/datetime/to_j_instant.md).

```python order=:log
from pandas import to_datetime, to_timedelta
from deephaven.time import to_j_duration, to_j_instant

date_time = to_datetime("2020-01-01T00:00:00Z", format="ISO8601")
pos_duration = to_timedelta("1h")
neg_duration = to_timedelta("-1m1s")

print(to_j_instant(date_time + pos_duration))
print(to_j_instant(date_time + neg_duration))
```

-->

## Related Documentation

- [Built-in query language constants](../../../how-to-guides/built-in-constants.md)
- [Built-in query language variables](../../../how-to-guides/built-in-variables.md)
- [Built-in query language functions](../../../how-to-guides/built-in-functions.md)
- [`date-time`](./date-time.md)
- [Pydoc](/core/pydoc/code/deephaven.dtypes.html#deephaven.dtypes.Period)
