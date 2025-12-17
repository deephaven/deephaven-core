---
title: parsePeriod
---

The `parsePeriod` method returns the string argument as a Period, which is a unit of time in terms of calendar time (days, weeks, months, years).

## Syntax

```
parsePeriod(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted.

Date-time strings are formatted according to the ISO-8601 duration format as `PnYnMnD` and `PnW`, where the coefficients can be positive or negative. Zero coefficients can be omitted. Optionally, the string can begin with a negative sign.

Examples:
`"P2Y"` -- Period.ofYears(2)
`"P3M"` -- Period.ofMonths(3)
`"P4W"` -- Period.ofWeeks(4)
`"P5D"` -- Period.ofDays(5)
`"P1Y2M3D"` -- Period.of(1, 2, 3)
`"P1Y2M3W4D"` -- Period.of(1, 2, 25)
`"P-1Y2M"` -- Period.of(-1, 2, 0)
`"-P1Y2M"` -- Period.of(-1, -2, 0)

</Param>
</ParamTable>

## Returns

A Period.

## Examples

```groovy order=:log
period = parsePeriod("P33Y4M23D")

println period
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parsePeriod(java.lang.String))
