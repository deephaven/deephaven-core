---
title: parseLocalDateQuiet
---

`parseLocalDateQuiet` parses the string argument as a local date, which is a date without a time or time zone.
Date-strings are formatted according to the ISO 8601 date time format as YYYY-MM-DD.

"Quiet" methods return `null` instead of throwing an exception when encountering a string that cannot be parsed.

## Syntax

```
parseLocalDateQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted. Format is `yyyy-MM-dd`.

</Param>
</ParamTable>

## Returns

A local date, or `null` if invalid input is given.

## Examples

```groovy order=:log
date = parseLocalDateQuiet("1995-08-02")
dateinvalid = parseLocalDateQuiet("1995e-08-02")
println date
println dateinvalid
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDateQuiet(java.lang.String))
