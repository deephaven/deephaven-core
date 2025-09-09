---
title: parseLocalDate
---

`parseLocalDate` parses the string argument as a local date, which is a date without a time or time zone.
Date strings are formatted according to the ISO 8601 date time format as YYYY-MM-DD.

## Syntax

```
parseLocalDate(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted. Format is `yyyy-MM-dd`.

</Param>
</ParamTable>

## Returns

An Instant.

## Examples

```groovy order=:log
date = parseLocalDate("1995-08-02")

println date
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalDate(java.lang.String))
