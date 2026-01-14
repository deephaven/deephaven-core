---
title: parseLocalTime
---

`parseLocalTime` parses the string argument as a local time, which is the time that would be read from a clock and does not have a date or timezone.

## Syntax

```
parseLocalTime(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted. Format is `hh:mm:ss[.nnnnnnnnn]`.

</Param>
</ParamTable>

## Returns

A local time. Throws an exception if the time cannot be parsed.

## Examples

```groovy order=:log
time = parseLocalTimeQuiet("11:29:23.123123")
println time
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalTime(java.lang.String))
