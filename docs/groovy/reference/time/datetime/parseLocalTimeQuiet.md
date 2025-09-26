---
title: parseLocalTimeQuiet
---

`parseLocalTimeQuiet` parses the string argument as a local time, which is the time that would be read from a clock and does not have a date or timezone.
Local time strings can be formatted as `hh:mm:ss[.nnnnnnnnn]`.

"Quiet" methods return `null` instead of throwing an exception when encountering a string that cannot be parsed.

## Syntax

```
parseLocalTimeQuiet(s)
```

## Parameters

<ParamTable>
<Param name="s" type="string">

The string to be converted. Format is `hh:mm:ss[.nnnnnnnnn]`.

</Param>
</ParamTable>

## Returns

A local time, or `null` if invalid input is given.

## Examples

```groovy order=:log
time = parseLocalTimeQuiet("11:29:23.123123")
timeinvalid = parseLocalTimeQuiet("2:333:342")
println time
println timeinvalid
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Durations](../../query-language/types/durations.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#parseLocalTimeQuiet(java.lang.String))
