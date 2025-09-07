---
title: timeZone
---

`timeZone` returns the time zone for a time zone name, or the default time zone if no name is provided.

## Syntax

```
timeZone()
timeZone(timeZone)
```

## Parameters

<ParamTable>
<Param name="timeZone" type="string">

The time zone name.

</Param>
</ParamTable>

## Returns

The corresponding time zone.

## Examples

```groovy order=:log
println timeZone()

println timeZone("ET")
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZone())
