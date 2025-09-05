---
title: timeZoneAliasRm
---

The `timeZoneAliasAdd` method removes a time zone alias.

## Syntax

```
timeZoneAliasRm(alias, timeZone)
```

## Parameters

<ParamTable>
<Param name="alias" type="String">

The new alias name.

</Param>
<Param name="timeZone" type="String">

The [ZoneId](https://docs.oracle.com/en/java/javase/17/docs//api/java.base/java/time/ZoneId.html) name.

</Param>
</ParamTable>

## Examples

```groovy reset
timeZoneAliasRm("ET")

println parseTimeZoneQuiet("ET")
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`parseTimeZoneQuiet`](./parseTimeZoneQuiet.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZoneAliasRm(java.lang.String))
