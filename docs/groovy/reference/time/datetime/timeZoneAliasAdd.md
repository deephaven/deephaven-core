---
title: timeZoneAliasAdd
---

The `timeZoneAliasAdd` method adds a new time zone alias.

## Syntax

```
timeZoneAliasAdd(alias, timeZone)
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

```groovy order=:log
timeZoneAliasAdd("PTR", "America/Puerto_Rico")

println timeZone("PTR")
```

## Related Documentation

- [Time in Deephaven](../../../conceptual/time-in-deephaven.md)
- [`timeZone`](./timeZone.md)
- [Javadoc](https://deephaven.io/core/javadoc/io/deephaven/time/DateTimeUtils.html#timeZoneAliasAdd(java.lang.String,java.lang.String))
