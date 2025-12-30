---
title: Replayer
---

`Replayer` is used to replay historical data with timestamps in a new, in-memory table.

## Syntax

```
Replayer(startTime, endTime)
```

## Parameters

<ParamTable>
<Param name="startTime" type="DateTime">

Historical data start time

</Param>
<Param name="endTime" type="DateTime">

Historical data end time.

</Param>
</ParamTable>

## Returns

A `Replayer` object that can be used to replay historical data.

## Methods

`Replayer` supports the following methods:

- [`replay()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#replay(io.deephaven.engine.table.Table,java.lang.String)) - Prepares a historical table for replaying.
- [`start()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#start()) - Starts replaying data.
- [`shutdown()`](https://deephaven.io/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html#shutdown()) - Shuts the replayer down.

## Example

The following example creates some fake historical data with timestamps and then replays it.

```groovy skip-test
import io.deephaven.engine.table.impl.replay.Replayer

result = newTable(
    instantCol("DateTime", convertDateTime("2000-01-01T00:00:01 NY"), convertDateTime("2000-01-01T00:00:03 NY"), convertDateTime("2000-01-01T00:00:06 NY")),
    intCol("Number", 1, 3, 6)
)

startTime = convertDateTime("2000-01-01T00:00:00 NY")
endTime = convertDateTime("2000-01-01T00:00:07 NY")

resultReplayer = new Replayer(startTime, endTime)

replayedResult = resultReplayer.replay(result, "DateTime")

resultReplayer.start()
```

<LoopedVideo src='../../../assets/reference/create/Replayer_refExample.mp4' />

## Related documentation

- [How to replay historical data](../../../how-to-guides/replay-data.md)
- [Javadoc](/core/javadoc/io/deephaven/engine/table/impl/replay/Replayer.html)
