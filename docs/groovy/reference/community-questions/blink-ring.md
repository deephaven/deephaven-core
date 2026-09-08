---
title: "How can I construct a ring table of the first row of the last N update cycles of a blink table?"
sidebar_label: "How can I store the first row of the last N blinks of a table?"
---

_I have a blink table, from which I want to extract the first row of the last N blinks into a separate ring table. How can I do that?_

To achieve this:

- Use a [Table listener](../../how-to-guides/table-listeners-groovy.md) to listen to the source blink table.
- Use a [Dynamic table writer](../../how-to-guides/table-publisher.md) to write the first row each update cycle.
- Convert the result to a ring table.

Here's a complete example:

```groovy order=source,result,resultRing
import io.deephaven.engine.table.impl.util.DynamicTableWriter
import io.deephaven.engine.updategraph.DynamicNode
import io.deephaven.time.DateTimeUtils
import static io.deephaven.api.agg.Aggregation.*
import io.deephaven.engine.table.impl.sources.ring.RingTableTools
import static io.deephaven.engine.util.TableTools.*
import io.deephaven.engine.context.ExecutionContext
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

source = timeTableBuilder().period("PT0.2S").blinkTable(true).build().update("X = (double)ii")

tableWriter = new DynamicTableWriter(
    ["Timestamp", "X"] as String[],
    [DateTimeUtils.now().getClass(), double.class] as Class[]
)

result = tableWriter.getTable()

// Capture the execution context for thread-safe operations
ctx = ExecutionContext.getContext()

listener = new InstrumentedTableUpdateListenerAdapter("MyListener", source, false) {
    @Override
    void onUpdate(TableUpdate update) {
        added = update.added()
        if (added.size() > 0) {
            firstTimestamp = source.getColumnSource("Timestamp").get(added.get(0))
            firstX = source.getColumnSource("X").getDouble(added.get(0))

            // Use the captured execution context for thread-safe writes
            ctx.apply(() -> {
                tableWriter.logRow(firstTimestamp, firstX)
            })
        }
    }

    @Override
    void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        originalException.printStackTrace()
    }
}

// Store the listener reference for potential cleanup
listenerHandle = source.addUpdateListener(listener)

resultRing = RingTableTools.of(result, 10).where("!isNull(X)")
```

![First row of last 10 blinks](../../assets/reference/faq/blink-ring.gif)

This example shows that the solution works, since the `X` column contains only the value `0`, which is the value from the first row on every update cycle.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
