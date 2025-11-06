---
title: "How can I construct a ring table of the first row of the last N update cycles of a blink table?"
sidebar_label: "How can I store the first row of the last N blinks of a table?"
---

_I have a blink table, from which I want to extract the first row of the last N blinks into a separate ring table. How can I do that?_

To achieve this:

- Use a [Table listener](../../how-to-guides/table-listeners-groovy.md) to listen to the source blink table.
- Use a [Dynamic table writer](../../how-to-guides/dynamic-table-writer.md) to write the first row each update cycle.
- Convert the result to a ring table.

Here's a complete example:

```groovy skip-test
import io.deephaven.engine.table.impl.util.DynamicTableWriter
import io.deephaven.engine.updategraph.DynamicNode
import io.deephaven.time.DateTimeUtils

import static io.deephaven.api.agg.Aggregation.*
import static io.deephaven.engine.util.TableTools.*

source = timeTable("PT0.2s", true).update("X = (double)ii")

tableWriter = new DynamicTableWriter(
    ["Timestamp": DateTimeUtils.currentInstant().getClass(), "X": double.class]
)

result = tableWriter.getTable()

handle = source.listenDo { update, isReplay ->
    added = update.added()
    if (added.size() > 0) {
        firstTimestamp = added.getColumn("Timestamp").get(0)
        firstX = added.getColumn("X").getDouble(0)
        
        tableWriter.logRow(firstTimestamp, firstX)
    }
}

result_ring = ringTable(result, 10)
```

![First row of last 10 blinks](../../assets/reference/faq/blink-ring.gif)

This example shows that the solution works, since the `X` column contains only the value `0`, which is the value from the first row on every update cycle.

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
