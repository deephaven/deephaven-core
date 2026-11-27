---
title: How do I automatically execute Groovy code based on a Deephaven table column value?
---

<em>Can I automatically execute Groovy code when incoming data in a ticking table meets certain criteria?</em>

<p></p>

Yes. This is common in algorithmic trading, and you can do it in two ways:

The first is by using [`where`](../table-operations/filter/where.md) and an [`update`](../table-operations/select/update.md) operation. The following example creates a [`timeTable`](../table-operations/create/timeTable.md) and prints to the console when new data in the `X` column is greater than 0.5:

```groovy order=events,t
t = timeTable("PT00:00:01").update("X = random()")

doSomething = { x ->
    println "Doing something: ${x}"
    return true
}

events = t.where("X > 0.5").update("DidIt = doSomething(X)")
```

Alternatively, use a [table listener](../../how-to-guides/table-listeners-groovy.md) to get the same behavior. Table listeners watch ticking tables for new data and can trigger external actions, such as Groovy code, when the table updates. The listener's `onUpdate` method receives a `TableUpdate`. Iterate over its added row keys and read each value from the table's column source.

The following example uses an `InstrumentedTableUpdateListenerAdapter` to print `Doing stuff` any time a new value in the `X` column of our table is greater than 0.5:

```groovy order=t
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

t = timeTable("PT00:00:01").update("X = random()")

myTableListener = new InstrumentedTableUpdateListenerAdapter(t, false) {

    @Override
    public void onUpdate(TableUpdate upstream) {
        def iter = upstream.added().iterator()
        println "New rows in t"
        def colSrc = t.getColumnSource("X")

        while (iter.hasNext()) {
            def rowKey = iter.next()
            def xData = colSrc.getDouble(rowKey)

            if (xData > 0.5) {
                println "Doing stuff"
            }
        }
    }
}

t.addUpdateListener(myTableListener)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
