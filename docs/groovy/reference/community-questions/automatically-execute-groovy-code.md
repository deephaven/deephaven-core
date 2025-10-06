---
title: How do I automatically execute Groovy code based on a Deephaven table column value?
sidebar_label: How do I automatically execute Groovy code based on a Deephaven table column value?
---

<em>Can I automatically execute Groovy code when incoming data in a ticking table meets certain criteria?</em>

<p></p>

Yes. This is common in algorithmic trading. There are two ways it can be achieved:

The first is by using [`where`](../table-operations/filter/where.md) and an [`update`](../table-operations/select/update.md) operation. The following example prints to the console when new data in the `X` column is greater than 0.5:

```groovy order=events,t
t = timeTable("PT00:00:01").update("X = random()")

doSomething = { x ->
    println "Doing something: ${x}"
    return true
}

events = t.where("X > 0.5").update("DidIt = doSomething(X)")
```

Alternatively, the same behavior can be accomplished with a [table listener](../../how-to-guides/table-listeners-groovy.md). Table listeners listen to ticking tables for new data. Table listeners can be used to trigger external actions, such as Groovy code, upon table updates. You can set the listener to trigger code based on new data meeting one or more criteria such as a value being in a certain range or a string containing a particular substring.

The following example prints `Doing stuff` any time a new value in the `X` column of our table is greater than 0.5:

```groovy order=t
import io.deephaven.engine.table.TableUpdate
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter

t = timeTable("PT00:00:01").update("X = random()")

myTableListener = new InstrumentedTableUpdateListenerAdapter(t, false) {

    @Override
    public void onUpdate(TableUpdate upstream) {
        it1 = upstream.added.iterator()
        println "X"
        colSrc = t.getColumnSource("X")

        while (it1.hasNext()) {
            it = it1.next()
            xData = colSrc.getDouble(it)

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
