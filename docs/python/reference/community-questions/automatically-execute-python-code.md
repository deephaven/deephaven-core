---
title: How do I automatically execute Python code based on a Deephaven table column value?
---

<em>Can I automatically execute Python code when incoming data in a ticking table meets certain criteria?</em>

<p></p>

Yes. This is common in algorithmic trading, and you can do it in two ways. Use [`where`](../../reference/table-operations/filter/where.md) and [`update`](../../reference/table-operations/select/update.md) when you want a live result table of matching rows — on an append-only source like `time_table`, qualifying rows accumulate as a running record. Use a [table listener](../../how-to-guides/table-listeners-python.md) when the action is a side effect outside the query engine, such as sending an alert, and you don't need a result table.

The first approach uses `where` and `update`. The following example prints to the console when new data in the `X` column is greater than 0.5:

```python ticking-table order=events,t
from deephaven import time_table

t = time_table("PT00:00:01").update("X = random()")


def do_something(x) -> bool:
    print(f"Doing something: {x}")
    return True


events = t.where("X > 0.5").update("DidIt = do_something(X)")
```

You can also use a [table listener](../../how-to-guides/table-listeners-python.md). The listener function receives a `TableUpdate` object. Its `added` method returns a dictionary that maps each column name to a NumPy array of the newly added values. Table listeners are notified every time a ticking table changes, and receive the added, removed, and modified rows. You can set the listener to trigger code based on new data meeting one or more criteria such as a value being in a certain range or a string containing a particular substring.

The following example prints `Doing stuff` any time a new value in the `X` column of our table is greater than 0.5:

```python ticking-table order=t
from deephaven import time_table
from deephaven.table_listener import listen


t = time_table("PT00:00:01").update("X = random()")


def my_table_listener(update, is_replay):
    added = update.added()["X"]
    for x in added:
        if x > 0.5:
            print("Doing stuff")


my_table_handler = listen(t, my_table_listener)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
