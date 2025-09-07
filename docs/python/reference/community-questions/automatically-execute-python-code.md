---
title: How do I automatically execute Python code based on a Deephaven table column value?
sidebar_label: How do I automatically execute Python code based on a Deephaven table column value?
---

<em>Can I automatically execute Python code when incoming data in a ticking table meets certain criteria?</em>

<p></p>

Yes. This is common in algorithmic trading. There are two ways it can be achieved:

The first is by using [`where`](../../reference/table-operations/filter/where.md) and an [`update`](../../reference/table-operations/select/update.md) operation. The following example prints to the console when new data in the `X` column is greater than 0.5:

```python order=events,t
from deephaven import time_table

t = time_table("PT00:00:01").update("X = random()")


def do_something(x) -> bool:
    print(f"Doing something: {x}")
    return True


events = t.where("X > 0.5").update("DidIt = do_something(X)")
```

Alternatively, the same behavior can be accomplished with a [table listener](../../how-to-guides/table-listeners-python.md). Table listeners listen to ticking tables for new data. Table listeners can be used to trigger external actions, such as Python code, upon table updates. You can set the listener to trigger code based on new data meeting one or more criteria such as a value being in a certain range or a string containing a particular substring.

New data is captured by table listeners in the form of a [dictionary](https://docs.python.org/3/tutorial/datastructures.html#dictionaries), where column names are the keys.

The following example prints `Doing stuff` any time a new value in the `X` column of our table is greater than 0.5:

```python order=t
from deephaven import time_table
from deephaven.table_listener import listen


t = time_table("PT00:00:01").update("X = random()")


def my_table_listener(update, is_replay):
    print("New row in t table")

    if update.added()["X"][0] > 0.5:
        print("Doing stuff")


my_table_handler = listen(t, my_table_listener)
```

> [!NOTE]
> These FAQ pages contain answers to questions about Deephaven Community Core that our users have asked in our [Community Slack](/slack). If you have a question that is not in our documentation, [join our Community](/slack) and we'll be happy to help!
