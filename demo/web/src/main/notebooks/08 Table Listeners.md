# Table Listeners

One of Deephaven's defining features is its ability to handle real-time data with continually updating tables. Table listeners allow you to define functions that execute every time one of these tables updates. Let's go through a few examples of using these listeners in Python.

To start, let's make a table that updates every two seconds with a random number.

```python
from deephaven.TableTools import timeTable

import random

table = timeTable('00:00:02').update("Number = (int)random.randint(1,100)")
```

As you can see, this table updates with a new row every two seconds.

Now let's add a listener. This listener will simply log every update to the table.

```python
from deephaven import listen

def log_table_update(update):
    print(f"FUNCTION LISTENER: update={update}")

log_table_update_handler = listen(table, log_table_update)
```

As you can see, each time a new row is added, the `print` statement in the listener is executed. Let's extend the listener to log the added values. To do this, we need to reference our `table` inside the listener, and use the `update` object's iterators to access the indexes of what has been added.

```python
def log_table_update_with_row(update):
    print(f"FUNCTION LISTENER: update={update}")

    added_iterator = update.added.iterator()

    while added_iterator.hasNext():
        added_index = added_iterator.nextLong()
        added_number = table.getColumnSource("Number").get(added_index)

        print(f"Added number: {added_number}")
log_table_update_with_row_handler = listen(table, log_table_update_with_row)
```

This example covers just the bare minimum of using listeners. Any code can be used as a listener. This allows you to do things such as trigger HTTP webhooks, Slack notifications, or email notifications.

## Parameterizing the listener

What if we had two time tables and wanted to reuse the same listener method? We can't change the listener's signature, so parameterizing the tables that way won't work. And we definitely don't want to copy-paste the code into separate functions that reference the two tables. Instead, we can create a function that takes a table and returns a function that takes the `update` object. If you're familiar with functional programming, this is very similar to [currying](https://en.wikipedia.org/wiki/Currying).

Let's start with our two time tables.

```python
table_one = timeTable('00:00:02').update("Number = (int)random.randint(1,100)")
table_two = timeTable('00:00:05').update("Number = (int)random.randint(1,100)")
```

Now let's build our function.

```python
def log_table_builder(table):
    def log_table_update_with_row(update):
        print(f"FUNCTION LISTENER: update={update}")

        added_iterator = update.added.iterator()

        while added_iterator.hasNext():
            added_index = added_iterator.nextLong()
            added_number = table.getColumnSource("Number").get(added_index)

            print(f"Added number: {added_number}")
    return log_table_update_with_row
```

As you can see, the `log_table_builder` function takes the `table` parameter, and returns the `log_table_update_with_row` method that takes the `update` parameter.

Now let's use this function to build our listener methods, and apply them to our tables.

```python
table_one_listener = log_table_builder(table_one)
table_two_listener = log_table_builder(table_two)

table_one_handler = listen(table_one, table_one_listener)
table_two_handler = listen(table_two, table_two_listener)
```

We've successfully reused our base listener across two tables! 


Now to turn off the table listeners on all tables, we deregister each handler.

```python
log_table_update_handler.deregister()

log_table_update_with_row_handler.deregister()

table_one_handler.deregister()
table_two_handler.deregister()
```


To learn more about table listeners, see [How to create table listeners in Python](https://deephaven.io/core/docs/how-to-guides/table-listeners-python/).
