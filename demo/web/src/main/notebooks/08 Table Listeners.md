# Table Listeners

One of Deephaven's defining features is its ability to handle real-time data with continually updating tables. Table listeners allow you to define functions that execute every time one of these tables updates. Let's go through a few examples of using these listeners in Python.

To start, let's make a table that updates every two seconds with a random number.

```python
from deephaven import time_table

import random

table = time_table('00:00:02').update(["Number = (int)random.randint(1,100)"])
```

As you can see, this table updates with a new row every two seconds.

Now let's add a listener. This listener will simply log every update to the table.

```python
from deephaven.table_listener import listen

def log_table_update(update, is_replay):
    print(f"FUNCTION LISTENER: update={update}")

log_table_update_handler = listen(table, log_table_update)
```

As you can see, each time a new row is added, the `print` statement in the listener is executed. Let's extend the listener to log the added values. To do this, we need to reference the `update.added()` attribute inside the listener.

```python
def log_table_update_with_row(update, is_replay):
    print(f"FUNCTION LISTENER: update={update}")

    added_dict = update.added()

    if "Number" in added_dict:
        added_number = added_dict["Number"]
        print(f"Added number: {added_number}")

log_table_update_with_row_handler = listen(table, log_table_update_with_row)
```

This example covers just the bare minimum of using listeners. Any code can be used as a listener. This allows you to do things such as trigger HTTP webhooks, Slack notifications, or email notifications.

To learn more about table listeners, see [How to create table listeners in Python](https://deephaven.io/core/docs/how-to-guides/table-listeners-python/).
