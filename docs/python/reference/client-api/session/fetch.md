---
title: fetch
---

The `fetch` method fetches a server object by ticket and returns an [`ExportTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.ExportTicket). This is a low-level method that can be used to fetch any server object (not just tables).

The ticket represents a fetchable server object, such as a [`PluginClient`](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html#pydeephaven.experimental.plugin_client.PluginClient) or [`Fetchable`](/core/client-api/python/code/pydeephaven.experimental.plugin_client.html#pydeephaven.experimental.plugin_client.Fetchable). This method is used together with the [`publish`](./publish.md) method to share server objects between sessions.

> [!NOTE]
> For tables specifically, use the higher-level [`fetch_table`](./fetch-table.md) method instead.

## Syntax

```python syntax
fetch(ticket: Ticket) -> ExportTicket
```

## Parameters

<ParamTable>
<Param name="ticket" type="Ticket">

The ticket representing a fetchable server object.

</Param>
</ParamTable>

## Returns

An [`ExportTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.ExportTicket) object that can be used to reference the fetched server object.

## Examples

### Fetching and publishing a Figure object

The following example fetches a Figure (plot) object and publishes it to a shared ticket.

```python skip-test
import os

os.system("pip install pydeephaven")
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket, ServerObject

# Create a session and generate a plot
session = Session(host="localhost", port=10000)
session.run_script("""
from deephaven.plot.figure import Figure
from deephaven import empty_table

source = empty_table(20).update([
    "X = 0.1 * i",
    "Y = randomDouble(0.0, 5.0)"
])

plot = Figure().plot_xy(
    series_name="Random numbers",
    t=source,
    x="X",
    y="Y"
).show()
""")

# Get the plot object from the session
plot_obj = session.exportable_objects["plot"]

# Create a plugin client to interact with the plot
plugin_client = session.plugin_client(plot_obj)

# Fetch the plugin object to get an export ticket
export_ticket = session.fetch(plugin_client)

# Now you can publish this export ticket to share it with other sessions
shared_ticket = SharedTicket.random_ticket()
session.publish(export_ticket, shared_ticket)

plugin_client.close()
session.close()
```

### Fetching a table using generic fetch

While [`fetch_table`](./fetch-table.md) is recommended for tables, you can also use the generic `fetch` method:

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import ScopeTicket

# Create a session and table
session = Session(host="localhost", port=10000)
session.run_script("source = empty_table(20).update('X = i')")

# Fetch the table as an export ticket using the generic fetch method
export_ticket = session.fetch(ScopeTicket.scope_ticket("source"))

# The export_ticket can now be used to reference the table
# or published to a shared ticket for other sessions to access

session.close()
```

### Fetching a shared plugin object

The following example shows how to fetch a plugin object that was published to a shared ticket by another session.

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket, ServerObject

# Assume another session published a Figure to this shared ticket
shared_ticket = SharedTicket.random_ticket()  # Use the actual shared ticket

# Connect to the session and fetch the shared object
session = Session(host="localhost", port=10000)

# Create a ServerObject reference with the appropriate type
server_obj = ServerObject(type="Figure", ticket=shared_ticket)

# Create a plugin client to interact with the shared Figure
plugin_client = session.plugin_client(server_obj)

# You can now interact with the Figure through the plugin client
payload, refs = next(plugin_client.resp_stream)

plugin_client.close()
session.close()
```

## Related documentation

- [`publish`](./publish.md) - Publish a server object to a shared ticket
- [`fetch_table`](./fetch-table.md) - Fetch a table specifically (higher-level method)
- [`publish_table`](./publish-table.md) - Publish a table to a shared ticket
- [Session API](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.fetch)
