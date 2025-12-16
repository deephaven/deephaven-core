---
title: publish
---

The `publish` method publishes a source ticket to a result ticket. This is a low-level method that can be used to publish any server object (not just tables) to a shared ticket.

The source ticket represents a previously fetched server object to be published, and the result ticket (typically a [`SharedTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket)) is the ticket to publish to. The result ticket can then be fetched by other sessions to access the object as long as the object is not released.

This method is used together with the [`fetch`](./fetch.md) method to share server objects between sessions.

> [!NOTE]
> For tables specifically, use the higher-level [`publish_table`](./publish-table.md) method instead.

> [!IMPORTANT]
> Shared tickets have a life cycle tied to the source. Tickets can be fetched by other Deephaven sessions to access the object _only_ as long as the object is not released. When the object is released either through an explicit call of the `release` method, implicitly through garbage collection, or through the closing of the publishing session, the shared ticket will no longer be valid.

## Syntax

```python syntax
publish(source_ticket: Ticket, result_ticket: Ticket) -> None
```

## Parameters

<ParamTable>
<Param name="source_ticket" type="Ticket">

The source ticket to publish from.

</Param>
<Param name="result_ticket" type="Ticket">

The result ticket to publish to, typically a [`SharedTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket).

</Param>
</ParamTable>

## Returns

This method returns `None`.

## Examples

### Publishing a Figure object

The following example publishes a Figure (plot) object to a shared ticket so it can be accessed by another session.

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

# Publish the plot to a shared ticket
shared_ticket = SharedTicket.random_ticket()
session.publish(export_ticket, shared_ticket)

# Another session can now fetch this plot
sub_session = Session(host="localhost", port=10000)
server_obj = ServerObject(type="Figure", ticket=shared_ticket)
sub_plugin_client = sub_session.plugin_client(server_obj)

# The subscriber session can now interact with the plot
payload, refs = next(sub_plugin_client.resp_stream)

sub_plugin_client.close()
sub_session.close()
plugin_client.close()
session.close()
```

### Publishing a table using generic publish

While [`publish_table`](./publish-table.md) is recommended for tables, you can also use the generic `publish` method:

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket, ScopeTicket

# Create a session and table
session = Session(host="localhost", port=10000)
session.run_script("source = empty_table(20).update('X = i')")

# Fetch the table as an export ticket
export_ticket = session.fetch(ScopeTicket.scope_ticket("source"))

# Publish using the generic publish method
shared_ticket = SharedTicket.random_ticket()
session.publish(export_ticket, shared_ticket)

# Another session can fetch this table
sub_session = Session(host="localhost", port=10000)
table = sub_session.fetch_table(shared_ticket)

sub_session.close()
session.close()
```

## Related documentation

- [`fetch`](./fetch.md) - Fetch a server object by ticket
- [`publish_table`](./publish-table.md) - Publish a table specifically (higher-level method)
- [`fetch_table`](./fetch-table.md) - Fetch a table by shared ticket
- [Session API](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.publish)
