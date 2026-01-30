---
title: publish_table
---

The `publish_table` method publishes a table to a given [`SharedTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket). The ticket can then be used by another session to fetch the table.

The shared ticket can be fetched by other sessions to access the table as long as the table is not released. When the table is released either through an explicit call of the `close` method on it, implicitly through garbage collection, or through the closing of the publishing session, the shared ticket will no longer be valid.

This is a convenience method specifically for tables. For publishing other types of server objects (such as plots, pandas DataFrames, or plugin objects), use the generic [`publish`](./publish.md) method.

## Syntax

```python syntax
publish_table(ticket: SharedTicket, table: Table) -> None
```

## Parameters

<ParamTable>
<Param name="ticket" type="SharedTicket">

A [`SharedTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket) object.

</Param>
<Param name="table" type="Table">

A Table object to publish.

</Param>
</ParamTable>

## Returns

This method returns `None`.

## Examples

The following example creates a table and publishes it to a shared ticket so that other sessions can access it.

```python skip-test
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket

# Create a session and table
pub_session = Session(host="localhost", port=10000)
table = pub_session.empty_table(1000).update(["X = i", "Y = 2*i"])

# Create and publish to a shared ticket
shared_ticket = SharedTicket.random_ticket()
pub_session.publish_table(shared_ticket, table)

# Another session can now fetch this table
sub_session = Session(host="localhost", port=10000)
fetched_table = sub_session.fetch_table(shared_ticket)

sub_session.close()
pub_session.close()
```

For a complete example showing how to use `publish_table` with Barrage sessions, see the Barrage [`subscribe`](../../data-import-export/barrage/subscribe.md) documentation.

## Related documentation

- [`fetch_table`](./fetch-table.md)
- [`publish`](./publish.md)
- [`fetch`](./fetch.md)
- [`subscribe`](../../data-import-export/barrage/subscribe.md)
- [`Session`](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session)
