---
title: fetch_table
---

The `fetch_table` method fetches a table by [`SharedTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket).

This is a convenience method specifically for tables. For fetching other types of server objects (such as plots, pandas DataFrames, or plugin objects), use the generic [`fetch`](./fetch.md) method.

> [!NOTE]
> If the remote table is closed or its owner session is closed, the ticket becomes invalid.

## Syntax

```python syntax
fetch_table(ticket: SharedTicket) -> Table
```

## Parameters

<ParamTable>
<Param name="ticket" type="SharedTicket">

A [`SharedTicket`](/core/client-api/python/code/pydeephaven.ticket.html#pydeephaven.ticket.SharedTicket) object.

</Param>
</ParamTable>

## Returns

A Table object.

## Examples

The following example fetches a table that was published to a shared ticket by another session.

```python skip-test
import os

os.system("pip install pydeephaven")
from pydeephaven import Session
from pydeephaven.ticket import SharedTicket

# Assume another session published a table to this shared ticket
# (In practice, you would get this ticket from the publishing session)
shared_ticket = SharedTicket.random_ticket()  # Use the actual shared ticket

# Connect and fetch the table
session = Session(host="localhost", port=10000)
table = session.fetch_table(shared_ticket)

session.close()
```

For a complete example showing how to use `fetch_table` with tables published from another session, see the Barrage [`subscribe`](../../data-import-export/barrage/subscribe.md) documentation.

## Related documentation

- [`publish_table`](./publish-table.md) - Publish a table to a shared ticket
- [`fetch`](./fetch.md) - Generic method to fetch any server object
- [`publish`](./publish.md) - Generic method to publish any server object
- [`subscribe`](../../data-import-export/barrage/subscribe.md) - Subscribe to a published table in real-time
- [Session API](/core/client-api/python/code/pydeephaven.session.html#pydeephaven.session.Session.fetch_table)
