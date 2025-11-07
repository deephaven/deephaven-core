---
title: snapshot
---

The `snapshot` method returns a snapshot of a published remote table that matches the specified `ticket`.

> [!NOTE]
> If the remote table is closed or its owner session is closed, the `ticket` becomes invalid. If the same `ticket` is snapshot multiple times, multiple snapshots will be created.

## Syntax

```python syntax
snapshot(ticket: bytes) -> Table
```

## Parameters

<ParamTable>
<Param name="ticket" type="bytes">

The bytes of the ticket.

</Param>
</ParamTable>

## Returns

A `Table` that is a snapshot of the remote table.

## Examples

The following example installs the Deephaven Python Client ([`pydeephaven`](/core/client-api/python/)) and creates a remote Deephaven server running on our local machine on port `9999` with [anonymous authentication](../../../how-to-guides/authentication/auth-anon.md).
Next, the remote server is used to create a table and a shared ticket pointing to the table that can be shared with other sessions.
Finally, a Barrage session is started that listens to the same server at port `9999`, and a snapshot of the shared ticket's table is obtained.

```python skip-test
import os

os.system("pip install pydeephaven")
from deephaven.barrage import barrage_session
from pydeephaven.session import SharedTicket
from pydeephaven import Session

# Create a client session connected to the server running on port 9999
client_session = Session(host="host.docker.internal", port=9999)
# Create a table on that server with the client session
client_table = client_session.empty_table(10).update(["X = i"])
# Create a ticket through which `client_table` can be published
client_ticket = SharedTicket.random_ticket()
# Publish the ticket (table)
client_session.publish_table(client_ticket, client_table)

# Create a barrage session that listens to the server on port 9999
my_barrage_session = barrage_session(host="host.docker.internal", port=9999)
# Subscribe to the client table ticking data
local_table = my_barrage_session.snapshot(client_ticket.bytes)
# Perform operations on this now-local table
new_local_table = local_table.where("X > 5")
```

<details>
  <summary> The following `docker-compose.yml` file was used to run the above code block. It starts two Deephaven servers: one running on port `10000` and the other on port `9999`. The code block was run from the server running on port `10000`. </summary>

```yaml
services:
  deephaven1:
    image: ghcr.io/deephaven/server:latest
    ports:
      - 10000:10000
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler

  deephaven2:
    image: ghcr.io/deephaven/server:latest
    ports:
      - 9999:10000
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler
```

</details>

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession.snapshot)
