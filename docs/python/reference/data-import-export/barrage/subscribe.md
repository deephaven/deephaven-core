---
title: subscribe
---

The `subscribe` method subscribes to a published remote table that matches the specified `ticket`.

> [!NOTE]
> If the remote table is closed or its owner session is closed, the `ticket` becomes invalid. If the same `ticket` is subscribed multiple times, multiple subscriptions will be created.

## Syntax

```python syntax
subscribe(ticket: bytes) -> Table
```

## Parameters

<ParamTable>
<Param name="ticket" type="bytes">

The bytes of the ticket.

</Param>
</ParamTable>

## Returns

A `Table` that is a subscribed to the remote table.

## Examples

The following example installs the Deephaven Python Client ([`pydeephaven`](/core/client-api/python/)) on a Deephaven server running on port `10001`. It also starts a Deephaven server running locally on port `10000` with [anonymous authentication](../../../how-to-guides/authentication/auth-anon.md).

Next, the client creates a table on the server on port `10000`. A shared ticket is created that publishes the table, which allows it to be shared with other sessions. Finally, a Barrage session is started that listens to the server on port `10000`, and a new local table that is subscribed to the shared ticket's table is obtained.

```python docker-config=pyclient order=local_table,new_local_table
import os

os.system("pip install pydeephaven")
from deephaven.barrage import barrage_session
from pydeephaven.session import SharedTicket
from pydeephaven import Session

# Create a client session connected to the server running on port 10000
client_session = Session(
    host="deephaven.local",
    port=10000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)
# Create a table on that server with the client session
client_table = client_session.time_table("PT1s").update(["X = 0.1 * i", "Y = sin(X)"])
# Create a ticket through which `client_table` can be published
client_ticket = SharedTicket.random_ticket()
# Publish the ticket (table)
client_session.publish_table(client_ticket, client_table)

# Create a barrage session that listens to the server on port 10000
my_barrage_session = barrage_session(
    host="deephaven.local",
    port=10000,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="YOUR_PASSWORD_HERE",
)
# Subscribe to the client table ticking data
local_table = my_barrage_session.subscribe(client_ticket.bytes)
# Perform operations on this now-local table
new_local_table = local_table.last_by()
```

> [!IMPORTANT]
> Shared tickets have a life cycle tied to the source. Tickets can be fetched by other Deephaven sessions to access the table _only_ as long as the table is not released. When the table is released either through an explicit call of the [`close`](./close.md) method, implicitly through garbage collection, or through the closing of the publishing session, the shared ticket will no longer be valid.

<details>
  <summary> The following `docker-compose.yml` file was used to run the above code block. It starts two Deephaven servers: one running on port `10001` and the other on port `10000`. The code block was run from the server running on port `10001`. </summary>

```yaml
services:
  deephaven1:
    image: ghcr.io/deephaven/server:latest
    ports:
      - 10001:10000
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler

  deephaven2:
    image: ghcr.io/deephaven/server:latest
    ports:
      - 10000:10000
    volumes:
      - ./data:/data
    environment:
      - START_OPTS=-Xmx4g -Dauthentication.psk=YOUR_PASSWORD_HERE
```

</details>

## Related documentation

- [Pydoc](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession.subscribe)
