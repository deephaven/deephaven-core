---
title: close
---

The `close` method closes the [`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession).

> [!NOTE]
> A [`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession) created by [`barrage_session`](./barrage-session.md) owns its gRPC channel, so `close` shuts down the channel as well. If closing the session or shutting down the channel throws an exception, `close` raises a `DHError`. `close` waits up to 10 seconds for the channel to terminate.

`BarrageSession` is also a context manager: a `with` block closes the session when it exits.

## Syntax

```python syntax
close()
```

## Parameters

This method does not take any parameters.

## Returns

`None`.

## Examples

The following examples use anonymous authentication, so the target server must allow it (`-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler`). For a server that requires credentials, pass `auth_type` and `auth_token` to [`barrage_session`](./barrage-session.md) and to `Session`.

```python skip-test
from deephaven.barrage import barrage_session

barrage_sesh = barrage_session("localhost", 10000)

barrage_sesh.close()
```

The following example uses a `with` block to close the session automatically:

```python skip-test
from deephaven.barrage import barrage_session
from pydeephaven import Session
from pydeephaven.session import SharedTicket

# Publish a table to a shared ticket with the Python client
client_session = Session(host="localhost", port=10000)
client_table = client_session.empty_table(10).update(["X = i"])
ticket = SharedTicket.random_ticket()
client_session.publish_table(ticket, client_table)

# The session closes when the with block exits
with barrage_session("localhost", 10000) as barrage_sesh:
    local_table = barrage_sesh.snapshot(ticket.bytes)
```

## Related documentation

- [What is Barrage?](../../../conceptual/what-is-barrage.md)
- [Capture Python client tables](../../../how-to-guides/capture-tables.md)
- [`barrage_session`](./barrage-session.md)
- [Pydoc](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession.close)
