---
title: barrage_session
---

The `barrage_session` method creates a new Deephaven gRPC session ([`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession)) to a remote server. Each call opens its own connection. Use the session's [`subscribe`](./subscribe.md) and [`snapshot`](./snapshot-barrage.md) methods to retrieve remote tables, and [`close`](./close.md) it when you are done.

<!--TODO: Add information about client mutual TLS authentication when it's added. -->

## Syntax

```python syntax
barrage_session(
  host: str,
  port: int = 10000,
  auth_type: str = 'Anonymous',
  auth_token: str = '',
  use_tls: bool = False,
  tls_root_certs: bytes = None,
  extra_headers: dict[str, str] = None
) -> BarrageSession
```

## Parameters

<ParamTable>
<Param name="host" type="str">

The host name or IP address of the Deephaven server.

</Param>
<Param name="port" type="int" optional>

The port number that the remote Deephaven server is listening on. The default value is `10000`.

</Param>
<Param name="auth_type" type="str" optional>

The authentication type string. Can be "Anonymous", "Basic", or any custom-built authenticator in the server, such as "io.deephaven.authentication.psk.PskAuthenticationHandler". The default is "Anonymous"

</Param>
<Param name="auth_token" type="str" optional>

The authentication token string. When auth_type is Basic, it must be "user:password". When `auth_type` is Anonymous, `auth_token` will be ignored. When `auth_type` is a custom-built authenticator, `auth_token` must conform to the specific requirements of the authenticator.

</Param>
<Param name="use_tls" type="bool" optional>

If `True`, the connection will be encrypted using TLS. The default is `False`.

</Param>
<Param name="tls_root_certs" type="bytes" optional>

The PEM-encoded root certificates to use for TLS connection, or `None` to use system defaults. Any value other than `None` requires a TLS connection, so `use_tls` must also be `True`; otherwise, `barrage_session` raises an error. Defaults to `None`.

</Param>
<Param name="extra_headers" type="dict[str, str]" optional>

Extra headers to set when configuring the gRPC channel. Defaults to `None`.

</Param>
</ParamTable>

## Returns

A [`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession).

## Examples

The following example creates a [`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession) on a Deephaven server running on `localhost` at port `10000`.

```python skip-test
from deephaven.barrage import barrage_session

barrage_sesh = barrage_session("localhost", 10000)
```

The following example connects to a [`BarrageSession`](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.BarrageSession) on a Deephaven server running on `localhost` at port `9999`. This example will fail unless there is already a Deephaven server running on `localhost` at port `9999`.

```python skip-test
from deephaven.barrage import barrage_session

barrage_sesh = barrage_session("localhost", 9999)
```

The following example connects to a Deephaven server running on a remote host at IP address `192.0.2.1` and port `10000`. This example will fail unless there is already a Deephaven server running at the specified IP address and port.

```python skip-test
from deephaven.barrage import barrage_session

barrage_sesh = barrage_session("192.0.2.1", 10000)
```

The following example connects to a Deephaven server running on `localhost` at port `9999`, with pre-shared-key (PSK) authentication. This example will fail unless there is already a Deephaven server running on `localhost` at port `9999`, with matching PSK authentication.

```python skip-test
from deephaven.barrage import barrage_session

barrage_sesh = barrage_session(
    host="localhost",
    port=9999,
    auth_type="io.deephaven.authentication.psk.PskAuthenticationHandler",
    auth_token="PASSWORD",
)
```

## Related documentation

- [What is Barrage?](../../../conceptual/what-is-barrage.md)
- [Capture Python client tables](../../../how-to-guides/capture-tables.md)
- [`subscribe`](./subscribe.md)
- [`snapshot`](./snapshot-barrage.md)
- [`close`](./close.md)
- [Pydoc](/core/pydoc/code/deephaven.barrage.html#deephaven.barrage.barrage_session)
