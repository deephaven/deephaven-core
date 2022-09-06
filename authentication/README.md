Deephaven's Approach to Authentication
======================================

Deephaven integrates with both the original and the re-envisioned versions of Flight Auth.

Configuration
-------------

Add `-DAuthHandlers=$CLASS_1,$CLASS_2,etc...` to your jvm command line to enable authentication for each supplied
class.

Flight Basic Auth
-----------------

Implement `io.deephaven.auth.BasicAuthMarshaller.Handler` and add to the list of enabled authenticators.
Deephaven will check both Auth1 and Auth2 payloads to see if the request is via Flight's `BasicAuth` before moving
to other authenticators.

More Complex Auth
-----------------

Implement `io.deephaven.auth.AuthenticationRequestHandler` and add to the list of enabled authenticators. User are
not required to implement both `login` methods unless they want to support clients using both Auth1 and Auth2. The
interface includes a `String getAuthType()` which is used to route requests from clients to the proper
authentication authority.

Flight Auth1 `FlightService#Handshake`
--------------------------------------
Flight's gRPC call for `Handshake()` takes a `HandshakeRequest`, which provides both a payload in bytes and a
protocol version int64. The protocol version value is never written by current FlightClient implementations, leaving
servers to only recognize the authentication details by the payload bytes.

As a result, our implementation tries to ignore protocol version. We felt that the API did not provided sufficient
means for supporting multiple forms of authentication simulatenously. Aside from the `BasicAuth` support, we expect
the `Handshake` payload to be a `WrappedAuthenticationRequest`. This is a tuple of `type` and `payload`. The `type`
is used to route the request to the instance of `io.deephaven.auth.AuthenticationRequestHandler` where `getAuthType`
is an exact match.


Flight Auth2 `Authentication` Header
------------------------------------
Flight Auth was reenvisioned for the client to send an `Authentication` header to identify themselves. The header
contains a prefix such as `Basic` or `Bearer` followed by a text payload. This prefix is used to route the request
to the instance of `io.deephaven.auth.AuthenticationRequestHandler` where `getAuthType` is an exact match.

Session Bearer Token
--------------------

Deephaven's server builds a Session around each authentication. An identification token is provided to the client
in the form of a bearer token. This token needs to be supplied with all subsequent gRPC requests to match RPCs to
a session and therefore an authorization context. As clients tend to be emphemeral, the server requires that the
client rotates this bearer token and in return will ensure oustanding state continues to be available to that client.
