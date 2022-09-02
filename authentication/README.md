Flight's gRPC call for Handshake() takes a HandshakeRequest, which ostensibly provides both a payload in bytes and a
protocol version int64, but the protocol version value is never written by current FlightClient implementations, leaving
servers to only recognize the authentication details by the payload bytes.

For the provided Flight.BasicAuth payload, no indicator is included that the payload _is_ a basic username/password pair
to authenticate with, so Deephaven prefers a specific envelope, both to confirm that the payload is the envelope, and
also to provide a specific type to expect the nested payload to be. This does mean that for typed payloads, there will
be three levels of nesting - the HandshakeRequest will contain a Deephaven TypedAuthenticationPayload, which will then
contain the actual provided details.
