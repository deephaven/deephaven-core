/**
 * Tools to help adapt bidirectional streams for use in browsers. Using the improbably-eng implementation of grpc-web,
 * we can use websockets for true bidirectional streams. Websockets are an imperfect solution, as this implementation
 * creates one per stream, which could exhaust the maximum allowed websocket count in a browser. Additionally, for
 * server-streaming operations, while fetch can be used with http/1.1 to get chunked content, this seems to fail more
 * easily than websockets in non-ssl environments, and this grpc-web client implementation can't seem to use fetch for
 * unary calls only without creating multiple client instances.
 *
 * On the other hand, when ssl is supported, browsers are able to use fetch and h2 to make connections, which today only
 * supports server-streamed content, but in the future might support all streams. These are all tunneled in a single tcp
 * socket, and there is no limitation on the number of connections that can be made, but without bidirectional streams,
 * we need to send calls to the server out-of-band in a one-off unary stream. That out-of-band "next" operation must
 * take a simple success/failure response to indicate that the server accepted the message, actual responses should be
 * sent on the server-stream.
 *
 * As such, we presently use a new websocket for all calls from non-ssl connections, and fetch/h2 for all ssl
 * connections. Certain unary calls will be used to send client messages, and these will be ordered with a
 * "x-deephaven-stream-sequence" header, and closed with empty messages with a "x-deephaven-stream-halfclose" header.
 */
package io.deephaven.grpc_api.browserstreaming;

