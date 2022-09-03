package io.deephaven.auth;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Simple interface to handle incoming authentication requests from flight/barrage clients, via Handshake or the Flight
 * Authentication header. This is intended to be a low-level way to handle incoming payload bytes.
 */
public interface AuthenticationRequestHandler {

    /**
     * This handler can be referred to via both Arrow Flight's original Auth and Auth2.
     *
     * To use via the original Arrow Flight Handshake, the request should be sent in a
     * {@link io.deephaven.proto.backplane.grpc.WrappedAuthenticationRequest} with this handler's identity string.
     *
     * To use via Arrow Flight Auth 2's metadata header, then the
     * {@link org.apache.arrow.flight.auth2.Auth2Constants#AUTHORIZATION_HEADER} should be prefixed with this handler's
     * identity string.
     *
     * @return the type string used to identify the handler
     */
    String getAuthType();

    /**
     * Given a protocol version (very likely to be zero) and payload bytes, if possible authenticate this user. If the
     * handler can correctly decode the payload and confirm the user's identity, an appropriate UserContext should be
     * returned. If the payload is correctly decoded and definitely isn't a valid user, an exception may be thrown. If
     * there is ambiguity in decoding the payload (leading to apparent "not a valid user") or the payload cannot be
     * decoded, an empty optional should be returned.
     *
     * Note that regular arrow flight clients cannot specify the protocolVersion; to be compatible with flight auth
     * assume protocolVersion will be zero.
     *
     * @param protocolVersion Mostly unused, this is an allowed field to set on HandshakeRequests from the Flight gRPC
     *        call.
     * @param payload The byte payload of the handshake, such as an encoded protobuf.
     * @param listener The handshake response observer, which enables multi-request authentication.
     * @return AuthContext for this user if applicable else Empty
     */
    Optional<AuthContext> login(long protocolVersion, ByteBuffer payload, HandshakeResponseListener listener)
            throws AuthenticationException;

    /**
     * Given a payload string, if possible authenticate this user. If the handler can correctly decode the payload and
     * confirm the user's identity, an appropriate UserContext should be returned. If the payload is correctly decoded
     * and definitely isn't a valid user, an exception may be thrown. If there is ambiguity in decoding the payload
     * (leading to apparent "not a valid user") or the payload cannot be decoded, an empty optional should be returned.
     *
     * Note that metadata can only be sent with the initial gRPC response; multi-message authentication via gRPC
     * metadata headers require multiple gRPC call attempts.
     *
     * @param payload The byte payload of the {@code Authorization} header, such as an encoded protobuf or b64 encoded
     *        string.
     * @param listener The metadata response observer, which enables multi-request authentication.
     * @return AuthContext for this user if applicable else Empty
     */
    Optional<AuthContext> login(String payload, MetadataResponseListener listener)
            throws AuthenticationException;

    /**
     * Initialize request handler with the provided url.
     *
     * @param targetUrl the base url of the hosted UI
     */
    void initialize(String targetUrl);

    interface HandshakeResponseListener {
        void respond(long protocolVersion, ByteBuffer payload);
    }
    interface MetadataResponseListener {
        void addHeader(String key, String string);
    }
}
