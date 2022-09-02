package io.deephaven.auth;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Handler to accept an empty payload and accept that user as anonymous. To prevent anonymous access, do not enable this
 * authentication handler.
 */
public class AnonymousAuthenticationHandler implements AuthenticationRequestHandler {
    @Override
    public String getAuthType() {
        return "Anonymous";
    }

    @Override
    public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload, HandshakeResponseListener listener) {
        if (!payload.hasRemaining()) {
            return Optional.of(new AuthContext.Anonymous());
        }
        return Optional.empty();
    }

    @Override
    public Optional<AuthContext> login(String payload, MetadataResponseListener listener) {
        if (payload.length() == 0) {
            return Optional.of(new AuthContext.Anonymous());
        }
        return Optional.empty();
    }
}
