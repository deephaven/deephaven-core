package io.deephaven.auth;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Handler to accept an empty payload and accept that user as anonymous. To prevent anonymous access, do not enable this
 * authentication handler.
 */
public class AnonymousAuthenticationHandler implements AuthenticationRequestHandler {
    private final static Logger log = LoggerFactory.getLogger(AnonymousAuthenticationHandler.class);

    @Override
    public String getAuthType() {
        return "Anonymous";
    }

    @Override
    public void initialize(String targetUrl) {
        // TODO(deephaven-core#2934): Enable anonymous authentication warning
        log.info().append("Anonymous authentication is enabled. Listening on ").append(targetUrl).endl();
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
