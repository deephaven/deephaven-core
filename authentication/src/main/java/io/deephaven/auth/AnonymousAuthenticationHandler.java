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
        for (int ii = 0; ii < 5; ++ii) {
            log.warn().endl();
        }
        log.warn().append("================================================================================").endl();
        log.warn().append("WARNING! Anonymous authentication is enabled. This is not recommended!").endl();
        log.warn().append("       Listening on ").append(targetUrl).endl();
        log.warn().append("================================================================================").endl();
        for (int ii = 0; ii < 5; ++ii) {
            log.warn().endl();
        }
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
