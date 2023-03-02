/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.auth;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.Optional;

/**
 * Handler to accept an empty payload and accept that user as anonymous. To prevent anonymous access, do not enable this
 * authentication handler.
 */
public class AnonymousAuthenticationHandler implements AuthenticationRequestHandler {
    private static final boolean WARN_ANONYMOUS_ACCESS =
            Configuration.getInstance().getBoolean("authentication.anonymous.warn");
    private final static Logger log = LoggerFactory.getLogger(AnonymousAuthenticationHandler.class);

    @Override
    public String getAuthType() {
        return "Anonymous";
    }

    @Override
    public void initialize(String targetUrl) {
        if (WARN_ANONYMOUS_ACCESS) {
            log.warn().endl().endl().endl().endl().endl();
            log.warn().append("================================================================================")
                    .endl();
            log.warn().append("WARNING! Anonymous authentication is enabled. This is not recommended!").endl();
            log.warn().append("       Listening on ").append(targetUrl).endl();
            log.warn().append("================================================================================")
                    .endl();
            log.warn().endl().endl().endl().endl().endl();
        } else {
            log.info().append("Anonymous authentication is enabled. Listening on ").append(targetUrl).endl();
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
        if (payload.isEmpty()) {
            return Optional.of(new AuthContext.Anonymous());
        }
        return Optional.empty();
    }
}
