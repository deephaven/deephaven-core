//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.auth;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.configuration.Configuration;

import java.nio.ByteBuffer;
import java.util.List;
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
            log.warn().nl().nl().nl().nl().endl();
            log.warn().append("================================================================================")
                    .endl();
            log.warn().append("WARNING! Anonymous authentication is enabled. This is not recommended!").endl();
            log.warn().append("       Listening on ").append(targetUrl).endl();
            log.warn().append("================================================================================")
                    .endl();
            log.warn().nl().nl().nl().nl().endl();
        } else {
            log.info().append("Anonymous authentication is enabled. Listening on ").append(targetUrl).endl();
        }
    }

    /**
     * Return the targetUrl as the only valid url for this handler. With anonymous authentication there's no query
     * parameters to add, so just return the targetUrl.
     *
     * @param targetUrl the base url of the hosted UI
     * @return a list containing the targetUrl
     */
    @Override
    public List<String> urls(String targetUrl) {
        return List.of(targetUrl);
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
