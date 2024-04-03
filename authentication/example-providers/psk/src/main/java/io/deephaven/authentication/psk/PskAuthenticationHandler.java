//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.authentication.psk;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.configuration.Configuration;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * On startup, check if a PSK was set by config, otherwise generate a new one and log it. Any user with the pre-shared
 * key will be identified as a superuser.
 */
public class PskAuthenticationHandler implements AuthenticationRequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(PskAuthenticationHandler.class);

    private static final String PSK;
    static {
        String pskFromConfig = Configuration.getInstance().getStringWithDefault("authentication.psk", null);
        // If this feature is enabled but no value is given, generate a 64-bit number and encode as
        // base-36 (lower case and numbers).
        PSK = Optional.ofNullable(pskFromConfig).map(String::trim).filter(s -> !s.isEmpty())
                .orElseGet(() -> Long.toString(Math.abs(new Random().nextLong()), 36));

        // limit to ascii for better log and url support
        if (!StandardCharsets.US_ASCII.newEncoder().canEncode(PSK)) {
            throw new IllegalArgumentException("Provided pre-shared key isn't valid ASCII, cannot be used: " + PSK);
        }
    }

    @Override
    public String getAuthType() {
        return getClass().getName();
    }

    @Override
    public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload, HandshakeResponseListener listener)
            throws AuthenticationException {
        // sanity check of size before reading bytes into a string
        if (payload.remaining() == PSK.length()) {
            if (StandardCharsets.US_ASCII.decode(payload).toString().equals(PSK)) {
                return Optional.of(new AuthContext.SuperUser());
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<AuthContext> login(String payload, MetadataResponseListener listener)
            throws AuthenticationException {
        if (payload.equals(PSK)) {
            return Optional.of(new AuthContext.SuperUser());
        }
        return Optional.empty();
    }

    @Override
    public void initialize(String targetUrl) {
        // Noisily log this, so the user can find the link to click easily
        logger.warn().nl().nl().nl().nl().endl();
        logger.warn().append("================================================================================").endl();
        logger.warn().append("Superuser access through pre-shared key is enabled - use ").append(PSK)
                .append(" to connect").endl();
        logger.warn().append("Connect automatically to Web UI with ").append(targetUrl).append("/?psk=")
                .append(PSK)
                .endl();
        logger.warn().append("================================================================================").endl();
        logger.warn().nl().nl().nl().nl().endl();
    }

    /**
     * Provide a list of URLs that the user can visit to authenticate. Adds the `psk` query parameter to the target URL.
     * 
     * @param targetUrl the base url of the hosted UI
     * @return The targetUrl with the PSK query parameter appended
     */
    @Override
    public List<String> urls(String targetUrl) {
        return List.of(targetUrl + "/?psk=" + PSK);
    }
}
