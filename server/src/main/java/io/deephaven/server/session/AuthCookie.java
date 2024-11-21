//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.github.f4b6a3.uuid.UuidCreator;
import com.github.f4b6a3.uuid.exception.InvalidUuidException;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.Optional;
import java.util.UUID;

/**
 * This exists to work around how the Flight SQL JDBC driver works out-of-the-box.
 */
final class AuthCookie {

    private static final String HEADER = "x-deephaven-auth-cookie-request";

    private static final String DEEPHAVEN_AUTH_COOKIE = "deephaven-auth-cookie";

    private static final Metadata.Key<String> REQUEST_AUTH_COOKIE_HEADER_KEY =
            Metadata.Key.of(HEADER, Metadata.ASCII_STRING_MARSHALLER);

    private static final Metadata.Key<String> SET_COOKIE =
            Metadata.Key.of("set-cookie", Metadata.ASCII_STRING_MARSHALLER);

    private static final Metadata.Key<String> COOKIE =
            Metadata.Key.of("cookie", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Returns {@code true} if the metadata contains the header {@value HEADER} with value "true".
     */
    public static boolean hasDeephavenAuthCookieRequest(Metadata md) {
        return Boolean.parseBoolean(md.get(REQUEST_AUTH_COOKIE_HEADER_KEY));
    }

    /**
     * Sets the auth cookie {@value DEEPHAVEN_AUTH_COOKIE} to {@code token}.
     */
    public static void setDeephavenAuthCookie(Metadata md, UUID token) {
        md.put(SET_COOKIE, DEEPHAVEN_AUTH_COOKIE + "=" + token.toString());
    }

    /**
     * Parses the "cookie" header for the Deephaven auth cookie if it is of the form "deephaven-auth-cookie=<UUID>".
     */
    public static Optional<UUID> parseAuthCookie(Metadata md) {
        final String cookie = md.get(COOKIE);
        if (cookie == null) {
            return Optional.empty();
        }
        // DH will only ever set one cookie of the form "deephaven-auth-cookie=<UUID>"; anything that doesn't match this
        // is invalid.
        final String[] split = cookie.split("=");
        if (split.length != 2 || !DEEPHAVEN_AUTH_COOKIE.equals(split[0])) {
            return Optional.empty();
        }
        final UUID uuid;
        try {
            uuid = UuidCreator.fromString(split[1]);
        } catch (InvalidUuidException e) {
            return Optional.empty();
        }
        return Optional.of(uuid);
    }
}
