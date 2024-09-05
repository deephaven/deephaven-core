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
 * This exists to work around how the FlightSQL JDBC driver works out-of-the-box.
 */
public final class AuthCookie {

    private static final String HEADER = "x-deephaven-auth-cookie-request";

    private static final String DEEPHAVEN_AUTH_COOKIE = "deephaven-auth-cookie";

    private static final Metadata.Key<String> REQUEST_AUTH_COOKIE_HEADER_KEY =
            Metadata.Key.of(HEADER, Metadata.ASCII_STRING_MARSHALLER);

    private static final Context.Key<Object> REQUEST_AUTH_COOKIE_CONTEXT_KEY =
            Context.key(AuthCookie.class.getSimpleName());

    private static final Metadata.Key<String> SET_COOKIE =
            Metadata.Key.of("Set-Cookie", Metadata.ASCII_STRING_MARSHALLER);

    private static final Metadata.Key<String> COOKIE =
            Metadata.Key.of("cookie", Metadata.ASCII_STRING_MARSHALLER);

    private static final Object SENTINEL = new Object();

    /**
     * A server interceptor that parses the header {@value HEADER}; when "true", the auth cookie will be set as part of
     * {@link #setAuthCookieIfRequested(Context, Metadata, UUID)}.
     *
     * @return the server interceptor
     */
    public static ServerInterceptor interceptor() {
        return Interceptor.REQUEST_AUTH_COOKIE_INTERCEPTOR;
    }

    /**
     * Sets the auth cookie {@value DEEPHAVEN_AUTH_COOKIE} to {@code token} if the auth cookie was requested. See
     * {@link #interceptor()}.
     */
    public static void setAuthCookieIfRequested(Context context, Metadata md, UUID token) {
        if (REQUEST_AUTH_COOKIE_CONTEXT_KEY.get(context) != SENTINEL) {
            return;
        }
        md.put(SET_COOKIE, AuthCookie.DEEPHAVEN_AUTH_COOKIE + "=" + token.toString());
    }

    static Optional<UUID> parseAuthCookie(Metadata md) {
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

    private enum Interceptor implements ServerInterceptor {
        REQUEST_AUTH_COOKIE_INTERCEPTOR;

        @Override
        public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                ServerCallHandler<ReqT, RespT> next) {
            if (!Boolean.parseBoolean(headers.get(REQUEST_AUTH_COOKIE_HEADER_KEY))) {
                return next.startCall(call, headers);
            }
            final Context newContext = Context.current().withValue(REQUEST_AUTH_COOKIE_CONTEXT_KEY, SENTINEL);
            return Contexts.interceptCall(newContext, call, headers, next);
        }
    }
}
