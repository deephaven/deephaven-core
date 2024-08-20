//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientCall.Listener;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

import static io.deephaven.client.impl.Authentication.AUTHORIZATION_HEADER;

/**
 * As a {@link ClientInterceptor}, this parser the responses for the bearer token.
 *
 * <p>
 * As a {@link CallCredentials}, this sets the (previously attained) bearer token on requests.
 */
public final class BearerHandler extends CallCredentials implements ClientInterceptor {

    // this is really about "authentication" not "authorization"
    public static final String BEARER_PREFIX = "Bearer ";
    private final AtomicReference<BearerTokenState> state = new AtomicReference<>(new BearerTokenState(null, null));

    private static Optional<String> parseBearerToken(Metadata metadata) {
        final Iterable<String> authenticationValues = metadata.getAll(AUTHORIZATION_HEADER);
        if (authenticationValues == null) {
            return Optional.empty();
        }
        String lastBearerValue = null;
        for (String authenticationValue : authenticationValues) {
            if (authenticationValue.startsWith(BEARER_PREFIX)) {
                lastBearerValue = authenticationValue;
            }
        }
        if (lastBearerValue == null) {
            return Optional.empty();
        }
        return Optional.of(lastBearerValue.substring(BEARER_PREFIX.length()));
    }

    // exposed for flight
    public void setBearerToken(String bearerToken) {
        BearerTokenState current;
        while ((current = state.get()).isRotation(bearerToken)) {
            final BearerTokenState next = current.rotate(bearerToken);
            if (state.compareAndSet(current, next)) {
                break;
            }
        }
    }

    @VisibleForTesting
    public UUID getCurrentToken() {
        return UUID.fromString(state.get().current);
    }

    private void handleMetadata(Metadata metadata) {
        parseBearerToken(metadata).ifPresent(BearerHandler.this::setBearerToken);
    }

    // exposed for flight
    String authenticationValue() {
        return BEARER_PREFIX + state.get().current;
    }

    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        final String bearerToken = state.get().current;
        if (bearerToken == null) {
            applier.fail(Status.UNAUTHENTICATED);
            return;
        }
        final Metadata headers = new Metadata();
        headers.put(AUTHORIZATION_HEADER, BEARER_PREFIX + bearerToken);
        applier.apply(headers);
    }

    @Override
    public void thisUsesUnstableApi() {

    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
            CallOptions callOptions, Channel next) {
        return new BearerCall<>(next.newCall(method, callOptions));
    }

    private final class BearerCall<ReqT, RespT> extends SimpleForwardingClientCall<ReqT, RespT> {
        public BearerCall(ClientCall<ReqT, RespT> delegate) {
            super(delegate);
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            super.start(new BearerListener<>(responseListener), headers);
        }
    }

    private final class BearerListener<RespT> extends SimpleForwardingClientCallListener<RespT> {
        public BearerListener(Listener<RespT> delegate) {
            super(delegate);
        }

        @Override
        public void onHeaders(Metadata headers) {
            try {
                handleMetadata(headers);
            } finally {
                super.onHeaders(headers);
            }
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            try {
                handleMetadata(trailers);
            } finally {
                super.onClose(status, trailers);
            }
        }
    }

    private static class BearerTokenState {
        private final String previous;
        private final String current;

        BearerTokenState(String previous, String current) {
            this.previous = previous;
            this.current = current;
        }

        boolean isRotation(String token) {
            return !Objects.equals(token, current) && !Objects.equals(token, previous);
        }

        BearerTokenState rotate(String newToken) {
            return new BearerTokenState(current, newToken);
        }
    }
}
