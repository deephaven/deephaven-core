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
    private volatile String bearerToken;

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
        String localBearerToken = this.bearerToken;
        // Only follow through with the volatile write if it's a different value.
        if (!Objects.equals(localBearerToken, bearerToken)) {
            this.bearerToken = Objects.requireNonNull(bearerToken);
        }
    }

    @VisibleForTesting
    public UUID getCurrentToken() {
        return UUID.fromString(bearerToken);
    }

    private void handleMetadata(Metadata metadata) {
        parseBearerToken(metadata).ifPresent(BearerHandler.this::setBearerToken);
    }

    // exposed for flight
    String authenticationValue() {
        return BEARER_PREFIX + bearerToken;
    }

    @Override
    public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor, MetadataApplier applier) {
        final String bearerToken = this.bearerToken;
        if (bearerToken == null) {
            applier.fail(Status.UNAUTHENTICATED);
            return;
        }
        final Metadata headers = new Metadata();
        headers.put(AUTHORIZATION_HEADER, authenticationValue());
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
}
