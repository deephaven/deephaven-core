//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse;
import io.grpc.CallCredentials;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class Authentication {

    /**
     * The "authorization" header. This is a misnomer in the specification, this is really an "authentication" header.
     */
    public static final Key<String> AUTHORIZATION_HEADER = Key.of("Authorization", Metadata.ASCII_STRING_MARSHALLER);

    /**
     * Starts an authentication request.
     *
     * @param channel the channel
     * @param authenticationTypeAndValue the authentication type and optional value
     * @return the authentication
     */
    public static Authentication authenticate(DeephavenChannel channel, String authenticationTypeAndValue) {
        final Authentication authentication = new Authentication(channel, authenticationTypeAndValue);
        authentication.start();
        return authentication;
    }

    private final DeephavenChannel channel;
    private final String authenticationTypeAndValue;
    private final BearerHandler bearerHandler = new BearerHandler();
    private final CountDownLatch done = new CountDownLatch(1);
    private final CompletableFuture<Authentication> future = new CompletableFuture<>();
    private ClientCallStreamObserver<?> requestStream;
    private ConfigurationConstantsResponse response;
    private Throwable error;

    private Authentication(DeephavenChannel channel, String authenticationTypeAndValue) {
        this.channel = Objects.requireNonNull(channel);
        this.authenticationTypeAndValue = Objects.requireNonNull(authenticationTypeAndValue);
    }

    /**
     * Causes the current thread to wait until the authentication request is finished, unless the thread is interrupted.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void await() throws InterruptedException {
        done.await();
    }

    /**
     * Causes the current thread to wait for up to {@code duration} until the authentication request is finished, unless
     * the thread is interrupted, or the specified waiting time elapses.
     *
     * @param duration the duration to wait
     * @return true if the authentication request is done and false if the waiting time elapsed before the request is
     *         done
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public boolean await(Duration duration) throws InterruptedException {
        return done.await(duration.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Waits for the request to finish. On interrupted, will cancel the request, and re-throw the interrupted exception.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public void awaitOrCancel() throws InterruptedException {
        try {
            done.await();
        } catch (InterruptedException e) {
            cancel("Thread interrupted", e);
            throw e;
        }
    }

    /**
     * Causes the current thread to wait for up to {@code duration} until the authentication request is finished, unless
     * the thread is interrupted, or the specified waiting time elapses. On interrupted, will cancel the request, and
     * re-throw the interrupted exception. On timed-out, will cancel the request.
     *
     * @param duration the duration to wait
     * @return true if the authentication request is done and false if the waiting time elapsed before the request is
     *         done
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public boolean awaitOrCancel(Duration duration) throws InterruptedException {
        final boolean finished;
        try {
            finished = done.await(duration.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            cancel("Thread interrupted", e);
            throw e;
        }
        if (!finished) {
            cancel("Timed out", null);
        }
        return finished;
    }

    /**
     * The future. Is always completed successfully with {@code this} when done. The caller is still responsible for
     * checking {@link #error()} as necessary. Presented as an alternative to the await methods.
     *
     * @return the future
     */
    public CompletableFuture<Authentication> future() {
        return future.whenComplete((r, t) -> {
            if (future.isCancelled()) {
                requestStream.cancel("User cancelled", null);
            }
        });
    }

    /**
     * Cancels the request.
     *
     * @param message the message
     * @param cause the cause
     */
    public void cancel(String message, Throwable cause) {
        requestStream.cancel(message, cause);
    }

    /**
     * Upon success, will return a channel that handles setting the Bearer token when messages are sent, and handles
     * updating the Bearer token when messages are received. The request must already be finished.
     *
     * <p>
     * Note: the caller is responsible for ensuring at least some messages are sent as appropriate during the token
     * timeout schedule. See {@link #configurationConstants()}.
     */
    public Optional<DeephavenChannel> bearerChannel() {
        if (done.getCount() != 0) {
            throw new IllegalStateException("Must await response");
        }
        if (response == null) {
            return Optional.empty();
        }
        return Optional.of(credsAndInterceptor(channel, bearerHandler, bearerHandler));
    }

    /**
     * The configuration constants. The request must already be finished.
     *
     * @return the configuration constants
     */
    public Optional<ConfigurationConstantsResponse> configurationConstants() {
        if (done.getCount() != 0) {
            throw new IllegalStateException("Must await response");
        }
        return Optional.ofNullable(response);
    }

    /**
     * The error. The request must already be finished.
     *
     * @return the error
     */
    public Optional<Throwable> error() {
        if (done.getCount() != 0) {
            throw new IllegalStateException("Must await response");
        }
        return Optional.ofNullable(error);
    }

    /**
     * Throws if an error has been returned. The request must already be finished.
     *
     * @throws RuntimeException if an error has been returned
     */
    public void throwOnError() {
        if (done.getCount() != 0) {
            throw new IllegalStateException("Must await response");
        }
        if (error != null) {
            throw toRuntimeException(error);
        }
    }

    BearerHandler bearerHandler() {
        return bearerHandler;
    }

    private void start() {
        final DeephavenChannel initialChannel = credsAndInterceptor(channel,
                new AuthenticationCallCredentials(authenticationTypeAndValue), bearerHandler);
        initialChannel.config().getConfigurationConstants(ConfigurationConstantsRequest.getDefaultInstance(),
                new Observer());
    }

    private class Observer
            implements ClientResponseObserver<ConfigurationConstantsRequest, ConfigurationConstantsResponse> {

        @Override
        public void beforeStart(ClientCallStreamObserver<ConfigurationConstantsRequest> stream) {
            requestStream = stream;
        }

        @Override
        public void onNext(ConfigurationConstantsResponse response) {
            Authentication.this.response = response;
        }

        @Override
        public void onError(Throwable t) {
            error = t;
            done.countDown();
            future.complete(Authentication.this);
        }

        @Override
        public void onCompleted() {
            if (Authentication.this.response == null) {
                error = new IllegalStateException("Completed without response");
            }
            done.countDown();
            future.complete(Authentication.this);
        }
    }

    private static DeephavenChannel credsAndInterceptor(DeephavenChannel channel, CallCredentials callCredentials,
            ClientInterceptor clientInterceptor) {
        return DeephavenChannel.withClientInterceptors(DeephavenChannel.withCallCredentials(channel, callCredentials),
                clientInterceptor);
    }

    // Similar to io.grpc.stub.ClientCalls.toStatusRuntimeException
    private static RuntimeException toRuntimeException(Throwable t) {
        Throwable cause = Objects.requireNonNull(t);
        while (cause != null) {
            // If we have an embedded status, use it and replace the cause
            if (cause instanceof StatusException) {
                StatusException se = (StatusException) cause;
                return new StatusRuntimeException(se.getStatus(), se.getTrailers());
            } else if (cause instanceof StatusRuntimeException) {
                StatusRuntimeException se = (StatusRuntimeException) cause;
                return new StatusRuntimeException(se.getStatus(), se.getTrailers());
            } else if (cause instanceof RuntimeException) {
                return (RuntimeException) cause;
            }
            cause = cause.getCause();
        }
        return Status.UNKNOWN.withDescription("unexpected exception").withCause(t)
                .asRuntimeException();
    }
}
