//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.grpc;

import io.deephaven.client.impl.UnaryGrpcFuture;
import io.deephaven.util.annotations.InternalUseOnly;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Utility methods that improve usability of {@link ClientCalls}.
 */
@InternalUseOnly
public final class Calls {

    public static void checkOptions(CallOptions options) {
        if (options.getDeadline() == null) {
            throw new IllegalArgumentException("Must specify deadline");
        }
        // we _could_ do this by default...
        if (!options.isWaitForReady()) {
            throw new IllegalArgumentException("When deadline is set, must set wait for ready");
        }
        // if (options.isWaitForReady() && options.getDeadline() == null) {
        // throw new IllegalArgumentException("todo");
        // }
        // if (options.getDeadline() != null && !options.isWaitForReady()) {
        // throw new IllegalArgumentException("todo");
        // }
    }

    /**
     * Executes a blocking unary call. Callers are strongly encouraged to set a {@link CallOptions#getDeadline()
     * deadline}.
     *
     * <p>
     * This is a helper over {@link ClientCalls#blockingV2UnaryCall(Channel, MethodDescriptor, CallOptions, Object)}
     * that elevates {@link Thread#interrupted()} into {@link InterruptedException} and
     * {@link Status.Code#DEADLINE_EXCEEDED} into {@link TimeoutException}.
     *
     * @return the single response message
     */
    public static <ReqT, RespT> RespT blockingUnaryCall(
            final Channel channel,
            final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions,
            final ReqT request) throws StatusException, InterruptedException, TimeoutException {
        checkOptions(callOptions);
        try {
            return ClientCalls.blockingV2UnaryCall(channel, method, callOptions, request);
        } catch (final StatusException e) {
            extractInterrupted(e);
            extractTimeout(e);
            throw e;
        }
    }

    public static boolean isTimeout(StatusException e) {
        return e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED;
    }

    public static void extractTimeout(final StatusException e) throws TimeoutException {
        if (isTimeout(e)) {
            final TimeoutException te = new TimeoutException(e.getMessage());
            te.initCause(e);
            throw te;
        }
    }

    private static boolean isInterruptedImpl(Status status, boolean clearIfSet) {
        // Note: it's possible that the current thread _is_ interrupted, but some other StatusException was thrown.
        // In that case, it's better to throw the explicit StatusException since the caller can always find out they
        // were interrupted by calling Thread.interrupted().
        return status.getCode() == Status.Code.CANCELLED
                // && "Thread interrupted".equals(status.getDescription())
                && status.getCause() instanceof InterruptedException
                && clearIfSet ? Thread.interrupted() : Thread.currentThread().isInterrupted();
    }

    public static boolean isInterrupted(StatusException e) {
        return isInterruptedImpl(e.getStatus(), false);
    }

    public static void extractInterrupted(final StatusException e) throws InterruptedException {
        if (isInterruptedImpl(e.getStatus(), true)) {
            throw new InterruptedException();
            // final InterruptedException ie = new InterruptedException(e.getMessage());
            // ie.initCause(e);
            // throw ie;
        }
    }

    public static StatusRuntimeException asStatusRuntime(StatusException e) {
        return e.getStatus().asRuntimeException(e.getTrailers());
    }

    /**
     * Executes a unary call and returns a {@link CompletableFuture} to the response. Callers are strongly encouraged to
     * set a {@link CallOptions#getDeadline() deadline}.
     *
     * <p>
     * Semantically, this is similar to {@link ClientCalls#futureUnaryCall(ClientCall, Object)}, but uses a
     * {@link Channel}, {@link MethodDescriptor}, and {@link CallOptions} instead of {@link ClientCall}; and returns a
     * {@link CompletableFuture}.
     *
     * @return a future for the single response message
     */
    public static <ReqT, RespT> CompletableFuture<RespT> completableFutureUnaryCall(
            final Channel channel,
            final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions,
            final ReqT request) {
        final UnaryGrpcFuture<ReqT, RespT, RespT> observer = new UnaryGrpcFuture<>(Function.identity());
        asyncUnaryCall(channel, method, callOptions, request, observer);
        return observer.future();
    }

    /**
     * Executes a unary call with a response {@link StreamObserver}. Callers are strongly encouraged to set a
     * {@link CallOptions#getDeadline() deadline}.
     *
     * <p>
     * If the provided {@code responseObserver} is an instance of {@link ClientResponseObserver}, {@code beforeStart()}
     * will be called.
     *
     * <p>
     * This is a helper over {@link ClientCalls#asyncUnaryCall(ClientCall, Object, StreamObserver)} that uses a
     * {@link Channel}, {@link MethodDescriptor}, and {@link CallOptions} instead of a {@link ClientCall}.
     *
     * <p>
     * Equivalent to
     * {@code ClientCalls.asyncUnaryCall(channel.newCall(method, callOptions), request, responseObserver)}.
     */
    public static <ReqT, RespT> void asyncUnaryCall(
            final Channel channel,
            final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions,
            final ReqT request,
            final StreamObserver<RespT> responseObserver) {
        checkOptions(callOptions);
        ClientCalls.asyncUnaryCall(channel.newCall(method, callOptions), request, responseObserver);
    }

    /**
     * Executes a server-streaming call with a response {@link StreamObserver}.
     *
     * <p>
     * If the provided {@code responseObserver} is an instance of {@link ClientResponseObserver}, {@code beforeStart()}
     * will be called.
     *
     * <p>
     * This is a helper over {@link ClientCalls#asyncServerStreamingCall(ClientCall, Object, StreamObserver)} that uses
     * a {@link Channel}, {@link MethodDescriptor}, and {@link CallOptions} instead of a {@link ClientCall}.
     *
     * <p>
     * Equivalent to
     * {@code ClientCalls.asyncServerStreamingCall(channel.newCall(method, callOptions), request, responseObserver)}.
     */
    public static <ReqT, RespT> void asyncServerStreamingCall(
            final Channel channel,
            final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions,
            final ReqT request,
            StreamObserver<RespT> responseObserver) {
        ClientCalls.asyncServerStreamingCall(channel.newCall(method, callOptions), request, responseObserver);
    }

    /**
     * Executes a bidirectional-streaming call.
     *
     * <p>
     * If the provided {@code responseObserver} is an instance of {@link ClientResponseObserver}, {@code beforeStart()}
     * will be called.
     *
     * <p>
     * This is a helper over {@link ClientCalls#asyncBidiStreamingCall(ClientCall, StreamObserver)} that uses a
     * {@link Channel}, {@link MethodDescriptor}, and {@link CallOptions} instead of a {@link ClientCall}; and is
     * explicit in returning a {@link ClientCallStreamObserver}.
     *
     * <p>
     * Equivalent to
     * {@code (ClientCallStreamObserver<ReqT>) ClientCalls.asyncBidiStreamingCall(channel.newCall(method, callOptions), responseObserver)}.
     *
     * @return request stream observer
     */
    public static <ReqT, RespT> ClientCallStreamObserver<ReqT> asyncBidiStreamingCall(
            final Channel channel,
            final MethodDescriptor<ReqT, RespT> method,
            final CallOptions callOptions,
            StreamObserver<RespT> responseObserver) {
        return (ClientCallStreamObserver<ReqT>) ClientCalls.asyncBidiStreamingCall(channel.newCall(method, callOptions),
                responseObserver);
    }

    private Calls() {}
}
