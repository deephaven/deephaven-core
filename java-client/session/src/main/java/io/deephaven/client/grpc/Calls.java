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
        try {
            return ClientCalls.blockingV2UnaryCall(channel, method, callOptions, request);
        } catch (final StatusException e) {
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                throw new TimeoutException(e.getMessage());
            }
            throw e;
        }
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
