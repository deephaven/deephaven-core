//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * ClientResponseObserver implementation that can map results into a CompletableFuture. To keep generics under control,
 * has two factory methods, {@link #of(Object, BiConsumer, Function)} and {@link #ignoreResponse(Object, BiConsumer)}.
 * 
 * @param <ReqT> The type of the gRPC request
 * @param <RespT> The type of the gRPC response
 * @param <V> The type of the result to make available in the future
 */
public class UnaryGrpcFuture<ReqT, RespT, V> implements ClientResponseObserver<ReqT, RespT> {
    /**
     * Factory method to take a request instance, a method reference to a unary call, and a mapping function to
     * transform the result before supplying it to the future.
     *
     * @param request the gRPC request value
     * @param call the unary gRPC call reference/lambda
     * @param mapping a mapping function to transform the response into a result
     * @param <ReqT> The type of the gRPC request
     * @param <RespT> The type of the gRPC response
     * @param <V> The type of the result to make available in the future
     * @return A CompletableFuture that will succeed, fail, or be canceled. If it succeeds, will contain a result from
     *         the mapping function.
     */
    public static <ReqT, RespT, V> CompletableFuture<V> of(ReqT request, BiConsumer<ReqT, StreamObserver<RespT>> call,
            Function<RespT, V> mapping) {
        UnaryGrpcFuture<ReqT, RespT, V> observer = new UnaryGrpcFuture<>(mapping);
        call.accept(request, observer);
        return observer.future();
    }

    /**
     * Factory method to take a request instance and a method reference to a unary call. Returns a void
     * completablefuture, as many gRPC calls in Deephaven have no meaningful result, except for the fact that they
     * succeeded and server-side state has been modified.
     *
     * @param request the gRPC request value
     * @param call the unary gRPC call reference/lambda
     * @param <ReqT> The type of the gRPC request
     * @param <RespT> The type of the gRPC response
     * @return a CompletableFuture that will succeed, fail, or be canceled, but won't contain a value
     */
    public static <ReqT, RespT> CompletableFuture<Void> ignoreResponse(ReqT request,
            BiConsumer<ReqT, StreamObserver<RespT>> call) {
        return of(request, call, ignore -> null);
    }

    private final CompletableFuture<V> future = new CompletableFuture<>();
    private final Function<RespT, V> mapping;

    private UnaryGrpcFuture(Function<RespT, V> mapping) {
        this.mapping = mapping;
    }

    public CompletableFuture<V> future() {
        return future;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
        future.whenComplete((session, throwable) -> {
            if (future.isCancelled()) {
                requestStream.cancel("User cancelled", null);
            }
        });
    }

    @Override
    public void onNext(RespT value) {
        future.complete(mapping.apply(value));
    }

    @Override
    public void onError(Throwable t) {
        future.completeExceptionally(t);
    }

    @Override
    public void onCompleted() {
        if (!future.isDone()) {
            future.completeExceptionally(
                    new IllegalStateException("Observer completed without response"));
        }
    }
}
