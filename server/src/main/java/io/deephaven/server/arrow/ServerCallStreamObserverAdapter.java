//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.grpc.stub.ServerCallStreamObserver;

import java.util.Objects;
import java.util.function.Function;

final class ServerCallStreamObserverAdapter<T, R> extends ServerCallStreamObserver<T> {

    private final ServerCallStreamObserver<R> delegate;
    private final Function<T, R> f;

    ServerCallStreamObserverAdapter(ServerCallStreamObserver<R> delegate, Function<T, R> f) {
        this.delegate = Objects.requireNonNull(delegate);
        this.f = Objects.requireNonNull(f);
    }

    @Override
    public void onNext(T value) {
        delegate.onNext(f.apply(value));
    }

    @Override
    public void onError(Throwable t) {
        delegate.onError(t);
    }

    @Override
    public void onCompleted() {
        delegate.onCompleted();
    }

    @Override
    public boolean isCancelled() {
        return delegate.isCancelled();
    }

    @Override
    public void setOnCancelHandler(Runnable onCancelHandler) {
        delegate.setOnCancelHandler(onCancelHandler);
    }

    @Override
    public void setCompression(String compression) {
        delegate.setCompression(compression);
    }

    @Override
    public boolean isReady() {
        return delegate.isReady();
    }

    @Override
    public void setOnReadyHandler(Runnable onReadyHandler) {
        delegate.setOnReadyHandler(onReadyHandler);
    }

    @Override
    public void request(int count) {
        delegate.request(count);
    }

    @Override
    public void setMessageCompression(boolean enable) {
        delegate.setMessageCompression(enable);
    }

    @Override
    public void disableAutoInboundFlowControl() {
        delegate.disableAutoInboundFlowControl();
    }
}
