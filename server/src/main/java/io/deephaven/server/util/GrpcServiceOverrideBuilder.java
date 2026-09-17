//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.util;

import com.google.rpc.Code;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.browserstreaming.BrowserStream;
import io.deephaven.server.browserstreaming.BrowserStreamInterceptor;
import io.deephaven.server.browserstreaming.StreamData;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.io.logger.Logger;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GrpcServiceOverrideBuilder {
    private static class GrpcOverride<ReqT, RespT> {
        private final MethodDescriptor<ReqT, RespT> method;
        private final ServerCallHandler<ReqT, RespT> handler;

        private GrpcOverride(@NotNull MethodDescriptor<ReqT, RespT> method,
                @NotNull ServerCallHandler<ReqT, RespT> handler) {
            this.method = method;
            this.handler = handler;
        }

        private void addMethod(ServerServiceDefinition.Builder builder) {
            builder.addMethod(method, handler);
        }
    }

    private final ServerServiceDefinition baseDefinition;
    private final List<GrpcOverride<?, ?>> overrides = new ArrayList<>();
    private final BrowserStreamInterceptor browserStreamInterceptor = new BrowserStreamInterceptor();
    private boolean needsBrowserInterceptor = false;

    private GrpcServiceOverrideBuilder(ServerServiceDefinition baseDefinition) {
        this.baseDefinition = baseDefinition;
    }

    public static GrpcServiceOverrideBuilder newBuilder(ServerServiceDefinition baseDefinition) {
        return new GrpcServiceOverrideBuilder(baseDefinition);
    }

    private <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method,
            ServerCalls.BidiStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.BIDI_STREAMING);
        overrides.add(new GrpcOverride<>(method, ServerCalls.asyncBidiStreamingCall(handler)));
        return this;
    }

    private <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method,
            ServerCalls.ServerStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.SERVER_STREAMING);
        overrides.add(new GrpcOverride<>(method, ServerCalls.asyncServerStreamingCall(handler)));
        return this;
    }

    private <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method,
            ServerCalls.UnaryMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.UNARY);
        overrides.add(new GrpcOverride<>(method, ServerCalls.asyncUnaryCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder onServerStreamingOverride(
            final Delegate<ReqT, RespT> delegate,
            final MethodDescriptor<?, ?> descriptor,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller) {
        return override(MethodDescriptor.<ReqT, RespT>newBuilder()
                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName(descriptor.getFullMethodName())
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(responseMarshaller)
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build(), new OpenBrowserStreamMethod<>(delegate));
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder onBidiOverride(
            final BidiDelegate<ReqT, RespT> delegate,
            final MethodDescriptor<?, ?> descriptor,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller) {
        return override(MethodDescriptor.<ReqT, RespT>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                .setFullMethodName(descriptor.getFullMethodName())
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(responseMarshaller)
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build(), new BidiStreamMethod<>(delegate));
    }

    public <ReqT, RespT, NextRespT> GrpcServiceOverrideBuilder onBidiOverrideWithBrowserSupport(
            final BidiDelegate<ReqT, RespT> delegate,
            final MethodDescriptor<?, ?> bidiDescriptor,
            final MethodDescriptor<?, ?> openDescriptor,
            final MethodDescriptor<?, ?> nextDescriptor,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller,
            final MethodDescriptor.Marshaller<NextRespT> nextResponseMarshaller,
            BrowserStream.Mode mode,
            Logger log, SessionService sessionService) {
        return this
                .onBidiOverride(
                        delegate,
                        bidiDescriptor,
                        requestMarshaller,
                        responseMarshaller)
                .onBidiBrowserSupport(delegate,
                        openDescriptor,
                        nextDescriptor,
                        requestMarshaller,
                        responseMarshaller,
                        nextResponseMarshaller,
                        mode,
                        log,
                        sessionService);
    }

    public <ReqT, RespT, NextRespT> GrpcServiceOverrideBuilder onBidiBrowserSupport(
            final BidiDelegate<ReqT, RespT> delegate,
            final MethodDescriptor<?, ?> openDescriptor,
            final MethodDescriptor<?, ?> nextDescriptor,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller,
            final MethodDescriptor.Marshaller<NextRespT> nextResponseMarshaller,
            BrowserStream.Mode mode,
            Logger log, SessionService sessionService) {
        BrowserStreamMethod<ReqT, RespT, NextRespT> method =
                new BrowserStreamMethod<>(log, mode, delegate, sessionService);
        needsBrowserInterceptor = true;
        return this
                .override(MethodDescriptor.<ReqT, RespT>newBuilder()
                        .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                        .setFullMethodName(openDescriptor.getFullMethodName())
                        .setSampledToLocalTracing(false)
                        .setRequestMarshaller(requestMarshaller)
                        .setResponseMarshaller(responseMarshaller)
                        .setSchemaDescriptor(openDescriptor.getSchemaDescriptor())
                        .build(), method.open())
                .override(MethodDescriptor.<ReqT, NextRespT>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName(nextDescriptor.getFullMethodName())
                        .setSampledToLocalTracing(false)
                        .setRequestMarshaller(requestMarshaller)
                        .setResponseMarshaller(nextResponseMarshaller)
                        .setSchemaDescriptor(nextDescriptor.getSchemaDescriptor())
                        .build(), method.next());
    }

    public ServerServiceDefinition build() {
        final String service = baseDefinition.getServiceDescriptor().getName();

        final Set<String> overrideMethodNames = overrides.stream()
                .map(o -> o.method.getFullMethodName())
                .collect(Collectors.toSet());

        // Make sure we preserve SchemaDescriptor fields on methods so that gRPC reflection still works.
        final ServiceDescriptor.Builder serviceDescriptorBuilder = ServiceDescriptor.newBuilder(service)
                .setSchemaDescriptor(baseDefinition.getServiceDescriptor().getSchemaDescriptor());

        // define descriptor overrides
        overrides.forEach(o -> serviceDescriptorBuilder.addMethod(o.method));

        // keep non-overridden descriptors
        baseDefinition.getServiceDescriptor().getMethods().stream()
                .filter(d -> !overrideMethodNames.contains(d.getFullMethodName()))
                .forEach(serviceDescriptorBuilder::addMethod);

        final ServiceDescriptor serviceDescriptor = serviceDescriptorBuilder.build();
        ServerServiceDefinition.Builder serviceBuilder = ServerServiceDefinition.builder(serviceDescriptor);

        // add method overrides
        overrides.forEach(dp -> dp.addMethod(serviceBuilder));

        // add non-overridden methods
        baseDefinition.getMethods().stream()
                .filter(d -> !overrideMethodNames.contains(d.getMethodDescriptor().getFullMethodName()))
                .forEach(serviceBuilder::addMethod);

        ServerServiceDefinition serviceDef = serviceBuilder.build();
        if (needsBrowserInterceptor) {
            return ServerInterceptors.intercept(serviceDef, browserStreamInterceptor);
        }
        return serviceDef;
    }

    @FunctionalInterface
    public interface Delegate<ReqT, RespT> {
        void doInvoke(final ReqT request, final StreamObserver<RespT> responseObserver);
    }

    public static final class BrowserStreamMethod<ReqT, RespT, NextRespT> {
        private final BrowserStream.Factory<ReqT, RespT> factory;
        private final SessionService sessionService;
        private final Logger log;

        public BrowserStreamMethod(Logger log, BrowserStream.Mode mode, BidiDelegate<ReqT, RespT> delegate,
                SessionService sessionService) {
            this.log = log;
            this.factory = BrowserStream.factory(mode, delegate);
            this.sessionService = sessionService;
        }

        public ServerCalls.ServerStreamingMethod<ReqT, RespT> open() {
            return this::invokeOpen;
        }

        public ServerCalls.UnaryMethod<ReqT, NextRespT> next() {
            return this::invokeNext;
        }

        public void invokeOpen(
                @NotNull final ReqT request,
                @NotNull final StreamObserver<RespT> responseObserver) {
            StreamData streamData = StreamData.STREAM_DATA_KEY.get();
            SessionState session = sessionService.getCurrentSession();
            if (streamData == null) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "no x-deephaven-stream headers, cannot handle open request");
            }

            final OpenCallObserver<RespT> openCall = responseObserver instanceof ServerCallStreamObserver
                    ? new OpenCallObserver<>((ServerCallStreamObserver<RespT>) responseObserver)
                    : null;
            final BrowserStream<ReqT> browserStream =
                    factory.create(session, openCall != null ? openCall : responseObserver);
            if (openCall != null) {
                openCall.setStreamOnCancel(browserStream::onCancel);
            }
            try {
                browserStream.onMessageReceived(request, streamData);
            } catch (final RuntimeException err) {
                browserStream.onError(err);
                throw err;
            }

            if (!streamData.isHalfClose()) {
                // if this isn't a half-close, we should export it for later calls - if it is, the client won't send
                // more messages
                final SessionState.ExportObject<BrowserStream<ReqT>> export;
                try {
                    export = session.<BrowserStream<ReqT>>newExport(streamData.getRpcTicket(), "rpcTicket")
                            // not setting an onError here, failure can only happen if the session ends
                            .submit(() -> browserStream);
                } catch (final RuntimeException err) {
                    // the session expired, or the ticket is already taken; either way the stream cannot be reached
                    browserStream.onError(err);
                    throw err;
                }
                // the stream releases this export when it completes, fails, or is cancelled
                browserStream.setExport(export);
            }
        }

        /**
         * Wraps the observer of the call that opened an emulated stream, so that a client abort ends the browser stream
         * in addition to running whatever cancel handler the underlying service installs.
         */
        private static final class OpenCallObserver<RespT> extends ServerCallStreamObserver<RespT> {
            private final ServerCallStreamObserver<RespT> delegate;
            /** cancellation is one-shot: a handler registered after it runs at once instead of never */
            private boolean cancelled;
            private Runnable serviceOnCancel;
            private Runnable streamOnCancel;

            private OpenCallObserver(final ServerCallStreamObserver<RespT> delegate) {
                this.delegate = delegate;
                delegate.setOnCancelHandler(this::onCancel);
            }

            private void setStreamOnCancel(final Runnable onCancel) {
                final boolean runNow;
                synchronized (this) {
                    runNow = cancelled;
                    if (!runNow) {
                        streamOnCancel = onCancel;
                    }
                }
                if (runNow) {
                    onCancel.run();
                }
            }

            @Override
            public void setOnCancelHandler(final Runnable onCancelHandler) {
                final boolean runNow;
                synchronized (this) {
                    runNow = cancelled;
                    if (!runNow) {
                        serviceOnCancel = onCancelHandler;
                    }
                }
                if (runNow) {
                    onCancelHandler.run();
                }
            }

            private void onCancel() {
                final Runnable service;
                final Runnable stream;
                synchronized (this) {
                    if (cancelled) {
                        return;
                    }
                    cancelled = true;
                    service = serviceOnCancel;
                    stream = streamOnCancel;
                    serviceOnCancel = null;
                    streamOnCancel = null;
                }
                // the service's handler is arbitrary cleanup code; the stream must end even if that handler throws
                try {
                    if (service != null) {
                        service.run();
                    }
                } finally {
                    if (stream != null) {
                        stream.run();
                    }
                }
            }

            @Override
            public void onNext(final RespT value) {
                delegate.onNext(value);
            }

            @Override
            public void onError(final Throwable t) {
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
            public void setCompression(final String compression) {
                delegate.setCompression(compression);
            }

            @Override
            public boolean isReady() {
                return delegate.isReady();
            }

            @Override
            public void setOnReadyHandler(final Runnable onReadyHandler) {
                delegate.setOnReadyHandler(onReadyHandler);
            }

            @Override
            public void setOnReadyThreshold(final int numBytes) {
                delegate.setOnReadyThreshold(numBytes);
            }

            @Override
            public void setOnCloseHandler(final Runnable onCloseHandler) {
                delegate.setOnCloseHandler(onCloseHandler);
            }

            @Override
            public void disableAutoInboundFlowControl() {
                delegate.disableAutoInboundFlowControl();
            }

            @Override
            public void request(final int count) {
                delegate.request(count);
            }

            @Override
            public void setMessageCompression(final boolean enable) {
                delegate.setMessageCompression(enable);
            }
        }

        public void invokeNext(
                @NotNull final ReqT request,
                @NotNull final StreamObserver<NextRespT> responseObserver) {
            StreamData streamData = StreamData.STREAM_DATA_KEY.get();
            if (streamData == null || streamData.getRpcTicket() == null) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "no x-deephaven-stream headers, cannot handle next request");
            }
            final SessionState session = sessionService.getCurrentSession();

            final SessionState.ExportObject<BrowserStream<ReqT>> browserStream =
                    session.getExport(streamData.getRpcTicket(), "rpcTicket");

            session.nonExport()
                    .require(browserStream)
                    .onError(responseObserver)
                    .submit(() -> {
                        browserStream.get().onMessageReceived(request, streamData);
                        responseObserver.onNext(null);// TODO simple response payload
                        responseObserver.onCompleted();
                    });
        }
    }

    public static class OpenBrowserStreamMethod<ReqT, RespT> implements ServerCalls.ServerStreamingMethod<ReqT, RespT> {

        private final Delegate<ReqT, RespT> delegate;

        public OpenBrowserStreamMethod(final Delegate<ReqT, RespT> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void invoke(final ReqT request, final StreamObserver<RespT> responseObserver) {
            final ServerCallStreamObserver<RespT> serverCall = (ServerCallStreamObserver<RespT>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            delegate.doInvoke(request, responseObserver);
        }
    }

    @FunctionalInterface
    public interface BidiDelegate<ReqT, RespT> {
        StreamObserver<ReqT> doInvoke(final StreamObserver<RespT> responseObserver);
    }

    public static class BidiStreamMethod<ReqT, RespT> implements ServerCalls.BidiStreamingMethod<ReqT, RespT> {
        private final BidiDelegate<ReqT, RespT> delegate;

        public BidiStreamMethod(final BidiDelegate<ReqT, RespT> delegate) {
            this.delegate = delegate;
        }

        @Override
        public StreamObserver<ReqT> invoke(final StreamObserver<RespT> responseObserver) {
            final ServerCallStreamObserver<RespT> serverCall = (ServerCallStreamObserver<RespT>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            return delegate.doInvoke(responseObserver);
        }
    }

    private static void validateMethodType(MethodDescriptor.MethodType methodType,
            MethodDescriptor.MethodType handlerType) {
        if (methodType != handlerType) {
            throw new IllegalArgumentException("Provided method's type (" + methodType.name()
                    + ") does not match handler's type of " + handlerType.name());
        }
    }
}
