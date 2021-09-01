/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.util;

import com.google.rpc.Code;
import io.deephaven.grpc_api.browserstreaming.BrowserStream;
import io.deephaven.grpc_api.browserstreaming.BrowserStreamInterceptor;
import io.deephaven.grpc_api.browserstreaming.StreamData;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.io.logger.Logger;
import io.grpc.*;
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

    public static GrpcServiceOverrideBuilder newBuilder(ServerServiceDefinition baseDefinition, String serviceName) {
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

    public static <ReqT, RespT> MethodDescriptor<ReqT, RespT> descriptorFor(
            final MethodDescriptor.MethodType methodType,
            final String serviceName,
            final String methodName,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller,
            final MethodDescriptor<?, ?> descriptor) {

        return MethodDescriptor.<ReqT, RespT>newBuilder()
                .setType(methodType)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName, methodName))
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(responseMarshaller)
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build();
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

        public void invokeOpen(ReqT request, StreamObserver<RespT> responseObserver) {
            GrpcUtil.rpcWrapper(log, responseObserver, () -> {
                StreamData streamData = StreamData.STREAM_DATA_KEY.get();
                SessionState session = sessionService.getCurrentSession();
                if (streamData == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "no x-deephaven-stream headers, cannot handle open request");
                }

                BrowserStream<ReqT> browserStream = factory.create(session, responseObserver);
                browserStream.onMessageReceived(request, streamData);

                if (!streamData.isHalfClose()) {
                    // if this isn't a half-close, we should export it for later calls - if it is, the client won't send
                    // more messages
                    session.newExport(streamData.getRpcTicket(), "rpcTicket")
                            // not setting an onError here, failure can only happen if the session ends
                            .submit(() -> browserStream);
                }

            });
        }

        public void invokeNext(ReqT request, StreamObserver<NextRespT> responseObserver) {
            StreamData streamData = StreamData.STREAM_DATA_KEY.get();
            if (streamData == null || streamData.getRpcTicket() == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "no x-deephaven-stream headers, cannot handle next request");
            }
            GrpcUtil.rpcWrapper(log, responseObserver, () -> {
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
