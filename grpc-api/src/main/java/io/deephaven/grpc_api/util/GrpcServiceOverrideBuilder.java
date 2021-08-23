/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.util;

import com.google.rpc.Code;
import io.deephaven.flightjs.protocol.BrowserFlight;
import io.deephaven.grpc_api.browserstreaming.BrowserStream;
import io.deephaven.grpc_api.browserstreaming.BrowserStreamInterceptor;
import io.deephaven.grpc_api.browserstreaming.StreamData;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.io.logger.Logger;
import io.grpc.*;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GrpcServiceOverrideBuilder {
    private static class GrpcOverrride<ReqT, RespT> {
        private final MethodDescriptor<ReqT, RespT> method;
        private final ServerCallHandler<ReqT, RespT> handler;

        private GrpcOverrride(@NotNull MethodDescriptor<ReqT, RespT> method, @NotNull ServerCallHandler<ReqT, RespT> handler) {
            this.method = method;
            this.handler = handler;
        }

        private void addMethod(ServerServiceDefinition.Builder builder) {
            builder.addMethod(method, handler);
        }
    }

    private final ServerServiceDefinition baseDefinition;
    private final List<GrpcOverrride<?, ?>> overrides = new ArrayList<>();
    private final String serviceName;

    private GrpcServiceOverrideBuilder(ServerServiceDefinition baseDefinition, String serviceName) {
        this.baseDefinition = baseDefinition;
        this.serviceName = serviceName;
    }

    public static GrpcServiceOverrideBuilder newBuilder(ServerServiceDefinition baseDefinition, String serviceName) {
        return new GrpcServiceOverrideBuilder(baseDefinition, serviceName);
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCalls.BidiStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.BIDI_STREAMING);
        overrides.add(new GrpcOverrride<>(method, ServerCalls.asyncBidiStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCalls.ServerStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.SERVER_STREAMING);
        overrides.add(new GrpcOverrride<>(method, ServerCalls.asyncServerStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCalls.ClientStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.CLIENT_STREAMING);
        overrides.add(new GrpcOverrride<>(method, ServerCalls.asyncClientStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCalls.UnaryMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.UNARY);
        overrides.add(new GrpcOverrride<>(method, ServerCalls.asyncUnaryCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCallHandler<ReqT, RespT> callHandler, ServerInterceptor interceptor) {
        overrides.add(new GrpcOverrride<>(method, InternalServerInterceptors.interceptCallHandler(interceptor, callHandler)));
        return this;
    }

    public <ReqT> GrpcServiceOverrideBuilder onNextOverride(
            final Delegate<ReqT, BrowserFlight.BrowserNextResponse> delegate,
            final String methodName,
            final MethodDescriptor<?, ?> descriptor,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller) {
        return override(MethodDescriptor.<ReqT, BrowserFlight.BrowserNextResponse>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName, methodName))
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(ProtoUtils.marshaller(BrowserFlight.BrowserNextResponse.getDefaultInstance()))
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build(), new NextBrowserStreamMethod<>(delegate));
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder onOpenOverride(
            final Delegate<ReqT, RespT> delegate,
            final String methodName,
            final MethodDescriptor<?, ?> descriptor,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller) {
        return override(MethodDescriptor.<ReqT, RespT>newBuilder()
                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName, methodName))
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(responseMarshaller)
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build(), new OpenBrowserStreamMethod<>(delegate));
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder onBidiOverride(
            final BidiDelegate<ReqT, RespT> delegate,
            final String methodName,
            final MethodDescriptor<?, ?> descriptor,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller) {
        return override(MethodDescriptor.<ReqT, RespT>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName, methodName))
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(responseMarshaller)
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build(), new BidiStreamMethod<>(delegate));
    }

    public <ReqT, RespT, NextRespT> GrpcServiceOverrideBuilder onBidiOverrideWithBrowserSupport(
            final BidiDelegate<ReqT, RespT> delegate,
            final String methodName,
            final MethodDescriptor<ReqT, RespT> bidiDescriptor,
            final MethodDescriptor<ReqT, RespT> openDescriptor,
            final MethodDescriptor<ReqT, NextRespT> nextDescriptor,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller,
            final MethodDescriptor.Marshaller<NextRespT> nextResponseMarshaller,
            BrowserStream.Mode mode,
            Logger log, SessionService sessionService) {
        BrowserStreamMethod<ReqT, RespT, NextRespT> method = new BrowserStreamMethod<>(log, mode, delegate, sessionService);
        return this
                .override(MethodDescriptor.<ReqT, RespT>newBuilder()
                        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                        .setFullMethodName(bidiDescriptor.getFullMethodName())
                        .setSampledToLocalTracing(false)
                        .setRequestMarshaller(requestMarshaller)
                        .setResponseMarshaller(responseMarshaller)
                        .setSchemaDescriptor(bidiDescriptor.getSchemaDescriptor())
                        .build(), new BidiStreamMethod<>(delegate)
                )
                .override(MethodDescriptor.<ReqT, RespT>newBuilder()
                        .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                        .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName, "Open" + methodName))
                        .setSampledToLocalTracing(false)
                        .setRequestMarshaller(requestMarshaller)
                        .setResponseMarshaller(responseMarshaller)
                        .setSchemaDescriptor(openDescriptor.getSchemaDescriptor())
                        .build(), method.open())
                .override(MethodDescriptor.<ReqT, NextRespT>newBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName, "Next" + methodName))
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

        return serviceBuilder.build();
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

        public BrowserStreamMethod(Logger log, BrowserStream.Mode mode, BidiDelegate<ReqT, RespT> delegate, SessionService sessionService) {
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
                StreamData streamData = BrowserStreamInterceptor.STREAM_DATA_KEY.get();
                SessionState session = sessionService.getCurrentSession();
                if (streamData == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no x-deephaven-stream headers, cannot handle open request");
                }

                BrowserStream<ReqT> browserStream = factory.create(session, responseObserver);
                browserStream.onMessageReceived(request, streamData);

                if (!streamData.isHalfClose()) {
                    // if this isn't a half-close, we should export it for later calls - if it is, the client won't send more messages
                    session.newExport(streamData.getRpcTicket())
                            .onError(responseObserver::onError)
                            .submit(() -> browserStream);
                }
            });
        }

        public void invokeNext(ReqT request, StreamObserver<NextRespT> responseObserver) {
            StreamData streamData = BrowserStreamInterceptor.STREAM_DATA_KEY.get();
            GrpcUtil.rpcWrapper(log, responseObserver, () -> {
                final SessionState session = sessionService.getCurrentSession();

                if (streamData == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no x-deephaven-stream headers, cannot handle open request");
                }

                final SessionState.ExportObject<BrowserStream<ReqT>> browserStream =
                        session.getExport(streamData.getRpcTicket());

                session.nonExport()
                        .require(browserStream)
                        .onError(responseObserver::onError)
                        .submit(() -> {
                            browserStream.get().onMessageReceived(request, streamData);
                            responseObserver.onNext(null);//TODO simple response payload
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
            // pass our response stream to the real bidi method and call the resulting
            // request observer with our first payload.

            // if halfclose, complete right away

            // otherwise, stash the request observer for the subsequent "next" call

            final ServerCallStreamObserver<RespT> serverCall = (ServerCallStreamObserver<RespT>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            delegate.doInvoke(request, responseObserver);
        }
    }

    public static class NextBrowserStreamMethod<ReqT, RespT> implements ServerCalls.UnaryMethod<ReqT, RespT> {

        private final Delegate<ReqT, RespT> delegate;

        public NextBrowserStreamMethod(final Delegate<ReqT, RespT> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void invoke(final ReqT request, final StreamObserver<RespT> responseObserver) {
            //
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

    private static void validateMethodType(MethodDescriptor.MethodType methodType, MethodDescriptor.MethodType handlerType) {
        if (methodType != handlerType) {
            throw new IllegalArgumentException("Provided method's type (" + methodType.name() + ") does not match handler's type of " + handlerType.name());
        }
    }
}
