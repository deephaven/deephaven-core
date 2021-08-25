/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.util;

import io.deephaven.flightjs.protocol.BrowserFlight;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
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

        private GrpcOverrride(@NotNull MethodDescriptor<ReqT, RespT> method,
            @NotNull ServerCallHandler<ReqT, RespT> handler) {
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

    public static GrpcServiceOverrideBuilder newBuilder(ServerServiceDefinition baseDefinition,
        String serviceName) {
        return new GrpcServiceOverrideBuilder(baseDefinition, serviceName);
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method,
        ServerCalls.BidiStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.BIDI_STREAMING);
        overrides.add(new GrpcOverrride<>(method, ServerCalls.asyncBidiStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method,
        ServerCalls.ServerStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.SERVER_STREAMING);
        overrides.add(new GrpcOverrride<>(method, ServerCalls.asyncServerStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method,
        ServerCalls.ClientStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.CLIENT_STREAMING);
        overrides.add(new GrpcOverrride<>(method, ServerCalls.asyncClientStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method,
        ServerCalls.UnaryMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.UNARY);
        overrides.add(new GrpcOverrride<>(method, ServerCalls.asyncUnaryCall(handler)));
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
            .setResponseMarshaller(
                ProtoUtils.marshaller(BrowserFlight.BrowserNextResponse.getDefaultInstance()))
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

    public ServerServiceDefinition build() {
        final String service = baseDefinition.getServiceDescriptor().getName();

        final Set<String> overrideMethodNames = overrides.stream()
            .map(o -> o.method.getFullMethodName())
            .collect(Collectors.toSet());

        // Make sure we preserve SchemaDescriptor fields on methods so that gRPC reflection still
        // works.
        final ServiceDescriptor.Builder serviceDescriptorBuilder =
            ServiceDescriptor.newBuilder(service)
                .setSchemaDescriptor(baseDefinition.getServiceDescriptor().getSchemaDescriptor());

        // define descriptor overrides
        overrides.forEach(o -> serviceDescriptorBuilder.addMethod(o.method));

        // keep non-overridden descriptors
        baseDefinition.getServiceDescriptor().getMethods().stream()
            .filter(d -> !overrideMethodNames.contains(d.getFullMethodName()))
            .forEach(serviceDescriptorBuilder::addMethod);

        final ServiceDescriptor serviceDescriptor = serviceDescriptorBuilder.build();
        ServerServiceDefinition.Builder serviceBuilder =
            ServerServiceDefinition.builder(serviceDescriptor);

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

    public static class OpenBrowserStreamMethod<ReqT, RespT>
        implements ServerCalls.ServerStreamingMethod<ReqT, RespT> {

        private final Delegate<ReqT, RespT> delegate;

        public OpenBrowserStreamMethod(final Delegate<ReqT, RespT> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void invoke(final ReqT request, final StreamObserver<RespT> responseObserver) {
            final ServerCallStreamObserver<RespT> serverCall =
                (ServerCallStreamObserver<RespT>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            delegate.doInvoke(request, responseObserver);
        }
    }

    public static class NextBrowserStreamMethod<ReqT, RespT>
        implements ServerCalls.UnaryMethod<ReqT, RespT> {

        private final Delegate<ReqT, RespT> delegate;

        public NextBrowserStreamMethod(final Delegate<ReqT, RespT> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void invoke(final ReqT request, final StreamObserver<RespT> responseObserver) {
            delegate.doInvoke(request, responseObserver);
        }
    }

    @FunctionalInterface
    public interface BidiDelegate<ReqT, RespT> {
        StreamObserver<ReqT> doInvoke(final StreamObserver<RespT> responseObserver);
    }

    public static class BidiStreamMethod<ReqT, RespT>
        implements ServerCalls.BidiStreamingMethod<ReqT, RespT> {
        private final BidiDelegate<ReqT, RespT> delegate;

        public BidiStreamMethod(final BidiDelegate<ReqT, RespT> delegate) {
            this.delegate = delegate;
        }

        @Override
        public StreamObserver<ReqT> invoke(final StreamObserver<RespT> responseObserver) {
            final ServerCallStreamObserver<RespT> serverCall =
                (ServerCallStreamObserver<RespT>) responseObserver;
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
