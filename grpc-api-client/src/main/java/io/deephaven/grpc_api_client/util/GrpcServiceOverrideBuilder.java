/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.util;

import io.grpc.MethodDescriptor;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.stub.ServerCalls;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class GrpcServiceOverrideBuilder {
    private static class Override<ReqT, RespT> {
        private final MethodDescriptor<ReqT, RespT> method;
        private final ServerCallHandler<ReqT, RespT> handler;

        private Override(@NotNull MethodDescriptor<ReqT, RespT> method, @NotNull ServerCallHandler<ReqT, RespT> handler) {
            this.method = method;
            this.handler = handler;
        }

        private void addMethod(ServerServiceDefinition.Builder builder) {
            builder.addMethod(method, handler);
        }
    }

    private final ServerServiceDefinition baseDefinition;
    private final List<Override<?, ?>> overrides = new ArrayList<>();

    private GrpcServiceOverrideBuilder(ServerServiceDefinition baseDefinition) {
        this.baseDefinition = baseDefinition;
    }

    public static GrpcServiceOverrideBuilder newBuilder(ServerServiceDefinition baseDefinition) {
        return new GrpcServiceOverrideBuilder(baseDefinition);
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCalls.BidiStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.BIDI_STREAMING);
        overrides.add(new GrpcServiceOverrideBuilder.Override<>(method, ServerCalls.asyncBidiStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCalls.ServerStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.SERVER_STREAMING);
        overrides.add(new GrpcServiceOverrideBuilder.Override<>(method, ServerCalls.asyncServerStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCalls.ClientStreamingMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.CLIENT_STREAMING);
        overrides.add(new GrpcServiceOverrideBuilder.Override<>(method, ServerCalls.asyncClientStreamingCall(handler)));
        return this;
    }

    public <ReqT, RespT> GrpcServiceOverrideBuilder override(MethodDescriptor<ReqT, RespT> method, ServerCalls.UnaryMethod<ReqT, RespT> handler) {
        validateMethodType(method.getType(), MethodDescriptor.MethodType.UNARY);
        overrides.add(new GrpcServiceOverrideBuilder.Override<>(method, ServerCalls.asyncUnaryCall(handler)));
        return this;
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
            final String fullMethodName,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller,
            final MethodDescriptor<?, ?> descriptor) {

        return MethodDescriptor.<ReqT, RespT>newBuilder()
                .setType(methodType)
                .setFullMethodName(fullMethodName)
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(responseMarshaller)
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build();
    }

    private static void validateMethodType(MethodDescriptor.MethodType methodType, MethodDescriptor.MethodType handlerType) {
        if (methodType != handlerType) {
            throw new IllegalArgumentException("Provided method's type (" + methodType.name() + ") does not match handler's type of " + handlerType.name());
        }
    }
}
