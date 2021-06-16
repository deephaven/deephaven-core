/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.barrage;

import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.grpc_api.util.PassthroughInputStreamMarshaller;
import io.deephaven.grpc_api_client.util.GrpcServiceOverrideBuilder;
import io.deephaven.proto.backplane.grpc.BarrageServiceGrpc;
import io.deephaven.proto.backplane.grpc.SubscriptionRequest;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;

@Singleton
public class BarrageServiceGrpcBinding<Options, View> implements BindableService {

    private static final String SERVICE = BarrageServiceGrpc.SERVICE_NAME;

    private static final String DO_SUBSCRIBE = MethodDescriptor.generateFullMethodName(SERVICE, "DoSubscribe");
    private static final String DO_SUBSCRIBE_NO_CLIENT_STREAM = MethodDescriptor.generateFullMethodName(SERVICE, "DoSubscribeNoClientStream");

    private final BarrageServiceGrpcImpl<Options, View> delegate;

    @Inject
    public BarrageServiceGrpcBinding(final BarrageServiceGrpcImpl<Options, View> service) {
        this.delegate = service;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService())
                .override(GrpcServiceOverrideBuilder.descriptorFor(
                        MethodDescriptor.MethodType.BIDI_STREAMING, DO_SUBSCRIBE,
                        ProtoUtils.marshaller(SubscriptionRequest.getDefaultInstance()),
                        PassthroughInputStreamMarshaller.INSTANCE,
                        BarrageServiceGrpc.getDoSubscribeMethod()), new DoSubscribe<>(delegate))
                .override(GrpcServiceOverrideBuilder.descriptorFor(
                        MethodDescriptor.MethodType.SERVER_STREAMING, DO_SUBSCRIBE_NO_CLIENT_STREAM,
                        ProtoUtils.marshaller(SubscriptionRequest.getDefaultInstance()),
                        PassthroughInputStreamMarshaller.INSTANCE,
                        BarrageServiceGrpc.getDoSubscribeNoClientStreamMethod()), new DoSubscribeNoClientStream<>(delegate))
                .build();
    }

    /**
     * Fetch the client side descriptor for a specific table schema.
     *
     * @param options           the set of options that last across the entire life of the subscription
     * @param columnChunkTypes  the chunk types per column
     * @param columnTypes       the class type per column
     * @param componentTypes    the component class type per column
     * @param streamReader      the stream reader - intended to be thread safe and re-usable
     * @param <Options>         the options related to deserialization
     * @return the client side method descriptor
     */
    public static <Options> MethodDescriptor<SubscriptionRequest, BarrageMessage> getClientDoSubscribeDescriptor(
            final Options options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final BarrageMessageConsumer.StreamReader<Options> streamReader) {
        return GrpcServiceOverrideBuilder.descriptorFor(
                MethodDescriptor.MethodType.BIDI_STREAMING, DO_SUBSCRIBE,
                ProtoUtils.marshaller(SubscriptionRequest.getDefaultInstance()),
                new BarrageDataMarshaller<>(options, columnChunkTypes, columnTypes, componentTypes, streamReader),
                BarrageServiceGrpc.getDoSubscribeMethod());
    }



    private static class DoSubscribe<Options, View> implements ServerCalls.BidiStreamingMethod<SubscriptionRequest, InputStream> {

        private final BarrageServiceGrpcImpl<Options, View> delegate;

        private DoSubscribe(final BarrageServiceGrpcImpl<Options, View> delegate) {
            this.delegate = delegate;
        }

        @Override
        public StreamObserver<SubscriptionRequest> invoke(final StreamObserver<InputStream> responseObserver) {
            final ServerCallStreamObserver<InputStream> serverCall = (ServerCallStreamObserver<InputStream>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            return delegate.doSubscribeCustom(responseObserver);
        }
    }

    private static class DoSubscribeNoClientStream<Options, View> implements ServerCalls.ServerStreamingMethod<SubscriptionRequest, InputStream> {

        private final BarrageServiceGrpcImpl<Options, View> delegate;

        private DoSubscribeNoClientStream(final BarrageServiceGrpcImpl<Options, View> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void invoke(final SubscriptionRequest request, final StreamObserver<InputStream> responseObserver) {
            final ServerCallStreamObserver<InputStream> serverCall = (ServerCallStreamObserver<InputStream>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            delegate.doSubscribeCustom(request, responseObserver);
        }
    }

    public static class BarrageDataMarshaller<Options> implements MethodDescriptor.Marshaller<BarrageMessage> {
        private final Options options;
        private final ChunkType[] columnChunkTypes;
        private final Class<?>[] columnTypes;
        private final Class<?>[] componentTypes;
        private final BarrageMessageConsumer.StreamReader<Options> streamReader;

        public BarrageDataMarshaller(
                final Options options,
                final ChunkType[] columnChunkTypes,
                final Class<?>[] columnTypes,
                final Class<?>[] componentTypes,
                final BarrageMessageConsumer.StreamReader<Options> streamReader) {
            this.options = options;
            this.columnChunkTypes = columnChunkTypes;
            this.columnTypes = columnTypes;
            this.componentTypes = componentTypes;
            this.streamReader = streamReader;
        }

        @Override
        public InputStream stream(final BarrageMessage value) {
            throw new UnsupportedOperationException("BarrageDataMarshaller unexpectedly used to directly convert BarrageMessage to InputStream");
        }

        @Override
        public BarrageMessage parse(final InputStream stream) {
            return streamReader.safelyParseFrom(options, columnChunkTypes, columnTypes, componentTypes, stream);
        }
    }
}
