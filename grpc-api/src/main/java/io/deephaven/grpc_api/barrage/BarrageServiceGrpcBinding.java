package io.deephaven.grpc_api.barrage;

import io.deephaven.db.backplane.barrage.BarrageMessage;
import io.deephaven.db.backplane.util.GrpcServiceOverrideBuilder;
import io.deephaven.db.v2.sources.chunk.ChunkType;
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
import java.io.InputStream;

public class BarrageServiceGrpcBinding<Options, View> implements BindableService {

    private static final String SERVICE = BarrageServiceGrpc.SERVICE_NAME;

    private static final String DO_SUBSCRIBE = MethodDescriptor.generateFullMethodName(SERVICE, "DoSubscribe");
    private static final String DO_SUBSCRIBE_NO_CLIENT_STREAM = MethodDescriptor.generateFullMethodName(SERVICE, "DoSubscribeNoClientStream");
    private static final PassthroughInputStreamMarshaller PASSTHROUGH_MARSHALLER = new PassthroughInputStreamMarshaller();

    private final BarrageServiceGrpcImpl<Options, View> delegate;

    @Inject
    public BarrageServiceGrpcBinding(final BarrageServiceGrpcImpl<Options, View> service) {
        this.delegate = service;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService())
                .override(getServerDoSubscribeDescriptor(), new DoSubscribe<>(delegate))
                .override(getServerDoSubscribeNoClientStreamDescriptor(), new DoSubscribeNoClientStream<>(delegate))
                .build();
    }

    /**
     * Fetch the client side descriptor for a specific table schema.
     *
     * @param options           the set of options that last across the entire life of the subscription
     * @param columnChunkTypes  the chunk types per column
     * @param columnTypes       the class type per column
     * @param streamReader      the stream reader - intended to be thread safe and re-usable
     * @param <Options>         the options related to deserialization
     * @return the client side method descriptor
     */
    public static <Options> MethodDescriptor<SubscriptionRequest, BarrageMessage> getClientDoSubscribeDescriptor(
            final Options options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final BarrageMessageConsumer.StreamReader<Options> streamReader) {
        return MethodDescriptor.<SubscriptionRequest, BarrageMessage>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                .setFullMethodName(DO_SUBSCRIBE)
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(ProtoUtils.marshaller(SubscriptionRequest.getDefaultInstance()))
                .setResponseMarshaller(new BarrageDataMarshaller<>(options, columnChunkTypes, columnTypes, streamReader))
                .setSchemaDescriptor(BarrageServiceGrpc.getDoSubscribeMethod().getSchemaDescriptor())
                .build();
    }

    private static MethodDescriptor<SubscriptionRequest, InputStream> getServerDoSubscribeDescriptor() {
        return MethodDescriptor.<SubscriptionRequest, InputStream>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                .setFullMethodName(DO_SUBSCRIBE)
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(ProtoUtils.marshaller(SubscriptionRequest.getDefaultInstance()))
                .setResponseMarshaller(PASSTHROUGH_MARSHALLER)
                .setSchemaDescriptor(BarrageServiceGrpc.getDoSubscribeMethod().getSchemaDescriptor())
                .build();
    }

    private static MethodDescriptor<SubscriptionRequest, InputStream> getServerDoSubscribeNoClientStreamDescriptor() {
        return MethodDescriptor.<SubscriptionRequest, InputStream>newBuilder()
                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName(DO_SUBSCRIBE_NO_CLIENT_STREAM)
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(ProtoUtils.marshaller(SubscriptionRequest.getDefaultInstance()))
                .setResponseMarshaller(PASSTHROUGH_MARSHALLER)
                .setSchemaDescriptor(BarrageServiceGrpc.getDoSubscribeNoClientStreamMethod().getSchemaDescriptor())
                .build();
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

    private static class PassthroughInputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {
        @Override
        public InputStream stream(final InputStream inputStream) {
            return inputStream;
        }

        @Override
        public InputStream parse(final InputStream inputStream) {
            throw new UnsupportedOperationException();
        }
    }

    private static class BarrageDataMarshaller<Options> implements MethodDescriptor.Marshaller<BarrageMessage> {
        private final Options options;
        private final ChunkType[] columnChunkTypes;
        private final Class<?>[] columnTypes;
        private final BarrageMessageConsumer.StreamReader<Options> streamReader;

        public BarrageDataMarshaller(
                final Options options,
                final ChunkType[] columnChunkTypes,
                final Class<?>[] columnTypes,
                final BarrageMessageConsumer.StreamReader<Options> streamReader) {
            this.options = options;
            this.columnChunkTypes = columnChunkTypes;
            this.columnTypes = columnTypes;
            this.streamReader = streamReader;
        }

        @Override
        public InputStream stream(final BarrageMessage value) {
            throw new UnsupportedOperationException("BarrageDataMarshaller unexpectedly used to directly convert BarrageMessage to InputStream");
        }

        @Override
        public BarrageMessage parse(final InputStream stream) {
            return streamReader.safelyParseFrom(options, columnChunkTypes, columnTypes, stream);
        }
    }
}
