/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.arrow;

import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.grpc_api.barrage.BarrageMessageConsumer;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
import io.deephaven.grpc_api.util.GrpcServiceOverrideBuilder;
import io.deephaven.grpc_api.util.PassthroughInputStreamMarshaller;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;

@Singleton
public class FlightServiceGrpcBinding implements BindableService {

    private final FlightServiceGrpcImpl delegate;

    @Inject
    public FlightServiceGrpcBinding(final FlightServiceGrpcImpl service) {
        this.delegate = service;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService(), FlightServiceGrpc.SERVICE_NAME)
                .onServerStreamingOverride(delegate::doGetCustom, FlightServiceGrpc.getDoGetMethod(),
                        ProtoUtils.marshaller(Flight.Ticket.getDefaultInstance()),
                        PassthroughInputStreamMarshaller.INSTANCE)
                .onBidiOverride(delegate::doPutCustom, FlightServiceGrpc.getDoPutMethod(),
                        PassthroughInputStreamMarshaller.INSTANCE,
                        ProtoUtils.marshaller(Flight.PutResult.getDefaultInstance()))
                .onBidiOverride(delegate::doExchangeCustom, FlightServiceGrpc.getDoExchangeMethod(),
                        PassthroughInputStreamMarshaller.INSTANCE,
                        PassthroughInputStreamMarshaller.INSTANCE)
                .build();
    }

    /**
     * Fetch the client side descriptor for a specific table schema.
     *
     * @param options the set of options that last across the entire life of the subscription
     * @param columnChunkTypes the chunk types per column
     * @param columnTypes the class type per column
     * @param componentTypes the component class type per column
     * @param streamReader the stream reader - intended to be thread safe and re-usable
     * @param <Options> the options related to deserialization
     * @return the client side method descriptor
     */
    public static <Options> MethodDescriptor<Flight.FlightData, BarrageMessage> getClientDoExchangeDescriptor(
            final Options options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final BarrageMessageConsumer.StreamReader<Options> streamReader) {
        return GrpcServiceOverrideBuilder.descriptorFor(
                MethodDescriptor.MethodType.BIDI_STREAMING, FlightServiceGrpc.SERVICE_NAME, "DoExchange",
                ProtoUtils.marshaller(Flight.FlightData.getDefaultInstance()),
                new BarrageDataMarshaller<>(options, columnChunkTypes, columnTypes, componentTypes, streamReader),
                FlightServiceGrpc.getDoExchangeMethod());
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
            throw new UnsupportedOperationException(
                    "BarrageDataMarshaller unexpectedly used to directly convert BarrageMessage to InputStream");
        }

        @Override
        public BarrageMessage parse(final InputStream stream) {
            return streamReader.safelyParseFrom(options, columnChunkTypes, columnTypes, componentTypes, stream);
        }
    }
}
