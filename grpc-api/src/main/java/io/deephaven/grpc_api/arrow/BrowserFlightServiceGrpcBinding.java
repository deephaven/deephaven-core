/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.arrow;

import io.deephaven.flightjs.protocol.BrowserFlight;
import io.deephaven.flightjs.protocol.BrowserFlightServiceGrpc;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
import io.deephaven.grpc_api.util.UnaryInputStreamMarshaller;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.util.GrpcServiceOverrideBuilder;
import io.deephaven.grpc_api.util.PassthroughInputStreamMarshaller;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.apache.arrow.flight.impl.Flight;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class BrowserFlightServiceGrpcBinding implements BindableService {

    // Our BrowserFlight related overrides:
    private final BrowserFlightServiceGrpcImpl<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> delegate;

    @Inject
    public BrowserFlightServiceGrpcBinding(final BrowserFlightServiceGrpcImpl<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> service) {
        this.delegate = service;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService(), BrowserFlightServiceGrpc.SERVICE_NAME)
                .onOpenOverride(delegate::openHandshakeCustom, "OpenHandshake", BrowserFlightServiceGrpc.getOpenHandshakeMethod(),
                        ProtoUtils.marshaller(Flight.HandshakeRequest.getDefaultInstance()),
                        ProtoUtils.marshaller(Flight.HandshakeResponse.getDefaultInstance()))
                .onNextOverride(delegate::nextHandshakeCustom, "NextHandshake", BrowserFlightServiceGrpc.getNextHandshakeMethod(),
                        ProtoUtils.marshaller(Flight.HandshakeRequest.getDefaultInstance()))
                .onOpenOverride(delegate::openDoPutCustom, "OpenDoPut", BrowserFlightServiceGrpc.getOpenDoPutMethod(),
                        UnaryInputStreamMarshaller.INSTANCE,
                        ProtoUtils.marshaller(Flight.PutResult.getDefaultInstance()))
                .onNextOverride(delegate::nextDoPutCustom, "NextDoPut", BrowserFlightServiceGrpc.getNextDoPutMethod(),
                        UnaryInputStreamMarshaller.INSTANCE)
                .onOpenOverride(delegate::openDoExchangeCustom, "OpenDoExchange", BrowserFlightServiceGrpc.getOpenDoExchangeMethod(),
                        UnaryInputStreamMarshaller.INSTANCE,
                        PassthroughInputStreamMarshaller.INSTANCE)
                .onNextOverride(delegate::nextDoExchangeCustom, "NextDoExchange", BrowserFlightServiceGrpc.getNextDoExchangeMethod(),
                        UnaryInputStreamMarshaller.INSTANCE)
                .build();
    }
}
