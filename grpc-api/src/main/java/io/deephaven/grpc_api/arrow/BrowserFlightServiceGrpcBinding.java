/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.arrow;

import io.deephaven.flightjs.protocol.BrowserFlightServiceGrpc;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
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
    private static final String BROWSER_SERVICE = BrowserFlightServiceGrpc.SERVICE_NAME;
    private static final String OPEN_HANDSHAKE = MethodDescriptor.generateFullMethodName(BROWSER_SERVICE, "OpenHandshake");
    private static final String NEXT_HANDSHAKE = MethodDescriptor.generateFullMethodName(BROWSER_SERVICE, "NextHandshake");
    private static final String OPEN_DO_PUT = MethodDescriptor.generateFullMethodName(BROWSER_SERVICE, "OpenDoPut");
    private static final String NEXT_DO_PUT = MethodDescriptor.generateFullMethodName(BROWSER_SERVICE, "NextDoPut");
    private static final String OPEN_DO_EXCHANGE = MethodDescriptor.generateFullMethodName(BROWSER_SERVICE, "OpenDoExchange");
    private static final String NEXT_DO_EXCHANGE = MethodDescriptor.generateFullMethodName(BROWSER_SERVICE, "NextDoExchange");

    private final BrowserFlightServiceGrpcImpl<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> delegate;

    @Inject
    public BrowserFlightServiceGrpcBinding(final BrowserFlightServiceGrpcImpl<ChunkInputStreamGenerator.Options, BarrageStreamGenerator.View> service) {
        this.delegate = service;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService())
                .onOpenOverride(delegate::openHandshakeCustom, OPEN_HANDSHAKE, BrowserFlightServiceGrpc.getOpenHandshakeMethod(),
                        ProtoUtils.marshaller(Flight.HandshakeRequest.getDefaultInstance()),
                        ProtoUtils.marshaller(Flight.HandshakeResponse.getDefaultInstance()))
                .onNextOverride(delegate::nextHandshakeCustom, NEXT_HANDSHAKE, BrowserFlightServiceGrpc.getNextHandshakeMethod(),
                        ProtoUtils.marshaller(Flight.HandshakeRequest.getDefaultInstance()))
                .onOpenOverride(delegate::openDoPutCustom, OPEN_DO_PUT, BrowserFlightServiceGrpc.getOpenDoPutMethod(),
                        PassthroughInputStreamMarshaller.INSTANCE,
                        ProtoUtils.marshaller(Flight.PutResult.getDefaultInstance()))
                .onNextOverride(delegate::nextDoPutCustom, NEXT_DO_PUT, BrowserFlightServiceGrpc.getNextDoPutMethod(),
                        PassthroughInputStreamMarshaller.INSTANCE)
                .onOpenOverride(delegate::openDoExchangeCustom, OPEN_DO_EXCHANGE, BrowserFlightServiceGrpc.getOpenDoExchangeMethod(),
                        PassthroughInputStreamMarshaller.INSTANCE,
                        PassthroughInputStreamMarshaller.INSTANCE)
                .onNextOverride(delegate::nextDoExchangeCustom, NEXT_DO_EXCHANGE, BrowserFlightServiceGrpc.getNextDoExchangeMethod(),
                        PassthroughInputStreamMarshaller.INSTANCE)
                .build();
    }
}
