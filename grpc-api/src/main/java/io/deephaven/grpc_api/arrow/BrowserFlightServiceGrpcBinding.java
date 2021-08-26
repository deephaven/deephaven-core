/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.arrow;

import io.deephaven.flightjs.protocol.BrowserFlight;
import io.deephaven.flightjs.protocol.BrowserFlightServiceGrpc;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
import io.deephaven.grpc_api.browserstreaming.BrowserStream;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.util.UnaryInputStreamMarshaller;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api.util.GrpcServiceOverrideBuilder;
import io.deephaven.grpc_api.util.PassthroughInputStreamMarshaller;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.apache.arrow.flight.impl.Flight;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class BrowserFlightServiceGrpcBinding implements BindableService {
    private static final Logger log = LoggerFactory.getLogger(BrowserFlightServiceGrpcBinding.class);


    private final FlightServiceGrpcImpl delegate;
    private final SessionService sessionService;

    @Inject
    public BrowserFlightServiceGrpcBinding(final FlightServiceGrpcImpl service, SessionService sessionService) {
        this.delegate = service;
        this.sessionService = sessionService;
    }

    @Override
    public ServerServiceDefinition bindService() {
        // we use the bindings for the "BrowserFlightService", but actually direct all calls to the real "FlightService"
        return GrpcServiceOverrideBuilder.newBuilder(
                new BrowserFlightServiceGrpc.BrowserFlightServiceImplBase() {}.bindService(),
                BrowserFlightServiceGrpc.SERVICE_NAME)
                .onBidiBrowserSupport(delegate::handshake,
                        BrowserFlightServiceGrpc.getOpenHandshakeMethod(),
                        BrowserFlightServiceGrpc.getNextHandshakeMethod(),
                        ProtoUtils.marshaller(Flight.HandshakeRequest.getDefaultInstance()),
                        ProtoUtils.marshaller(Flight.HandshakeResponse.getDefaultInstance()),
                        ProtoUtils.marshaller(BrowserFlight.BrowserNextResponse.getDefaultInstance()),
                        BrowserStream.Mode.IN_ORDER,
                        log,
                        sessionService)
                .onBidiBrowserSupport(delegate::doPutCustom,
                        BrowserFlightServiceGrpc.getOpenDoPutMethod(),
                        BrowserFlightServiceGrpc.getNextDoPutMethod(),
                        UnaryInputStreamMarshaller.INSTANCE,
                        ProtoUtils.marshaller(Flight.PutResult.getDefaultInstance()),
                        ProtoUtils.marshaller(BrowserFlight.BrowserNextResponse.getDefaultInstance()),
                        BrowserStream.Mode.IN_ORDER,
                        log,
                        sessionService)
                .onBidiBrowserSupport(delegate::doExchangeCustom,
                        BrowserFlightServiceGrpc.getOpenDoExchangeMethod(),
                        BrowserFlightServiceGrpc.getNextDoExchangeMethod(),
                        UnaryInputStreamMarshaller.INSTANCE,
                        PassthroughInputStreamMarshaller.INSTANCE,
                        ProtoUtils.marshaller(BrowserFlight.BrowserNextResponse.getDefaultInstance()),
                        BrowserStream.Mode.IN_ORDER,
                        log,
                        sessionService)
                .build();
    }
}
