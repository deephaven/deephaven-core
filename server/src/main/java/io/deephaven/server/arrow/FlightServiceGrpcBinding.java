/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.server.arrow;

import io.deephaven.server.util.GrpcServiceOverrideBuilder;
import io.deephaven.server.util.PassthroughInputStreamMarshaller;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class FlightServiceGrpcBinding implements BindableService {

    private final FlightServiceGrpcImpl delegate;

    @Inject
    public FlightServiceGrpcBinding(final FlightServiceGrpcImpl service) {
        this.delegate = service;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService())
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
}
