/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.arrow;

import io.deephaven.grpc_api_client.util.GrpcServiceOverrideBuilder;
import io.deephaven.grpc_api.util.PassthroughInputStreamMarshaller;
import io.deephaven.proto.backplane.grpc.BarrageServiceGrpc;
import io.grpc.BindableService;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;

@Singleton
public class FlightServiceGrpcBinding implements BindableService {
    private static final String SERVICE = FlightServiceGrpc.SERVICE_NAME;

    private static final String DO_GET = MethodDescriptor.generateFullMethodName(SERVICE, "DoGet");

    private static final String DO_PUT = MethodDescriptor.generateFullMethodName(SERVICE, "DoPut");
    private static final String DO_PUT_OOB_CLIENT_STREAM = MethodDescriptor.generateFullMethodName(SERVICE, "DoPutOOBClientStream");
    private static final String DO_PUT_OOB_CLIENT_STREAM_UPDATE = MethodDescriptor.generateFullMethodName(SERVICE, "DoPutOOBClientStreamUpdate");

    private final FlightServiceGrpcImpl delegate;

    @Inject
    public FlightServiceGrpcBinding(final FlightServiceGrpcImpl service) {
        this.delegate = service;
    }

    @Override
    public ServerServiceDefinition bindService() {
        return GrpcServiceOverrideBuilder.newBuilder(delegate.bindService())
                .override(GrpcServiceOverrideBuilder.descriptorFor(
                        MethodDescriptor.MethodType.SERVER_STREAMING, DO_GET,
                        ProtoUtils.marshaller(Flight.Ticket.getDefaultInstance()),
                        PassthroughInputStreamMarshaller.INSTANCE,
                        FlightServiceGrpc.getDoGetMethod()), new DoGet(delegate))
                .override(GrpcServiceOverrideBuilder.descriptorFor(
                        MethodDescriptor.MethodType.BIDI_STREAMING, DO_PUT,
                        PassthroughInputStreamMarshaller.INSTANCE,
                        ProtoUtils.marshaller(Flight.PutResult.getDefaultInstance()),
                        FlightServiceGrpc.getDoPutMethod()), new DoPut(delegate))
                .override(GrpcServiceOverrideBuilder.descriptorFor(
                        MethodDescriptor.MethodType.SERVER_STREAMING, DO_PUT_OOB_CLIENT_STREAM,
                        PassthroughInputStreamMarshaller.INSTANCE,
                        ProtoUtils.marshaller(Flight.PutResult.getDefaultInstance()),
                        FlightServiceGrpc.getDoPutOOBClientStreamMethod()), new DoPutOOB(delegate))
                .override(GrpcServiceOverrideBuilder.descriptorFor(
                        MethodDescriptor.MethodType.UNARY, DO_PUT_OOB_CLIENT_STREAM_UPDATE,
                        PassthroughInputStreamMarshaller.INSTANCE,
                        ProtoUtils.marshaller(Flight.OOBPutResult.getDefaultInstance()),
                        FlightServiceGrpc.getDoPutOOBClientStreamUpdateMethod()), new DoPutOOBUpdate(delegate))
                .build();
    }

    private static class DoGet implements ServerCalls.ServerStreamingMethod<Flight.Ticket, InputStream> {
        private final FlightServiceGrpcImpl delegate;

        private DoGet(final FlightServiceGrpcImpl delegate) {
            this.delegate = delegate;
        }

        @Override
        public void invoke(final Flight.Ticket request, final StreamObserver<InputStream> responseObserver) {
            final ServerCallStreamObserver<InputStream> serverCall = (ServerCallStreamObserver<InputStream>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            delegate.doGetCustom(request, responseObserver);
        }
    }

    private static class DoPut implements ServerCalls.BidiStreamingMethod<InputStream, Flight.PutResult> {
        private final FlightServiceGrpcImpl delegate;

        private DoPut(final FlightServiceGrpcImpl delegate) {
            this.delegate = delegate;
        }

        @Override
        public StreamObserver<InputStream> invoke(final StreamObserver<Flight.PutResult> responseObserver) {
            final ServerCallStreamObserver<Flight.PutResult> serverCall = (ServerCallStreamObserver<Flight.PutResult>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            return delegate.doPutCustom(responseObserver);
        }
    }

    private static class DoPutOOB implements ServerCalls.ServerStreamingMethod<InputStream, Flight.PutResult> {
        private final FlightServiceGrpcImpl delegate;

        private DoPutOOB(final FlightServiceGrpcImpl delegate) {
            this.delegate = delegate;
        }

        @Override
        public void invoke(final InputStream request, final StreamObserver<Flight.PutResult> responseObserver) {
            final ServerCallStreamObserver<Flight.PutResult> serverCall = (ServerCallStreamObserver<Flight.PutResult>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            delegate.doPutCustom(request, responseObserver);
        }
    }

    private static class DoPutOOBUpdate implements ServerCalls.UnaryMethod<InputStream, Flight.OOBPutResult> {
        private final FlightServiceGrpcImpl delegate;

        private DoPutOOBUpdate(final FlightServiceGrpcImpl delegate) {
            this.delegate = delegate;
        }

        @Override
        public void invoke(final InputStream request, final StreamObserver<Flight.OOBPutResult> responseObserver) {
            final ServerCallStreamObserver<Flight.OOBPutResult> serverCall = (ServerCallStreamObserver<Flight.OOBPutResult>) responseObserver;
            serverCall.disableAutoInboundFlowControl();
            serverCall.request(Integer.MAX_VALUE);
            delegate.doPutUpdateCustom(request, responseObserver);
        }
    }
}
