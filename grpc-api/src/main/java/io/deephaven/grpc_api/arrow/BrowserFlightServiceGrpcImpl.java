package io.deephaven.grpc_api.arrow;

import io.deephaven.flightjs.protocol.BrowserFlight;
import io.deephaven.flightjs.protocol.BrowserFlightServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;

@Singleton
public class BrowserFlightServiceGrpcImpl<Options, View> extends BrowserFlightServiceGrpc.BrowserFlightServiceImplBase {
    @Inject()
    public BrowserFlightServiceGrpcImpl() {

    }

    public void openHandshakeCustom(final Flight.HandshakeRequest request, final StreamObserver<Flight.HandshakeResponse> responseObserver) {
        throw new UnsupportedOperationException("TODO: open handshake");
    }

    public void nextHandshakeCustom(final Flight.HandshakeRequest request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        throw new UnsupportedOperationException("TODO: next handshake");
    }

    public void openDoPutCustom(final InputStream request, final StreamObserver<Flight.PutResult> responseObserver) {
        throw new UnsupportedOperationException("TODO: open do put");
    }

    public void nextDoPutCustom(final InputStream request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        throw new UnsupportedOperationException("TODO: next do put");
    }

    public void openDoExchangeCustom(final InputStream request, final StreamObserver<InputStream> responseObserver) {
        throw new UnsupportedOperationException("TODO: open do exchange");
    }

    public void nextDoExchangeCustom(final InputStream request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        throw new UnsupportedOperationException("TODO: next do exchange");
    }
}
