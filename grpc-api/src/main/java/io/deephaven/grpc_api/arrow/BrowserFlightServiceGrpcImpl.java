package io.deephaven.grpc_api.arrow;

import com.google.rpc.Code;
import io.deephaven.flightjs.protocol.BrowserFlight;
import io.deephaven.flightjs.protocol.BrowserFlightServiceGrpc;
import io.deephaven.grpc_api.browserstreaming.BrowserStreamInterceptor;
import io.deephaven.grpc_api.browserstreaming.StreamData;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.browserstreaming.BrowserStream;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;
import java.util.function.Function;

import static io.deephaven.grpc_api.arrow.ArrowFlightUtil.parseProtoMessage;

@Singleton
public class BrowserFlightServiceGrpcImpl<Options, View> extends BrowserFlightServiceGrpc.BrowserFlightServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(BrowserFlightServiceGrpcImpl.class);

    private final SessionService sessionService;
    private final TicketRouter ticketRouter;
    private final ArrowFlightUtil.DoExchangeMarshaller.Factory<Options, View> doExchangeFactory;

    @Inject()
    public BrowserFlightServiceGrpcImpl(final SessionService sessionService,
                                        final TicketRouter ticketRouter,
                                        final ArrowFlightUtil.DoExchangeMarshaller.Factory<Options, View> doExchangeFactory) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.doExchangeFactory = doExchangeFactory;
    }

    public void openHandshakeCustom(final Flight.HandshakeRequest request, final StreamObserver<Flight.HandshakeResponse> responseObserver) {
        throw GrpcUtil.statusRuntimeException(Code.UNIMPLEMENTED, "See deephaven-core#997; support flight auth.");
    }

    public void nextHandshakeCustom(final Flight.HandshakeRequest request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        throw GrpcUtil.statusRuntimeException(Code.UNIMPLEMENTED, "See deephaven-core#997; support flight auth.");
    }

    public void openDoPutCustom(final InputStream request, final StreamObserver<Flight.PutResult> responseObserver) {
        internalOnOpen(request, responseObserver, session -> {
            final ArrowFlightUtil.DoPutObserver marshaller = new ArrowFlightUtil.DoPutObserver(session, ticketRouter, responseObserver);
            return new BrowserStream<>(BrowserStream.Mode.IN_ORDER, session, marshaller);
        });
    }

    public void nextDoPutCustom(final InputStream request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        internalOnNext(request, responseObserver);
    }

    public void openDoExchangeCustom(final InputStream request, final StreamObserver<InputStream> responseObserver) {
        internalOnOpen(request, responseObserver, session -> {
            final ArrowFlightUtil.DoExchangeMarshaller<Options, View> marshaller = doExchangeFactory.openExchange(session, responseObserver);
            return new BrowserStream<>(BrowserStream.Mode.IN_ORDER, session, marshaller);
        });
    }

    public void nextDoExchangeCustom(final InputStream request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        internalOnNext(request, responseObserver);
    }

    private <T> void internalOnOpen(final InputStream request, final StreamObserver<T> responseObserver,
                                    final Function<SessionState, BrowserStream<ArrowFlightUtil.MessageInfo>> browserStreamSupplier) {
        StreamData streamData = BrowserStreamInterceptor.STREAM_DATA_KEY.get();
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final ArrowFlightUtil.MessageInfo mi = parseProtoMessage(request);

            if (streamData == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no x-deephaven-stream headers, cannot handle open request");
            }

            final BrowserStream<ArrowFlightUtil.MessageInfo> browserStream = browserStreamSupplier.apply(session);
            browserStream.onMessageReceived(mi, streamData);

            if (!streamData.isHalfClose()) {
                // if this isn't a half-close, we should export it for later calls - if it is, the client won't send more messages
                session.newExport(streamData.getRpcTicket())
                        .onError(responseObserver::onError)
                        .submit(() -> browserStream);
            }
        });
    }

    private void internalOnNext(final InputStream request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        StreamData streamData = BrowserStreamInterceptor.STREAM_DATA_KEY.get();
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final ArrowFlightUtil.MessageInfo mi = parseProtoMessage(request);

            if (streamData == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no x-deephaven-stream headers, cannot handle open request");
            }

            final SessionState.ExportObject<BrowserStream<ArrowFlightUtil.MessageInfo>> browserStream =
                    session.getExport(streamData.getRpcTicket());

            session.nonExport()
                    .require(browserStream)
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        browserStream.get().onMessageReceived(mi, streamData);
                        responseObserver.onNext(BrowserFlight.BrowserNextResponse.getDefaultInstance());
                        responseObserver.onCompleted();
                    });
        });
    }
}
