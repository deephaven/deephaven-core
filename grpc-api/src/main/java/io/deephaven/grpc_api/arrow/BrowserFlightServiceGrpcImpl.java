package io.deephaven.grpc_api.arrow;

import com.google.rpc.Code;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.flightjs.protocol.BrowserFlight;
import io.deephaven.flightjs.protocol.BrowserFlightServiceGrpc;
import io.deephaven.grpc_api.barrage.BarrageMessageProducer;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.util.BrowserStream;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.function.Function;

import static io.deephaven.grpc_api.arrow.ArrowFlightUtil.parseProtoMessage;

@Singleton
public class BrowserFlightServiceGrpcImpl<Options, View> extends BrowserFlightServiceGrpc.BrowserFlightServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(BrowserFlightServiceGrpcImpl.class);

    private final SessionService sessionService;
    private final TicketRouter ticketRouter;

    private final BarrageMessageProducer.Operation.Factory<Options, View> operationFactory;
    private final BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<View>> listenerAdapter;
    private final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, Options> optionsAdapter;

    @Inject()
    public BrowserFlightServiceGrpcImpl(final SessionService sessionService,
                                        final TicketRouter ticketRouter,
                                        final BarrageMessageProducer.Operation.Factory<Options, View> operationFactory,
                                        final BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<View>> listenerAdapter,
                                        final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, Options> optionsAdapter) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.operationFactory = operationFactory;
        this.listenerAdapter = listenerAdapter;
        this.optionsAdapter = optionsAdapter;
    }

    public void openHandshakeCustom(final Flight.HandshakeRequest request, final StreamObserver<Flight.HandshakeResponse> responseObserver) {
        throw new UnsupportedOperationException("TODO: open handshake");
    }

    public void nextHandshakeCustom(final Flight.HandshakeRequest request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        throw new UnsupportedOperationException("TODO: next handshake");
    }

    public void openDoPutCustom(final InputStream request, final StreamObserver<Flight.PutResult> responseObserver) {
        internalOnOpen(request, responseObserver, session -> {
            final ArrowFlightUtil.DoPutObserver marshaller = new ArrowFlightUtil.DoPutObserver(session, ticketRouter, responseObserver);
            return new BrowserStream<>(BrowserStream.Mode.IN_ORDER, session, marshaller::process, marshaller::onCancel, marshaller::onComplete);
        });
    }

    public void nextDoPutCustom(final InputStream request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        internalOnNext(request, responseObserver);
    }

    public void openDoExchangeCustom(final InputStream request, final StreamObserver<InputStream> responseObserver) {
        throw new UnsupportedOperationException("TODO: open do exchange");
    }

    public void nextDoExchangeCustom(final InputStream request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        internalOnNext(request, responseObserver);
    }

    private <T> void internalOnOpen(final InputStream request, final StreamObserver<T> responseObserver,
                                    final Function<SessionState, BrowserStream<ArrowFlightUtil.MessageInfo>> browserStreamSupplier) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final ArrowFlightUtil.MessageInfo mi = parseProtoMessage(request);
            if (mi.app_metadata == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "app_metadata not provided or was not a BarrageMessageWrapper");
            }

            final ByteBuffer ticketBuffer = mi.app_metadata.rpcTicketAsByteBuffer();
            if (ticketBuffer == null && !mi.app_metadata.halfCloseAfterMessage()) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "no rpc_ticket provided; cannot export this browser stream but message does not half close");
            }

            final BrowserStream<ArrowFlightUtil.MessageInfo> browserStream = browserStreamSupplier.apply(session);
            browserStream.onMessageReceived(mi);

            if (ticketBuffer != null) {
                session.newExport(ExportTicketHelper.exportIdToTicket(ticketBuffer))
                        .onError(responseObserver::onError)
                        .submit(() -> browserStream);
            }
        });
    }

    public void internalOnNext(final InputStream request, final StreamObserver<BrowserFlight.BrowserNextResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            final ArrowFlightUtil.MessageInfo mi = parseProtoMessage(request);
            if (mi.app_metadata == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "app_metadata not provided or was not a BarrageMessageWrapper");
            }

            final ByteBuffer ticketBuffer = mi.app_metadata.rpcTicketAsByteBuffer();
            if (ticketBuffer == null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "No rpc_ticket provided; cannot append to existing browser stream");
            }

            final SessionState.ExportObject<BrowserStream<ArrowFlightUtil.MessageInfo>> browserStream =
                    session.getExport(ExportTicketHelper.ticketToExportId(ticketBuffer));

            session.nonExport()
                    .require(browserStream)
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        browserStream.get().onMessageReceived(mi);
                    });
        });
    }
}
