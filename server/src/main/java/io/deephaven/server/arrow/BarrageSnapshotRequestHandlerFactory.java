//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import com.google.auto.service.AutoService;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;

@AutoService(ExchangeRequestHandlerFactory.class)
public class BarrageSnapshotRequestHandlerFactory implements ExchangeRequestHandlerFactory {
    @Override
    public byte type() {
        return BarrageMessageType.BarrageSnapshotRequest;
    }

    @Override
    public ArrowFlightUtil.DoExchangeMarshaller.Handler create(final ArrowFlightUtil.DoExchangeMarshaller marshaller,
            final TicketRouter ticketRouter,
            final SessionState session,
            final StreamObserver<BarrageMessageWriter.MessageView> listener,
            final BarrageMessageWriter.Factory streamGeneratorFactory) {
        return new BarrageSnapshotRequestHandler(marshaller, ticketRouter, session, listener, streamGeneratorFactory);
    }
}
