//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * The ExchangeRequestHandlerFactory is a pluggable component within {@link ArrowFlightUtil} that provides a
 * {@link ArrowFlightUtil.DoExchangeMarshaller.Handler handler} for each type of barrage message.
 *
 * <p>
 * Only one factory is permitted per message type.
 * </p>
 *
 * <p>
 * Note: this interface is not yet stable.
 * </p>
 */
public interface ExchangeRequestHandlerFactory {
    /**
     * The type of message that this factory produces a handler for.
     * 
     * @return the type of message handled by this factory
     */
    byte type();

    ArrowFlightUtil.DoExchangeMarshaller.Handler create(final ArrowFlightUtil.DoExchangeMarshaller marshaller,
            final TicketRouter ticketRouter,
            final SessionState session,
            final StreamObserver<BarrageMessageWriter.MessageView> listener,
            final BarrageMessageWriter.Factory streamGeneratorFactory);
}
