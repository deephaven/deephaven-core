//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.grpc.stub.StreamObserver;

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
     * <p>
     * Message types are defined in {@link io.deephaven.barrage.flatbuf.BarrageMessageType}.
     * </p>
     * 
     * @return the type of message handled by this factory
     */
    byte type();

    /**
     * Creates a handler for a DoExchange message.
     * 
     * @param marshaller the DoExchangeMarshaller handling the message
     * @param listener the listener for results of this message
     * @return the Handler for the message
     */
    ArrowFlightUtil.DoExchangeMarshaller.Handler create(final ArrowFlightUtil.DoExchangeMarshaller marshaller,
            final StreamObserver<BarrageMessageWriter.MessageView> listener);
}
