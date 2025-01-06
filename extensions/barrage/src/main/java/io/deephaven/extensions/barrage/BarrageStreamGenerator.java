//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.BitSet;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * A StreamGenerator takes a BarrageMessage and re-uses portions of the serialized payload across different subscribers
 * that may subscribe to different viewports and columns.
 */
public interface BarrageStreamGenerator extends SafeCloseable {

    /**
     * Represents a single update, which might be sent as multiple distinct payloads as necessary based in the
     * implementation.
     */
    interface MessageView {
        void forEachStream(Consumer<DefensiveDrainable> visitor) throws IOException;
    }

    interface Factory {
        /**
         * Create a StreamGenerator that now owns the BarrageMessage.
         *
         * @param message the message that contains the update that we would like to propagate
         * @param metricsConsumer a method that can be used to record write metrics
         */
        BarrageStreamGenerator newGenerator(
                BarrageMessage message, BarragePerformanceLog.WriteMetricsConsumer metricsConsumer);

        /**
         * Create a MessageView of the Schema to send as the initial message to a new subscriber.
         *
         * @param schemaPayloadWriter a function that writes schema data to a {@link FlatBufferBuilder} and returns the
         *        schema offset
         * @return a MessageView that can be sent to a subscriber
         */
        MessageView getSchemaView(@NotNull ToIntFunction<FlatBufferBuilder> schemaPayloadWriter);
    }

    /**
     * @return the BarrageMessage that this generator is operating on
     */
    BarrageMessage getMessage();

    /**
     * Obtain a Full-Subscription View of this StreamGenerator that can be sent to a single subscriber.
     *
     * @param options serialization options for this specific view
     * @param isInitialSnapshot indicates whether this is the first snapshot for the listener
     * @return a MessageView filtered by the subscription properties that can be sent to that subscriber
     */
    MessageView getSubView(BarrageSubscriptionOptions options, boolean isInitialSnapshot);

    /**
     * Obtain a View of this StreamGenerator that can be sent to a single subscriber.
     * <p>
     * Note that all passed in arguments are owned by the caller and may be modified external to this method.
     *
     * @param options serialization options for this specific view
     * @param isInitialSnapshot indicates whether this is the first snapshot for the listener
     * @param isFullSubscription whether this is a full subscription (possibly a growing viewport)
     * @param viewport is the position-space viewport
     * @param reverseViewport is the viewport reversed (relative to end of table instead of beginning)
     * @param keyspaceViewportPrev is the key-space viewport prior to applying the update
     * @param keyspaceViewport is the key-space viewport
     * @param subscribedColumns are the columns subscribed for this view
     * @return a MessageView filtered by the subscription properties that can be sent to that subscriber
     */
    MessageView getSubView(
            BarrageSubscriptionOptions options,
            boolean isInitialSnapshot,
            boolean isFullSubscription,
            @Nullable RowSet viewport,
            boolean reverseViewport,
            @Nullable RowSet keyspaceViewportPrev,
            @Nullable RowSet keyspaceViewport,
            BitSet subscribedColumns);

    /**
     * Obtain a Full-Snapshot View of this StreamGenerator that can be sent to a single requestor.
     *
     * @param options serialization options for this specific view
     * @return a MessageView filtered by the snapshot properties that can be sent to that requestor
     */
    MessageView getSnapshotView(BarrageSnapshotOptions options);

    /**
     * Obtain a View of this StreamGenerator that can be sent to a single requestor.
     * <p>
     * Note that all passed in arguments are owned by the caller and may be modified external to this method.
     *
     * @param options serialization options for this specific view
     * @param viewport is the position-space viewport
     * @param reverseViewport is the viewport reversed (relative to end of table instead of beginning)
     * @param snapshotColumns are the columns included for this view
     * @return a MessageView filtered by the snapshot properties that can be sent to that requestor
     */
    MessageView getSnapshotView(
            BarrageSnapshotOptions options,
            @Nullable RowSet viewport,
            boolean reverseViewport,
            @Nullable RowSet keyspaceViewport, BitSet snapshotColumns);

}
