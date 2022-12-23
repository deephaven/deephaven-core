package io.deephaven.extensions.barrage;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

import java.util.BitSet;
import java.util.Map;

/**
 * A StreamGenerator takes a BarrageMessage and re-uses portions of the serialized payload across different subscribers
 * that may subscribe to different viewports and columns.
 *
 * @param <MessageView> The sub-view type that the listener expects to receive.
 */
public interface BarrageStreamGenerator<MessageView> extends SafeCloseable {

    interface Factory<MessageView> {
        /**
         * Create a StreamGenerator that now owns the BarrageMessage.
         *
         * @param message the message that contains the update that we would like to propagate
         * @param metricsConsumer a method that can be used to record write metrics
         */
        BarrageStreamGenerator<MessageView> newGenerator(
                BarrageMessage message, BarragePerformanceLog.WriteMetricsConsumer metricsConsumer);

        /**
         * Create a MessageView of the Schema to send as the initial message to a new subscriber.
         *
         * @param table the description of the table's data layout
         * @param attributes the table attributes
         * @return a MessageView that can be sent to a subscriber
         */
        MessageView getSchemaView(TableDefinition table, Map<String, Object> attributes);
    }

    /**
     * @return the BarrageMessage that this generator is operating on
     */
    BarrageMessage getMessage();

    /**
     * Obtain a Full-Subscription View of this StreamGenerator that can be sent to a single subscriber.
     *
     * @param options serialization options for this specific view
     * @param isInitialSnapshot indicates whether or not this is the first snapshot for the listener
     * @return a MessageView filtered by the subscription properties that can be sent to that subscriber
     */
    MessageView getSubView(BarrageSubscriptionOptions options, boolean isInitialSnapshot);

    /**
     * Obtain a View of this StreamGenerator that can be sent to a single subscriber.
     *
     * @param options serialization options for this specific view
     * @param isInitialSnapshot indicates whether or not this is the first snapshot for the listener
     * @param viewport is the position-space viewport
     * @param reverseViewport is the viewport reversed (relative to end of table instead of beginning)
     * @param keyspaceViewport is the key-space viewport
     * @param subscribedColumns are the columns subscribed for this view
     * @return a MessageView filtered by the subscription properties that can be sent to that subscriber
     */
    MessageView getSubView(BarrageSubscriptionOptions options, boolean isInitialSnapshot, @Nullable RowSet viewport,
            boolean reverseViewport, @Nullable RowSet keyspaceViewport, BitSet subscribedColumns);

    /**
     * Obtain a Full-Snapshot View of this StreamGenerator that can be sent to a single requestor.
     *
     * @param options serialization options for this specific view
     * @return a MessageView filtered by the snapshot properties that can be sent to that requestor
     */
    MessageView getSnapshotView(BarrageSnapshotOptions options);

    /**
     * Obtain a View of this StreamGenerator that can be sent to a single requestor.
     *
     * @param options serialization options for this specific view
     * @param viewport is the position-space viewport
     * @param reverseViewport is the viewport reversed (relative to end of table instead of beginning)
     * @param snapshotColumns are the columns included for this view
     * @return a MessageView filtered by the snapshot properties that can be sent to that requestor
     */
    MessageView getSnapshotView(BarrageSnapshotOptions options, @Nullable RowSet viewport, boolean reverseViewport,
            @Nullable RowSet keyspaceViewport, BitSet snapshotColumns);

}
