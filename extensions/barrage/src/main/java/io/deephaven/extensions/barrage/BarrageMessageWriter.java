//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.util.SafeCloseable;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.BitSet;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * A {@code BarrageMessageWriter} takes a {@link BarrageMessage} and re-uses portions of the serialized payload across
 * different subscribers that may subscribe to different viewports and columns.
 */
public interface BarrageMessageWriter extends SafeCloseable {

    /**
     * Represents a single update, which might be sent as multiple distinct payloads as necessary based in the
     * implementation.
     *
     * <p>
     * A view of a message, from {@link BarrageMessageWriter#getSubView}, {@link BarrageMessageWriter#getSnapshotView}
     * or {@link BarrageMessageWriter#getDictionaryBatchView}, may be visited only once: visiting it writes the message,
     * and releases what the message was written from.
     */
    interface MessageView {
        void forEachStream(Consumer<DefensiveDrainable> visitor) throws IOException;
    }

    interface Factory {
        /**
         * Create a {@code BarrageMessageWriter} that now owns the {@link BarrageMessage}.
         *
         * @param message the message that contains the update that we would like to propagate
         * @param metricsConsumer a method that can be used to record write metrics
         */
        BarrageMessageWriter newMessageWriter(
                @NotNull BarrageMessage message,
                @NotNull ChunkWriter<Chunk<Values>>[] chunkWriters,
                @NotNull BarrageMessageWriter.WriteMetricsConsumer metricsConsumer);

        /**
         * Create a {@link MessageView} of the Schema to send as the initial message to a new subscriber.
         *
         * @param schemaPayloadWriter a function that writes schema data to a {@link FlatBufferBuilder} and returns the
         *        schema offset
         * @return a MessageView that can be sent to a subscriber
         */
        default MessageView getSchemaView(@NotNull ToIntFunction<FlatBufferBuilder> schemaPayloadWriter) {
            return getSchemaView(schemaPayloadWriter, null);
        }

        /**
         * Create a {@link MessageView} of the Schema to send as the initial message, with a flight descriptor for the
         * table.
         *
         * @param schemaPayloadWriter a function that writes schema data to a {@link FlatBufferBuilder} and returns the
         *        schema offset
         * @param flightDescriptor the flight descriptor for the table, null if it need not be set
         * @return a MessageView that can be sent to a subscriber
         */
        MessageView getSchemaView(@NotNull ToIntFunction<FlatBufferBuilder> schemaPayloadWriter,
                @Nullable Flight.FlightDescriptor flightDescriptor);
    }

    /**
     * @return the {@link BarrageMessage} that this writer is operating on
     */
    BarrageMessage getMessage();

    /**
     * Obtain a Full-Subscription {@link MessageView} of this {@code BarrageMessageWriter} that can be sent to a single
     * subscriber.
     *
     * @param options serialization options for this specific view
     * @param isInitialSnapshot indicates whether this is the first snapshot for the listener
     * @return a MessageView filtered by the subscription properties that can be sent to that subscriber
     */
    MessageView getSubView(BarrageSubscriptionOptions options, boolean isInitialSnapshot);

    /**
     * Obtain a {@link MessageView} of this {@code BarrageMessageWriter} that can be sent to a single subscriber.
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
     * Obtain a {@link MessageView} of this {@code BarrageMessageWriter} that can be sent to a single subscriber, with a
     * persistent per-subscription {@link DictionaryWriterRegistry} that survives across multiple ticking updates.
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
     * @param dictionaryRegistry the persistent dictionary registry for this subscription, or {@code null} if unused
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
            BitSet subscribedColumns,
            @Nullable DictionaryWriterRegistry dictionaryRegistry);

    /**
     * Obtain a Full-Snapshot {@link MessageView} of this {@code BarrageMessageWriter} that can be sent to a single
     * requestor.
     *
     * @param options serialization options for this specific view
     * @return a MessageView filtered by the snapshot properties that can be sent to that requestor
     */
    MessageView getSnapshotView(BarrageSnapshotOptions options);

    /**
     * Obtain a {@link MessageView} of this {@code BarrageMessageWriter} that can be sent to a single requestor.
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

    /**
     * Obtain a {@link MessageView} of this {@code BarrageMessageWriter} that can be sent to a single requestor, with a
     * {@link DictionaryWriterRegistry} for any dictionary-encoded columns in the snapshot.
     * <p>
     * Note that all passed in arguments are owned by the caller and may be modified external to this method.
     *
     * @param options serialization options for this specific view
     * @param viewport is the position-space viewport
     * @param reverseViewport is the viewport reversed (relative to end of table instead of beginning)
     * @param snapshotColumns are the columns included for this view
     * @param dictionaryRegistry the dictionary registry for this snapshot, or {@code null} if unused
     * @return a MessageView filtered by the snapshot properties that can be sent to that requestor
     */
    MessageView getSnapshotView(
            BarrageSnapshotOptions options,
            @Nullable RowSet viewport,
            boolean reverseViewport,
            @Nullable RowSet keyspaceViewport,
            BitSet snapshotColumns,
            @Nullable DictionaryWriterRegistry dictionaryRegistry);

    /**
     * Obtain a {@link MessageView} of standalone DictionaryBatches carrying {@code values} for dictionary
     * {@code dictId}, framed like this writer's other messages. A producer that writes one message for several
     * subscribers sends these first, to bring each subscriber's dictionary level with the others'.
     * <p>
     * The values make one DictionaryBatch, or several when one would reach the maximum message size {@code options}
     * allow or carry more values than their batch size, as a message's rows are split into record batches. Only the
     * first replaces the dictionary when {@code isDelta} is {@code false}; the rest append to it.
     * <p>
     * Takes ownership of {@code values}. The returned view must be visited exactly once.
     *
     * @param options serialization options for the values, including the maximum message size
     * @param dictId the dictionary id
     * @param valuesWriter the writer for the dictionary's values
     * @param values the values to send
     * @param isDelta whether the values append to the dictionary the subscriber holds, rather than replace it
     * @return a MessageView of the DictionaryBatches, one stream each
     */
    MessageView getDictionaryBatchView(
            BarrageOptions options,
            long dictId,
            ChunkWriter<Chunk<Values>> valuesWriter,
            WritableChunk<Values> values,
            boolean isDelta);

    /**
     * Optional metrics API to track how many bytes a barrage message consumed, and how many nanoseconds it took to
     * write it.
     */
    interface WriteMetricsConsumer {
        WriteMetricsConsumer NO_OP = (bytes, writeNanos) -> {
        };

        /**
         * Called once the whole message has been written, with its size and how long it took to write. A view that
         * several subscribers share is written once, so this is called once for all of them.
         *
         * @param bytes the number of bytes in the message, every batch it was written as counted, dictionary batches
         *        included
         * @param writeNanos the elapsed nanoseconds, not CPU time, from starting to encode the message until the
         *        visitor returned from its last batch; this includes the visitor's own time, such as delivering each
         *        batch
         */
        void onWrite(long bytes, long writeNanos);
    }
}
