//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import org.jetbrains.annotations.NotNull;

/**
 * Re-usable abstract implementation of {@link StreamPublisher} for stream ingestion to column-chunks.
 * <p>
 * Users must {@link #register(StreamConsumer) register a consumer} before allowing other threads or objects to interact
 * with a StreamPublisherBase.
 * <p>
 * Implementations should override {@link #shutdown() shutdown} to ensure that their upstream source is properly
 * shutdown, and synchronize on {@code this} if they have a need to prevent concurrent calls to {@link #flush() flush}.
 */
public abstract class StreamPublisherBase extends ReferenceCountedLivenessNode implements StreamPublisher {

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    private final TableDefinition tableDefinition;

    protected StreamConsumer consumer;

    protected WritableChunk<Values>[] chunks;

    protected StreamPublisherBase(@NotNull final TableDefinition tableDefinition) {
        super(false);
        this.tableDefinition = tableDefinition;
    }

    @Override
    public void register(@NotNull final StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException(String.format(
                    "Can not register multiple stream consumers: %s already registered, attempted to re-register %s",
                    this.consumer, consumer));
        }
        this.consumer = consumer;
    }

    protected synchronized WritableChunk<Values>[] getChunksToFill() {
        if (chunks == null) {
            chunks = StreamChunkUtils.makeChunksForDefinition(tableDefinition, CHUNK_SIZE);
        }
        return chunks;
    }

    @Override
    public synchronized void flush() {
        if (chunks != null) {
            consumer.accept(chunks);
            chunks = null;
        }
    }
}
