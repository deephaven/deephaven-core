//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.util.TableLoggers;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/**
 * A stream publisher for the update performance ancestors log to produce the
 * {@link TableLoggers#updatePerformanceAncestorsLog()} table.
 */
class UpdatePerformanceAncestorStreamPublisher implements StreamPublisher {
    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("UpdateGraph"),
            ColumnDefinition.ofLong("EntryId"),
            ColumnDefinition.ofString("EntryDescription"),
            ColumnDefinition.ofVector("Ancestors", LongVector.class));

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    public UpdatePerformanceAncestorStreamPublisher() {
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(String updateGraphName, long id, String description, long[] ancestors) {
        // ColumnDefinition.ofString("UpdateGraph"),
        chunks[0].asWritableObjectChunk().add(updateGraphName);
        // ColumnDefinition.ofInt("EntryId"),
        chunks[1].asWritableLongChunk().add(id);
        // ColumnDefinition.ofString("EntryDescription"),
        chunks[2].asWritableObjectChunk().add(description);
        // ColumnDefinition.ofLong("Ancestors"),
        chunks[3].asWritableObjectChunk().add(new LongVectorDirect(ancestors));

        if (chunks[0].size() == CHUNK_SIZE) {
            flushInternal();
        }
    }

    @Override
    public synchronized void flush() {
        if (chunks[0].size() == 0) {
            return;
        }
        flushInternal();
    }

    private void flushInternal() {
        consumer.accept(chunks);
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    public void acceptFailure(Throwable e) {
        consumer.acceptFailure(e);
    }

    @Override
    public void shutdown() {
        flush();
        SafeCloseableArray.close(chunks);
        chunks = null;
    }
}
