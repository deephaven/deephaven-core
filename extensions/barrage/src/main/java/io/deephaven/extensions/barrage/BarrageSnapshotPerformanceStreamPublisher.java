//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class BarrageSnapshotPerformanceStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("TableId"),
            ColumnDefinition.ofString("TableKey"),
            ColumnDefinition.ofTime("RequestTime"),
            ColumnDefinition.ofLong("QueueNanos"),
            ColumnDefinition.ofLong("SnapshotNanos"),
            ColumnDefinition.ofLong("WriteNanos"),
            ColumnDefinition.ofLong("WriteBytes"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    BarrageSnapshotPerformanceStreamPublisher() {
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(
            String tableId,
            String tableKey,
            long requestTimeEpochNanos,
            long queueNanos,
            long snapshotNanos,
            long writeNanos,
            long writeBytes) {
        if (chunks == null) {
            // Shut down: the blink table is gone, so there is nothing to publish to.
            return;
        }
        chunks[0].<String>asWritableObjectChunk().add(tableId);
        chunks[1].<String>asWritableObjectChunk().add(tableKey);
        chunks[2].asWritableLongChunk().add(requestTimeEpochNanos);
        chunks[3].asWritableLongChunk().add(queueNanos);
        chunks[4].asWritableLongChunk().add(snapshotNanos);
        chunks[5].asWritableLongChunk().add(writeNanos);
        chunks[6].asWritableLongChunk().add(writeBytes);
        if (chunks[0].size() == CHUNK_SIZE) {
            flushInternal();
        }
    }

    @Override
    public synchronized void flush() {
        if (chunks == null || chunks[0].size() == 0) {
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
    public synchronized void shutdown() {
        if (chunks == null) {
            return;
        }
        // The blink table is being destroyed; any pending rows will never be delivered.
        SafeCloseableArray.close(chunks);
        chunks = null;
    }
}
