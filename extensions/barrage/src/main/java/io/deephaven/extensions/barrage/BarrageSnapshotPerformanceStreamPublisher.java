//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class BarrageSnapshotPerformanceStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("TableId"),
            ColumnDefinition.ofString("TableKey"),
            ColumnDefinition.ofTime("RequestTime"),
            ColumnDefinition.ofDouble("QueueMillis"),
            ColumnDefinition.ofDouble("SnapshotMillis"),
            ColumnDefinition.ofDouble("WriteMillis"),
            ColumnDefinition.ofDouble("WriteMegabits"));
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
            long requestNanos,
            double queueMillis,
            double snapshotMillis,
            double writeMillis,
            double writeMegabits) {
        chunks[0].<String>asWritableObjectChunk().add(tableId);
        chunks[1].<String>asWritableObjectChunk().add(tableKey);
        chunks[2].asWritableLongChunk().add(requestNanos);
        chunks[3].asWritableDoubleChunk().add(queueMillis);
        chunks[4].asWritableDoubleChunk().add(snapshotMillis);
        chunks[5].asWritableDoubleChunk().add(writeMillis);
        chunks[6].asWritableDoubleChunk().add(writeMegabits);
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
    public void shutdown() {}
}
