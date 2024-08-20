//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class ServerStateStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofTime("IntervalStartTime"),
            ColumnDefinition.ofInt("IntervalDurationMicros"),
            ColumnDefinition.ofInt("TotalMemoryMiB"),
            ColumnDefinition.ofInt("FreeMemoryMiB"),
            ColumnDefinition.ofShort("IntervalCollections"),
            ColumnDefinition.ofInt("IntervalCollectionTimeMicros"),
            ColumnDefinition.ofShort("IntervalUGPCyclesOnBudget"),
            ColumnDefinition.of("IntervalUGPCyclesTimeMicros", Type.intType().arrayType()),
            ColumnDefinition.ofShort("IntervalUGPCyclesSafePoints"),
            ColumnDefinition.ofInt("IntervalUGPCyclesSafePointTimeMicros"));

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    ServerStateStreamPublisher() {
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
            final long intervalStartTimeMillis,
            final int intervalDurationMicros,
            final int totalMemoryMiB,
            final int freeMemoryMiB,
            final short intervalCollections,
            final int intervalCollectionTimeMicros,
            final short intervalUGPCyclesOnBudget,
            final int[] intervalUGPCyclesTimeMicros,
            final short intervalUGPCyclesSafePoints,
            final int intervalUGPCyclesSafePointTimeMicros) {
        chunks[0].asWritableLongChunk().add(DateTimeUtils.millisToNanos(intervalStartTimeMillis));
        chunks[1].asWritableIntChunk().add(intervalDurationMicros);
        chunks[2].asWritableIntChunk().add(totalMemoryMiB);
        chunks[3].asWritableIntChunk().add(freeMemoryMiB);
        chunks[4].asWritableShortChunk().add(intervalCollections);
        chunks[5].asWritableIntChunk().add(intervalCollectionTimeMicros);
        chunks[6].asWritableShortChunk().add(intervalUGPCyclesOnBudget);
        chunks[7].<int[]>asWritableObjectChunk().add(intervalUGPCyclesTimeMicros);
        chunks[8].asWritableShortChunk().add(intervalUGPCyclesSafePoints);
        chunks[9].asWritableIntChunk().add(intervalUGPCyclesSafePointTimeMicros);
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
