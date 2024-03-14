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

class BarrageSubscriptionPerformanceStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("TableId"),
            ColumnDefinition.ofString("TableKey"),
            ColumnDefinition.ofString("StatType"),
            ColumnDefinition.ofTime("Time"),
            ColumnDefinition.ofLong("Count"),
            ColumnDefinition.ofDouble("Pct50"),
            ColumnDefinition.ofDouble("Pct75"),
            ColumnDefinition.ofDouble("Pct90"),
            ColumnDefinition.ofDouble("Pct95"),
            ColumnDefinition.ofDouble("Pct99"),
            ColumnDefinition.ofDouble("Max"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    BarrageSubscriptionPerformanceStreamPublisher() {
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
            String statType,
            long timeNanos,
            long count,
            double p50,
            double p75,
            double p90,
            double p95,
            double p99,
            double max) {
        chunks[0].<String>asWritableObjectChunk().add(tableId);
        chunks[1].<String>asWritableObjectChunk().add(tableKey);
        chunks[2].<String>asWritableObjectChunk().add(statType);
        chunks[3].asWritableLongChunk().add(timeNanos);
        chunks[4].asWritableLongChunk().add(count);
        chunks[5].asWritableDoubleChunk().add(p50);
        chunks[6].asWritableDoubleChunk().add(p75);
        chunks[7].asWritableDoubleChunk().add(p90);
        chunks[8].asWritableDoubleChunk().add(p95);
        chunks[9].asWritableDoubleChunk().add(p99);
        chunks[10].asWritableDoubleChunk().add(max);
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
