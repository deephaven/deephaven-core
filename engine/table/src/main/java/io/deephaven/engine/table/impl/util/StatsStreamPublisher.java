//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class StatsStreamPublisher implements StreamPublisher {
    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("ProcessUniqueId"),
            ColumnDefinition.ofTime("Timestamp"),
            ColumnDefinition.ofString("Name"),
            ColumnDefinition.ofString("Interval"),
            ColumnDefinition.ofString("Type"),
            ColumnDefinition.ofLong("N"),
            ColumnDefinition.ofLong("Sum"),
            ColumnDefinition.ofLong("Last"),
            ColumnDefinition.ofLong("Min"),
            ColumnDefinition.ofLong("Max"),
            ColumnDefinition.ofLong("Avg"),
            ColumnDefinition.ofLong("Sum2"),
            ColumnDefinition.ofLong("Stdev"));

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    StatsStreamPublisher() {
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
            String id,
            long nowMillis,
            String name,
            String interval,
            String type,
            long n,
            long sum,
            long last,
            long min,
            long max,
            long avg,
            long sum2,
            long stdev) {
        chunks[0].<String>asWritableObjectChunk().add(id);
        chunks[1].asWritableLongChunk().add(DateTimeUtils.millisToNanos(nowMillis));
        chunks[2].<String>asWritableObjectChunk().add(name);
        chunks[3].<String>asWritableObjectChunk().add(interval);
        chunks[4].<String>asWritableObjectChunk().add(type);
        chunks[5].asWritableLongChunk().add(n);
        chunks[6].asWritableLongChunk().add(sum);
        chunks[7].asWritableLongChunk().add(last);
        chunks[8].asWritableLongChunk().add(min);
        chunks[9].asWritableLongChunk().add(max);
        chunks[10].asWritableLongChunk().add(avg);
        chunks[11].asWritableLongChunk().add(sum2);
        chunks[12].asWritableLongChunk().add(stdev);
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
