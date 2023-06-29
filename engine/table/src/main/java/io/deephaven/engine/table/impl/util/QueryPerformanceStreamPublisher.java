/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryProcessingResults;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class QueryPerformanceStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("ProcessUniqueId"),
            ColumnDefinition.ofLong("EvaluationNumber"),
            ColumnDefinition.ofTime("StartTime"),
            ColumnDefinition.ofTime("EndTime"),
            ColumnDefinition.ofLong("DurationNanos"),
            ColumnDefinition.ofLong("CpuNanos"),
            ColumnDefinition.ofLong("UserCpuNanos"),
            ColumnDefinition.ofLong("FreeMemory"),
            ColumnDefinition.ofLong("TotalMemory"),
            ColumnDefinition.ofLong("FreeMemoryChange"),
            ColumnDefinition.ofLong("TotalMemoryChange"),
            ColumnDefinition.ofLong("Collections"),
            ColumnDefinition.ofLong("CollectionTimeNanos"),
            ColumnDefinition.ofLong("AllocatedBytes"),
            ColumnDefinition.ofLong("PoolAllocatedBytes"),
            ColumnDefinition.ofBoolean("WasInterrupted"),
            ColumnDefinition.ofBoolean("IsReplayer"),
            ColumnDefinition.ofString("Exception"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    QueryPerformanceStreamPublisher() {
        chunks = StreamToBlinkTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(
            final String id,
            final long evaluationNumber,
            final QueryProcessingResults queryProcessingResults,
            final QueryPerformanceNugget nugget) {
        // ColumnDefinition.ofString("ProcessUniqueId"),
        chunks[0].<String>asWritableObjectChunk().add(id);

        // ColumnDefinition.ofLong("EvaluationNumber")
        chunks[1].asWritableLongChunk().add(evaluationNumber);

        // ColumnDefinition.ofTime("StartTime");
        chunks[2].asWritableLongChunk().add(DateTimeUtils.millisToNanos(nugget.getStartClockTime()));

        // ColumnDefinition.ofTime("EndTime")
        // this is a lie; timestamps should _NOT_ be created based on adding nano time durations to timestamps.
        chunks[3].asWritableLongChunk().add(nugget.getTotalTimeNanos() == null ? QueryConstants.NULL_LONG
                : DateTimeUtils.millisToNanos(nugget.getStartClockTime()) + nugget.getTotalTimeNanos());

        // ColumnDefinition.ofLong("DurationNanos")
        chunks[4].asWritableLongChunk()
                .add(nugget.getTotalTimeNanos() == null ? QueryConstants.NULL_LONG : nugget.getTotalTimeNanos());

        // ColumnDefinition.ofLong("CpuNanos")
        chunks[5].asWritableLongChunk().add(nugget.getCpuNanos());

        // ColumnDefinition.ofLong("UserCpuNanos")
        chunks[6].asWritableLongChunk().add(nugget.getUserCpuNanos());

        // ColumnDefinition.ofLong("FreeMemory")
        chunks[7].asWritableLongChunk().add(nugget.getEndFreeMemory());

        // ColumnDefinition.ofLong("TotalMemory")
        chunks[8].asWritableLongChunk().add(nugget.getEndTotalMemory());

        // ColumnDefinition.ofLong("FreeMemoryChange")
        chunks[9].asWritableLongChunk().add(nugget.getDiffFreeMemory());

        // ColumnDefinition.ofLong("TotalMemoryChange")
        chunks[10].asWritableLongChunk().add(nugget.getDiffTotalMemory());

        // ColumnDefinition.ofLong("Collections")
        chunks[11].asWritableLongChunk().add(nugget.getDiffCollections());

        // ColumnDefinition.ofLong("CollectionTimeNanos")
        chunks[12].asWritableLongChunk().add(nugget.getDiffCollectionTimeNanos());

        // ColumnDefinition.ofLong("AllocatedBytes")
        chunks[13].asWritableLongChunk().add(nugget.getAllocatedBytes());

        // ColumnDefinition.ofLong("PoolAllocatedBytes")
        chunks[14].asWritableLongChunk().add(nugget.getPoolAllocatedBytes());

        // ColumnDefinition.ofBoolean("WasInterrupted")
        chunks[15].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.wasInterrupted()));

        // ColumnDefinition.ofBoolean("IsReplayer")
        chunks[16].asWritableByteChunk().add(BooleanUtils.booleanAsByte(queryProcessingResults.isReplayer()));

        // ColumnDefinition.ofString("Exception")
        chunks[17].<String>asWritableObjectChunk().add(queryProcessingResults.getException());

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
        chunks = StreamToBlinkTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    public void acceptFailure(Throwable e) {
        consumer.acceptFailure(e);
    }
}
