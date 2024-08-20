//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

class QueryPerformanceStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("EvaluationNumber"),
            ColumnDefinition.ofLong("ParentEvaluationNumber"),
            ColumnDefinition.ofString("SessionId"),
            ColumnDefinition.ofString("Description"),
            ColumnDefinition.ofTime("StartTime"),
            ColumnDefinition.ofTime("EndTime"),
            ColumnDefinition.ofLong("UsageNanos"),
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
            ColumnDefinition.ofString("Exception"),
            ColumnDefinition.ofString("AuthContext"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    QueryPerformanceStreamPublisher() {
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
            @NotNull final QueryPerformanceNugget nugget,
            @Nullable final Exception exception) {

        // ColumnDefinition.ofLong("EvaluationNumber")
        chunks[0].asWritableLongChunk().add(nugget.getEvaluationNumber());

        // ColumnDefinition.ofLong("ParentEvaluationNumber")
        chunks[1].asWritableLongChunk().add(nugget.getParentEvaluationNumber());

        // ColumnDefinition.ofString("SessionId")
        chunks[2].<String>asWritableObjectChunk().add(nugget.getSessionId());

        // ColumnDefinition.ofString("Description")
        chunks[3].<String>asWritableObjectChunk().add(nugget.getDescription());

        // ColumnDefinition.ofTime("StartTime");
        chunks[4].asWritableLongChunk().add(nugget.getStartClockEpochNanos());

        // ColumnDefinition.ofTime("EndTime")
        chunks[5].asWritableLongChunk().add(nugget.getEndClockEpochNanos());

        // ColumnDefinition.ofLong("UsageNanos")
        chunks[6].asWritableLongChunk().add(nugget.getUsageNanos());

        // ColumnDefinition.ofLong("CpuNanos")
        chunks[7].asWritableLongChunk().add(nugget.getCpuNanos());

        // ColumnDefinition.ofLong("UserCpuNanos")
        chunks[8].asWritableLongChunk().add(nugget.getUserCpuNanos());

        // ColumnDefinition.ofLong("FreeMemory")
        chunks[9].asWritableLongChunk().add(nugget.getEndFreeMemory());

        // ColumnDefinition.ofLong("TotalMemory")
        chunks[10].asWritableLongChunk().add(nugget.getEndTotalMemory());

        // ColumnDefinition.ofLong("FreeMemoryChange")
        chunks[11].asWritableLongChunk().add(nugget.getDiffFreeMemory());

        // ColumnDefinition.ofLong("TotalMemoryChange")
        chunks[12].asWritableLongChunk().add(nugget.getDiffTotalMemory());

        // ColumnDefinition.ofLong("Collections")
        chunks[13].asWritableLongChunk().add(nugget.getDiffCollections());

        // ColumnDefinition.ofLong("CollectionTimeNanos")
        chunks[14].asWritableLongChunk().add(nugget.getDiffCollectionTimeNanos());

        // ColumnDefinition.ofLong("AllocatedBytes")
        chunks[15].asWritableLongChunk().add(nugget.getAllocatedBytes());

        // ColumnDefinition.ofLong("PoolAllocatedBytes")
        chunks[16].asWritableLongChunk().add(nugget.getPoolAllocatedBytes());

        // ColumnDefinition.ofBoolean("WasInterrupted")
        chunks[17].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.wasInterrupted()));

        // ColumnDefinition.ofString("Exception")
        chunks[18].<String>asWritableObjectChunk().add(exception == null ? null : exception.getMessage());

        // ColumnDefinition.ofString("AuthContext")
        chunks[19].<String>asWritableObjectChunk().add(Objects.toString(nugget.getAuthContext()));

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
