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

import java.util.Objects;

class QueryOperationPerformanceStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("EvaluationNumber"),
            ColumnDefinition.ofLong("ParentEvaluationNumber"),
            ColumnDefinition.ofInt("OperationNumber"),
            ColumnDefinition.ofInt("ParentOperationNumber"),
            ColumnDefinition.ofInt("Depth"),
            ColumnDefinition.ofString("SessionId"),
            ColumnDefinition.ofString("Description"),
            ColumnDefinition.ofString("CallerLine"),
            ColumnDefinition.ofBoolean("IsCompilation"),
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
            ColumnDefinition.ofLong("InputSizeLong"),
            ColumnDefinition.ofBoolean("WasInterrupted"),
            ColumnDefinition.ofString("AuthContext"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    QueryOperationPerformanceStreamPublisher() {
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(final QueryPerformanceNugget nugget) {

        // ColumnDefinition.ofLong("EvaluationNumber"),
        chunks[0].asWritableLongChunk().add(nugget.getEvaluationNumber());

        // ColumnDefinition.ofLong("ParentEvaluationNumber"),
        chunks[1].asWritableLongChunk().add(nugget.getParentEvaluationNumber());

        // ColumnDefinition.ofInt("OperationNumber"),
        chunks[2].asWritableIntChunk().add(nugget.getOperationNumber());

        // ColumnDefinition.ofInt("ParentOperationNumber"),
        chunks[3].asWritableIntChunk().add(nugget.getParentOperationNumber());

        // ColumnDefinition.ofInt("Depth"),
        chunks[4].asWritableIntChunk().add(nugget.getDepth());

        // ColumnDefinition.ofString("SessionId"),
        chunks[5].<String>asWritableObjectChunk().add(nugget.getSessionId());

        // ColumnDefinition.ofString("Description"),
        chunks[6].<String>asWritableObjectChunk().add(nugget.getDescription());

        // ColumnDefinition.ofString("CallerLine"),
        chunks[7].<String>asWritableObjectChunk().add(nugget.getCallerLine());

        // ColumnDefinition.ofBoolean("IsCompilation"),
        chunks[8].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.isCompilation()));

        // ColumnDefinition.ofTime("StartTime"),
        chunks[9].asWritableLongChunk().add(nugget.getStartClockEpochNanos());

        // ColumnDefinition.ofTime("EndTime"),
        chunks[10].asWritableLongChunk().add(nugget.getEndClockEpochNanos());

        // ColumnDefinition.ofLong("UsageNanos"),
        chunks[11].asWritableLongChunk().add(nugget.getUsageNanos());

        // ColumnDefinition.ofLong("CpuNanos"),
        chunks[12].asWritableLongChunk().add(nugget.getCpuNanos());

        // ColumnDefinition.ofLong("UserCpuNanos"),
        chunks[13].asWritableLongChunk().add(nugget.getUserCpuNanos());

        // ColumnDefinition.ofLong("FreeMemory"),
        chunks[14].asWritableLongChunk().add(nugget.getEndFreeMemory());

        // ColumnDefinition.ofLong("TotalMemory"),
        chunks[15].asWritableLongChunk().add(nugget.getEndTotalMemory());

        // ColumnDefinition.ofLong("FreeMemoryChange"),
        chunks[16].asWritableLongChunk().add(nugget.getDiffFreeMemory());

        // ColumnDefinition.ofLong("TotalMemoryChange"),
        chunks[17].asWritableLongChunk().add(nugget.getDiffTotalMemory());

        // ColumnDefinition.ofLong("Collections")
        chunks[18].asWritableLongChunk().add(nugget.getDiffCollections());

        // ColumnDefinition.ofLong("CollectionTimeNanos"),
        chunks[19].asWritableLongChunk().add(nugget.getDiffCollectionTimeNanos());

        // ColumnDefinition.ofLong("AllocatedBytes"),
        chunks[20].asWritableLongChunk().add(nugget.getAllocatedBytes());

        // ColumnDefinition.ofLong("PoolAllocatedBytes"),
        chunks[21].asWritableLongChunk().add(nugget.getPoolAllocatedBytes());

        // ColumnDefinition.ofLong("InputSizeLong"),
        chunks[22].asWritableLongChunk().add(nugget.getInputSize());

        // ColumnDefinition.ofBoolean("WasInterrupted")
        chunks[23].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.wasInterrupted()));

        // ColumnDefinition.ofString("AuthContext")
        chunks[24].<String>asWritableObjectChunk().add(Objects.toString(nugget.getAuthContext()));

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
