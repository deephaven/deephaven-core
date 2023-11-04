/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
            ColumnDefinition.ofString("ProcessUniqueId"),
            ColumnDefinition.ofLong("EvaluationNumber"),
            ColumnDefinition.ofLong("ParentEvaluationNumber"),
            ColumnDefinition.ofInt("OperationNumber"),
            ColumnDefinition.ofInt("ParentOperationNumber"),
            ColumnDefinition.ofInt("Depth"),
            ColumnDefinition.ofString("Description"),
            ColumnDefinition.ofString("CallerLine"),
            ColumnDefinition.ofBoolean("IsQueryLevel"),
            ColumnDefinition.ofBoolean("IsTopLevel"),
            ColumnDefinition.ofBoolean("IsCompilation"),
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

    public synchronized void add(
            final String id,
            final QueryPerformanceNugget nugget) {

        // ColumnDefinition.ofString("ProcessUniqueId"),
        chunks[0].<String>asWritableObjectChunk().add(id);

        // ColumnDefinition.ofLong("EvaluationNumber"),
        chunks[1].asWritableLongChunk().add(nugget.getEvaluationNumber());

        // ColumnDefinition.ofLong("ParentEvaluationNumber"),
        chunks[2].asWritableLongChunk().add(nugget.getParentEvaluationNumber());

        // ColumnDefinition.ofInt("OperationNumber"),
        chunks[3].asWritableIntChunk().add(nugget.getOperationNumber());

        // ColumnDefinition.ofInt("ParentOperationNumber"),
        chunks[4].asWritableIntChunk().add(nugget.getParentOperationNumber());

        // ColumnDefinition.ofInt("Depth"),
        chunks[5].asWritableIntChunk().add(nugget.getDepth());

        // ColumnDefinition.ofString("Description"),
        chunks[6].<String>asWritableObjectChunk().add(nugget.getName());

        // ColumnDefinition.ofString("CallerLine"),
        chunks[7].<String>asWritableObjectChunk().add(nugget.getCallerLine());

        // ColumnDefinition.ofBoolean("IsQueryLevel"),
        chunks[8].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.isQueryLevel()));

        // ColumnDefinition.ofBoolean("IsTopLevel"),
        chunks[9].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.isTopLevel()));

        // ColumnDefinition.ofBoolean("IsCompilation"),
        chunks[10].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.getName().startsWith("Compile:")));

        // ColumnDefinition.ofTime("StartTime"),
        chunks[11].asWritableLongChunk().add(nugget.getStartClockTime());

        // ColumnDefinition.ofTime("EndTime"),
        chunks[12].asWritableLongChunk().add(nugget.getEndClockTime());

        // ColumnDefinition.ofLong("DurationNanos"),
        chunks[13].asWritableLongChunk().add(nugget.getTotalTimeNanos());

        // ColumnDefinition.ofLong("CpuNanos"),
        chunks[14].asWritableLongChunk().add(nugget.getCpuNanos());

        // ColumnDefinition.ofLong("UserCpuNanos"),
        chunks[15].asWritableLongChunk().add(nugget.getUserCpuNanos());

        // ColumnDefinition.ofLong("FreeMemory"),
        chunks[16].asWritableLongChunk().add(nugget.getEndFreeMemory());

        // ColumnDefinition.ofLong("TotalMemory"),
        chunks[17].asWritableLongChunk().add(nugget.getEndTotalMemory());

        // ColumnDefinition.ofLong("FreeMemoryChange"),
        chunks[18].asWritableLongChunk().add(nugget.getDiffFreeMemory());

        // ColumnDefinition.ofLong("TotalMemoryChange"),
        chunks[19].asWritableLongChunk().add(nugget.getDiffTotalMemory());

        // ColumnDefinition.ofLong("Collections")
        chunks[20].asWritableLongChunk().add(nugget.getDiffCollections());

        // ColumnDefinition.ofLong("CollectionTimeNanos"),
        chunks[21].asWritableLongChunk().add(nugget.getDiffCollectionTimeNanos());

        // ColumnDefinition.ofLong("AllocatedBytes"),
        chunks[22].asWritableLongChunk().add(nugget.getAllocatedBytes());

        // ColumnDefinition.ofLong("PoolAllocatedBytes"),
        chunks[23].asWritableLongChunk().add(nugget.getPoolAllocatedBytes());

        // ColumnDefinition.ofLong("InputSizeLong"),
        chunks[24].asWritableLongChunk().add(nugget.getInputSize());

        // ColumnDefinition.ofBoolean("WasInterrupted")
        chunks[25].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.wasInterrupted()));

        // ColumnDefinition.ofString("AuthContext")
        chunks[26].<String>asWritableObjectChunk().add(Objects.toString(nugget.getAuthContext()));

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
