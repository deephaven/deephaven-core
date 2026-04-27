//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
            ColumnDefinition.ofLong("DataReadNanos"),
            ColumnDefinition.ofLong("DataReadCount"),
            ColumnDefinition.ofLong("DataReadBytes"),
            ColumnDefinition.ofLong("MetadataReadNanos"),
            ColumnDefinition.ofLong("MetadataReadCount"),
            ColumnDefinition.ofBoolean("WasInterrupted"),
            ColumnDefinition.ofString("Exception"),
            ColumnDefinition.ofString("AuthContext"),
            ColumnDefinition.ofLong("WorkerHeapSize"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;
    private final long heapSize;

    QueryPerformanceStreamPublisher() {
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
        heapSize = HeapSize.getMaximumHeapSizeBytes();
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
        int chunkIdx = 0;

        // ColumnDefinition.ofLong("EvaluationNumber")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getEvaluationNumber());

        // ColumnDefinition.ofLong("ParentEvaluationNumber")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getParentEvaluationNumber());

        // ColumnDefinition.ofString("SessionId")
        chunks[chunkIdx++].<String>asWritableObjectChunk().add(nugget.getSessionId());

        // ColumnDefinition.ofString("Description")
        chunks[chunkIdx++].<String>asWritableObjectChunk().add(nugget.getDescription());

        // ColumnDefinition.ofTime("StartTime");
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getStartClockEpochNanos());

        // ColumnDefinition.ofTime("EndTime")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getEndClockEpochNanos());

        // ColumnDefinition.ofLong("UsageNanos")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getUsageNanos());

        // ColumnDefinition.ofLong("CpuNanos")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getCpuNanos());

        // ColumnDefinition.ofLong("UserCpuNanos")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getUserCpuNanos());

        // ColumnDefinition.ofLong("FreeMemory")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getEndFreeMemory());

        // ColumnDefinition.ofLong("TotalMemory")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getEndTotalMemory());

        // ColumnDefinition.ofLong("FreeMemoryChange")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getDiffFreeMemory());

        // ColumnDefinition.ofLong("TotalMemoryChange")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getDiffTotalMemory());

        // ColumnDefinition.ofLong("Collections")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getDiffCollections());

        // ColumnDefinition.ofLong("CollectionTimeNanos")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getDiffCollectionTimeNanos());

        // ColumnDefinition.ofLong("AllocatedBytes")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getAllocatedBytes());

        // ColumnDefinition.ofLong("PoolAllocatedBytes")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getPoolAllocatedBytes());

        // ColumnDefinition.ofLong("DataReadNanos")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getDataReadNanos());

        // ColumnDefinition.ofLong("DataReadCount")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getDataReadCount());

        // ColumnDefinition.ofLong("DataReadBytes")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getDataReadBytes());

        // ColumnDefinition.ofLong("MetadataReadNanos")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getMetadataReadNanos());

        // ColumnDefinition.ofLong("MetadataReadCount")
        chunks[chunkIdx++].asWritableLongChunk().add(nugget.getMetadataReadCount());

        // ColumnDefinition.ofBoolean("WasInterrupted")
        chunks[chunkIdx++].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.wasInterrupted()));

        // ColumnDefinition.ofString("Exception")
        chunks[chunkIdx++].<String>asWritableObjectChunk().add(exception == null ? null : exception.getMessage());

        // ColumnDefinition.ofString("AuthContext")
        chunks[chunkIdx++].<String>asWritableObjectChunk().add(Objects.toString(nugget.getAuthContext()));

        // ColumnDefinition.ofLong("WorkerHeapSize")
        chunks[20].asWritableLongChunk().add(heapSize);

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
