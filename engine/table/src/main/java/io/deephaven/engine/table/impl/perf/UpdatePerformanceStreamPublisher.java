//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker.IntervalLevelDetails;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.util.HeapSize;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class UpdatePerformanceStreamPublisher implements StreamPublisher {
    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("EntryId"),
            ColumnDefinition.ofLong("EvaluationNumber"),
            ColumnDefinition.ofInt("OperationNumber"),
            ColumnDefinition.ofString("EntryDescription"),
            ColumnDefinition.ofString("EntryCallerLine"),
            ColumnDefinition.ofTime("IntervalStartTime"),
            ColumnDefinition.ofTime("IntervalEndTime"),
            ColumnDefinition.ofLong("UsageNanos"),
            ColumnDefinition.ofLong("CpuNanos"),
            ColumnDefinition.ofLong("UserCpuNanos"),
            ColumnDefinition.ofLong("RowsAdded"),
            ColumnDefinition.ofLong("RowsRemoved"),
            ColumnDefinition.ofLong("RowsModified"),
            ColumnDefinition.ofLong("RowsShifted"),
            ColumnDefinition.ofLong("InvocationCount"),
            ColumnDefinition.ofLong("MinFreeMemory"),
            ColumnDefinition.ofLong("MaxTotalMemory"),
            ColumnDefinition.ofLong("Collections"),
            ColumnDefinition.ofLong("CollectionTimeNanos"),
            ColumnDefinition.ofLong("AllocatedBytes"),
            ColumnDefinition.ofLong("PoolAllocatedBytes"),
            ColumnDefinition.ofLong("DataReadNanos"),
            ColumnDefinition.ofLong("DataReadCount"),
            ColumnDefinition.ofLong("DataReadBytes"),
            ColumnDefinition.ofLong("MetadataReadNanos"),
            ColumnDefinition.ofLong("MetadataReadCount"),
            ColumnDefinition.ofString("AuthContext"),
            ColumnDefinition.ofString("UpdateGraph"),
            ColumnDefinition.ofLong("WorkerHeapSize"));

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;
    private final long heapSize;

    public UpdatePerformanceStreamPublisher() {
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

    public synchronized void add(IntervalLevelDetails intervalLevelDetails, PerformanceEntry performanceEntry) {
        int ci = 0;
        // ColumnDefinition.ofLong("EntryId"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getId());
        // ColumnDefinition.ofLong("EvaluationNumber"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getEvaluationNumber());
        // ColumnDefinition.ofInt("OperationNumber"),
        chunks[ci++].asWritableIntChunk().add(performanceEntry.getOperationNumber());
        // ColumnDefinition.ofString("EntryDescription"),
        chunks[ci++].<String>asWritableObjectChunk().add(performanceEntry.getDescription());
        // ColumnDefinition.ofString("EntryCallerLine"),
        chunks[ci++].<String>asWritableObjectChunk().add(performanceEntry.getCallerLine());
        // ColumnDefinition.ofTime("IntervalStartTime"),
        chunks[ci++].asWritableLongChunk().add(intervalLevelDetails.getIntervalStartTimeEpochNanos());
        // ColumnDefinition.ofTime("IntervalEndTime"),
        chunks[ci++].asWritableLongChunk().add(intervalLevelDetails.getIntervalEndTimeEpochNanos());
        // ColumnDefinition.ofLong("UsageNanos"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getUsageNanos());
        // ColumnDefinition.ofLong("CpuNanos"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getCpuNanos());
        // ColumnDefinition.ofLong("UserCpuNanos"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getUserCpuNanos());
        // ColumnDefinition.ofLong("RowsAdded"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getRowsAdded());
        // ColumnDefinition.ofLong("RowsRemoved"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getRowsRemoved());
        // ColumnDefinition.ofLong("RowsModified"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getRowsModified());
        // ColumnDefinition.ofLong("RowsShifted"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getRowsShifted());
        // ColumnDefinition.ofLong("InvocationCount"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getInvocationCount());
        // ColumnDefinition.ofLong("MinFreeMemory"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getMinFreeMemory());
        // ColumnDefinition.ofLong("MaxTotalMemory"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getMaxTotalMemory());
        // ColumnDefinition.ofLong("Collections"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getCollections());
        // ColumnDefinition.ofLong("CollectionTimeNanos"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getCollectionTimeNanos());
        // ColumnDefinition.ofLong("AllocatedBytes"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getAllocatedBytes());
        // ColumnDefinition.ofLong("PoolAllocatedBytes"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getPoolAllocatedBytes());
        // ColumnDefinition.ofLong("DataReadNanos"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getDataReadNanos());
        // ColumnDefinition.ofLong("DataReadCount"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getDataReadCount());
        // ColumnDefinition.ofLong("DataReadBytes"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getDataReadBytes());
        // ColumnDefinition.ofLong("MetadataReadNanos"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getMetadataReadNanos());
        // ColumnDefinition.ofLong("MetadataReadCount"),
        chunks[ci++].asWritableLongChunk().add(performanceEntry.getMetadataReadCount());
        // ColumnDefinition.ofString("AuthContext"),
        chunks[ci++].<String>asWritableObjectChunk().add(Objects.toString(performanceEntry.getAuthContext()));
        // ColumnDefinition.ofString("UpdateGraph"));
        chunks[ci++].<String>asWritableObjectChunk().add(Objects.toString(performanceEntry.getUpdateGraphName()));
        // ColumnDefinition.ofLong("WorkerHeapSize")
        chunks[ci++].asWritableLongChunk().add(heapSize);

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
    public void shutdown() {
        // this is allocating chunks just to free them; but if flush is overridden we would like the override behavior
        // to occur
        flush();
        SafeCloseableArray.close(chunks);
        chunks = null;
    }
}
