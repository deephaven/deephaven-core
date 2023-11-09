/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.perf;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker.IntervalLevelDetails;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.time.DateTimeUtils;
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
            ColumnDefinition.ofLong("IntervalDurationNanos"),
            ColumnDefinition.ofLong("EntryIntervalUsage"),
            ColumnDefinition.ofLong("EntryIntervalCpuNanos"),
            ColumnDefinition.ofLong("EntryIntervalUserCpuNanos"),
            ColumnDefinition.ofLong("EntryIntervalAdded"),
            ColumnDefinition.ofLong("EntryIntervalRemoved"),
            ColumnDefinition.ofLong("EntryIntervalModified"),
            ColumnDefinition.ofLong("EntryIntervalShifted"),
            ColumnDefinition.ofLong("EntryIntervalInvocationCount"),
            ColumnDefinition.ofLong("MinFreeMemory"),
            ColumnDefinition.ofLong("MaxTotalMemory"),
            ColumnDefinition.ofLong("Collections"),
            ColumnDefinition.ofLong("CollectionTimeNanos"),
            ColumnDefinition.ofLong("EntryIntervalAllocatedBytes"),
            ColumnDefinition.ofLong("EntryIntervalPoolAllocatedBytes"),
            ColumnDefinition.ofString("AuthContext"),
            ColumnDefinition.ofString("UpdateGraph"));

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    public UpdatePerformanceStreamPublisher() {
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(IntervalLevelDetails intervalLevelDetails, PerformanceEntry performanceEntry) {
        // ColumnDefinition.ofInt("EntryId"),
        chunks[0].asWritableLongChunk().add(performanceEntry.getId());
        // ColumnDefinition.ofLong("EvaluationNumber"),
        chunks[1].asWritableLongChunk().add(performanceEntry.getEvaluationNumber());
        // ColumnDefinition.ofInt("OperationNumber"),
        chunks[2].asWritableIntChunk().add(performanceEntry.getOperationNumber());
        // ColumnDefinition.ofString("EntryDescription"),
        chunks[3].<String>asWritableObjectChunk().add(performanceEntry.getDescription());
        // ColumnDefinition.ofString("EntryCallerLine"),
        chunks[4].<String>asWritableObjectChunk().add(performanceEntry.getCallerLine());
        // ColumnDefinition.ofTime("IntervalStartTime"),
        chunks[5].asWritableLongChunk()
                .add(DateTimeUtils.millisToNanos(intervalLevelDetails.getIntervalStartTimeMillis()));
        // ColumnDefinition.ofTime("IntervalEndTime"),
        chunks[6].asWritableLongChunk()
                .add(DateTimeUtils.millisToNanos(intervalLevelDetails.getIntervalEndTimeMillis()));
        // ColumnDefinition.ofLong("IntervalDurationNanos"),
        chunks[7].asWritableLongChunk().add(intervalLevelDetails.getIntervalDurationNanos());
        // ColumnDefinition.ofLong("EntryIntervalUsage"),
        chunks[8].asWritableLongChunk().add(performanceEntry.getUsageNanos());
        // ColumnDefinition.ofLong("EntryIntervalCpuNanos"),
        chunks[9].asWritableLongChunk().add(performanceEntry.getCpuNanos());
        // ColumnDefinition.ofLong("EntryIntervalUserCpuNanos"),
        chunks[10].asWritableLongChunk().add(performanceEntry.getUserCpuNanos());
        // ColumnDefinition.ofLong("EntryIntervalAdded"),
        chunks[11].asWritableLongChunk().add(performanceEntry.getIntervalAdded());
        // ColumnDefinition.ofLong("EntryIntervalRemoved"),
        chunks[12].asWritableLongChunk().add(performanceEntry.getIntervalRemoved());
        // ColumnDefinition.ofLong("EntryIntervalModified"),
        chunks[13].asWritableLongChunk().add(performanceEntry.getIntervalModified());
        // ColumnDefinition.ofLong("EntryIntervalShifted"),
        chunks[14].asWritableLongChunk().add(performanceEntry.getIntervalShifted());
        // ColumnDefinition.ofLong("EntryIntervalInvocationCount"),
        chunks[15].asWritableLongChunk().add(performanceEntry.getIntervalInvocationCount());
        // ColumnDefinition.ofLong("MinFreeMemory"),
        chunks[16].asWritableLongChunk().add(performanceEntry.getMinFreeMemory());
        // ColumnDefinition.ofLong("MaxTotalMemory"),
        chunks[17].asWritableLongChunk().add(performanceEntry.getMaxTotalMemory());
        // ColumnDefinition.ofLong("Collections"),
        chunks[18].asWritableLongChunk().add(performanceEntry.getCollections());
        // ColumnDefinition.ofLong("CollectionTimeNanos"),
        chunks[19].asWritableLongChunk().add(performanceEntry.getCollectionTimeNanos());
        // ColumnDefinition.ofLong("EntryIntervalAllocatedBytes"),
        chunks[20].asWritableLongChunk().add(performanceEntry.getAllocatedBytes());
        // ColumnDefinition.ofLong("EntryIntervalPoolAllocatedBytes"),
        chunks[21].asWritableLongChunk().add(performanceEntry.getPoolAllocatedBytes());
        // ColumnDefinition.ofString("AuthContext"),
        chunks[22].<String>asWritableObjectChunk().add(Objects.toString(performanceEntry.getAuthContext()));
        // ColumnDefinition.ofString("UpdateGraph"));
        chunks[23].<String>asWritableObjectChunk().add(Objects.toString(performanceEntry.getUpdateGraphName()));

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
