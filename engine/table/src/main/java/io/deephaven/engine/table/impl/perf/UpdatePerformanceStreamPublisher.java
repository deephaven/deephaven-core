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
import io.deephaven.engine.table.impl.util.EngineMetrics;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class UpdatePerformanceStreamPublisher implements StreamPublisher {
    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("ProcessUniqueId"),
            ColumnDefinition.ofInt("EntryId"),
            ColumnDefinition.ofInt("EvaluationNumber"),
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
        chunks[0].<String>asWritableObjectChunk().add(EngineMetrics.getProcessInfo().getId().value());
        chunks[1].asWritableIntChunk().add(performanceEntry.getId());
        chunks[2].asWritableIntChunk().add(performanceEntry.getEvaluationNumber());
        chunks[3].asWritableIntChunk().add(performanceEntry.getOperationNumber());
        chunks[4].<String>asWritableObjectChunk().add(performanceEntry.getDescription());
        chunks[5].<String>asWritableObjectChunk().add(performanceEntry.getCallerLine());
        chunks[6].asWritableLongChunk()
                .add(DateTimeUtils.millisToNanos(intervalLevelDetails.getIntervalStartTimeMillis()));
        chunks[7].asWritableLongChunk()
                .add(DateTimeUtils.millisToNanos(intervalLevelDetails.getIntervalEndTimeMillis()));
        chunks[8].asWritableLongChunk().add(intervalLevelDetails.getIntervalDurationNanos());
        chunks[9].asWritableLongChunk().add(performanceEntry.getIntervalUsageNanos());
        chunks[10].asWritableLongChunk().add(performanceEntry.getIntervalCpuNanos());
        chunks[11].asWritableLongChunk().add(performanceEntry.getIntervalUserCpuNanos());
        chunks[12].asWritableLongChunk().add(performanceEntry.getIntervalAdded());
        chunks[13].asWritableLongChunk().add(performanceEntry.getIntervalRemoved());
        chunks[14].asWritableLongChunk().add(performanceEntry.getIntervalModified());
        chunks[15].asWritableLongChunk().add(performanceEntry.getIntervalShifted());
        chunks[16].asWritableLongChunk().add(performanceEntry.getIntervalInvocationCount());
        chunks[17].asWritableLongChunk().add(performanceEntry.getMinFreeMemory());
        chunks[18].asWritableLongChunk().add(performanceEntry.getMaxTotalMemory());
        chunks[19].asWritableLongChunk().add(performanceEntry.getCollections());
        chunks[20].asWritableLongChunk().add(performanceEntry.getCollectionTimeNanos());
        chunks[21].asWritableLongChunk().add(performanceEntry.getIntervalAllocatedBytes());
        chunks[22].asWritableLongChunk().add(performanceEntry.getIntervalPoolAllocatedBytes());
        chunks[23].<String>asWritableObjectChunk().add(Objects.toString(performanceEntry.getAuthContext()));
        chunks[24].<String>asWritableObjectChunk().add(Objects.toString(performanceEntry.getUpdateGraphName()));
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
