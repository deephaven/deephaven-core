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
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

class QueryOperationPerformanceStreamPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("ProcessUniqueId"),
            ColumnDefinition.ofInt("EvaluationNumber"),
            ColumnDefinition.ofInt("ParentEvaluationNumber"),
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

        chunks[0].<String>asWritableObjectChunk().add(id);
        chunks[1].asWritableIntChunk().add(nugget.getEvaluationNumber());
        chunks[2].asWritableIntChunk().add(nugget.getParentEvaluationNumber());
        chunks[3].asWritableIntChunk().add(nugget.getOperationNumber());
        chunks[4].asWritableIntChunk().add(nugget.getParentOperationNumber());
        chunks[5].asWritableIntChunk().add(nugget.getDepth());
        chunks[6].<String>asWritableObjectChunk().add(nugget.getName());
        chunks[7].<String>asWritableObjectChunk().add(nugget.getCallerLine());
        chunks[8].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.isQueryLevel()));
        chunks[9].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.isTopLevel()));
        chunks[10].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.getName().startsWith("Compile:")));
        chunks[11].asWritableLongChunk().add(nugget.getStartClockTime());
        chunks[12].asWritableLongChunk().add(nugget.getEndClockTime());
        chunks[13].asWritableLongChunk().add(nugget.getTotalTimeNanos());
        chunks[14].asWritableLongChunk().add(nugget.getCpuNanos());
        chunks[15].asWritableLongChunk().add(nugget.getUserCpuNanos());
        chunks[16].asWritableLongChunk().add(nugget.getEndFreeMemory());
        chunks[17].asWritableLongChunk().add(nugget.getEndTotalMemory());
        chunks[18].asWritableLongChunk().add(nugget.getDiffFreeMemory());
        chunks[19].asWritableLongChunk().add(nugget.getDiffTotalMemory());
        chunks[20].asWritableLongChunk().add(nugget.getDiffCollectionTimeNanos());
        chunks[21].asWritableLongChunk().add(nugget.getAllocatedBytes());
        chunks[22].asWritableLongChunk().add(nugget.getPoolAllocatedBytes());
        chunks[23].asWritableByteChunk().add(BooleanUtils.booleanAsByte(nugget.wasInterrupted()));
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
