//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.app;

import com.sun.management.GcInfo;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.util.PercentileOutput;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

final class GcPoolsPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("Id"),
            ColumnDefinition.ofString("Name"),
            ColumnDefinition.ofLong("BeforeInit"),
            ColumnDefinition.ofLong("BeforeUsed"),
            ColumnDefinition.ofLong("BeforeCommitted"),
            ColumnDefinition.ofLong("BeforeMax"),
            ColumnDefinition.ofLong("AfterInit"),
            ColumnDefinition.ofLong("AfterUsed"),
            ColumnDefinition.ofLong("AfterCommitted"),
            ColumnDefinition.ofLong("AfterMax"));

    private static final int INITIAL_CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    public static Table stats(Table pools) {
        return pools
                .updateView(
                        "UsedReclaimed=BeforeUsed-AfterUsed",
                        "CommittedReclaimed=BeforeCommitted-AfterCommitted")
                .aggBy(Arrays.asList(
                        Aggregation.AggCount("Count"),
                        Aggregation.AggSum(
                                "UsedReclaimedTotal=UsedReclaimed",
                                "CommittedReclaimedTotal=CommittedReclaimed"),
                        Aggregation.AggAvg(
                                "UsedReclaimedAvg=UsedReclaimed",
                                "CommittedReclaimedAvg=CommittedReclaimed"),
                        Aggregation.AggApproxPct("UsedReclaimed",
                                PercentileOutput.of(0.5, "UsedReclaimedP_50")),
                        Aggregation.AggApproxPct("CommittedReclaimed",
                                PercentileOutput.of(0.5, "CommittedReclaimedP_50")),
                        Aggregation.AggLast(
                                "BeforeInit",
                                "BeforeUsed",
                                "BeforeCommitted",
                                "BeforeMax",
                                "AfterInit",
                                "AfterUsed",
                                "AfterCommitted",
                                "AfterMax")),
                        "Name");
    }

    private int chunkSize;
    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;
    private boolean isFirst;

    GcPoolsPublisher() {
        chunkSize = INITIAL_CHUNK_SIZE;
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, chunkSize);
        isFirst = true;
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(GcInfo info) {
        final Map<String, MemoryUsage> afterMap = info.getMemoryUsageAfterGc();
        final int poolSize = afterMap.size();
        final int initialSize = chunks[0].size();
        // Note: we don't expect this to trigger since we are checking the size after the add, but it's an extra
        // safety if a JVM implementation adds additional pools types during runtime.
        if (initialSize + poolSize >= chunkSize) {
            maybeFlushAndGrow(initialSize, poolSize);
        }
        for (Entry<String, MemoryUsage> e : info.getMemoryUsageBeforeGc().entrySet()) {
            final String poolName = e.getKey();
            final MemoryUsage before = e.getValue();
            final MemoryUsage after = afterMap.get(poolName);
            if (!isFirst && equals(before, after)) {
                continue;
            }
            // Note: there's potential for possible further optimization by having a typed field per column (ie, doing
            // the casts only once per flush). Also, potential to use `set` with our own size field instead of `add`.
            chunks[0].asWritableLongChunk().add(info.getId());
            chunks[1].<String>asWritableObjectChunk().add(poolName);

            chunks[2].asWritableLongChunk().add(negativeOneToNullLong(before.getInit()));
            chunks[3].asWritableLongChunk().add(before.getUsed());
            chunks[4].asWritableLongChunk().add(before.getCommitted());
            chunks[5].asWritableLongChunk().add(negativeOneToNullLong(before.getMax()));

            chunks[6].asWritableLongChunk().add(negativeOneToNullLong(after.getInit()));
            chunks[7].asWritableLongChunk().add(after.getUsed());
            chunks[8].asWritableLongChunk().add(after.getCommitted());
            chunks[9].asWritableLongChunk().add(negativeOneToNullLong(after.getMax()));
        }
        isFirst = false;
        if (initialSize + 2 * poolSize >= chunkSize) {
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

    private void maybeFlushAndGrow(int initialSize, int poolSize) {
        if (initialSize > 0) {
            consumer.accept(chunks);
        }
        chunkSize = Math.max(chunkSize, poolSize);
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, chunkSize);
    }

    private void flushInternal() {
        consumer.accept(chunks);
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, chunkSize);
    }

    public void acceptFailure(Throwable e) {
        consumer.acceptFailure(e);
    }

    @Override
    public void shutdown() {}

    private static boolean equals(MemoryUsage before, MemoryUsage after) {
        return before.getUsed() == after.getUsed()
                && before.getCommitted() == after.getCommitted()
                && before.getMax() == after.getMax()
                && before.getInit() == after.getInit();
    }

    private static long negativeOneToNullLong(long x) {
        return x == -1 ? QueryConstants.NULL_LONG : x;
    }
}
