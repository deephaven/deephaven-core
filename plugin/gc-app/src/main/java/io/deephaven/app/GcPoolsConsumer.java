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
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamToTableAdapter;
import io.deephaven.util.QueryConstants;

import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

final class GcPoolsConsumer {

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

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    // Let's be conservative since we don't know exactly how many pools there may be.
    private static final int POOL_SIZE_UPPER_BOUND = 128;

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

    private final StreamConsumer consumer;
    private WritableChunk<Values>[] chunks;
    private boolean isFirst;

    GcPoolsConsumer(StreamConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
        // noinspection unchecked
        chunks = StreamToTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
        isFirst = true;
    }

    public synchronized void add(GcInfo info) {
        final Map<String, MemoryUsage> afterMap = info.getMemoryUsageAfterGc();
        for (Entry<String, MemoryUsage> e : info.getMemoryUsageBeforeGc().entrySet()) {
            final String poolName = e.getKey();
            final MemoryUsage before = e.getValue();
            final MemoryUsage after = afterMap.get(poolName);
            if (!isFirst && equals(before, after)) {
                continue;
            }
            chunks[0].asWritableLongChunk().add(info.getId());
            chunks[1].<String>asWritableObjectChunk().add(poolName.intern());

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
        if (chunks[0].size() + POOL_SIZE_UPPER_BOUND >= CHUNK_SIZE) {
            flushInternal();
        }
    }

    public synchronized void flush() {
        if (chunks[0].size() == 0) {
            return;
        }
        flushInternal();
    }

    private void flushInternal() {
        consumer.accept(chunks);
        // noinspection unchecked
        chunks = StreamToTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    public void acceptFailure(Throwable e) {
        consumer.acceptFailure(e);
    }

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
