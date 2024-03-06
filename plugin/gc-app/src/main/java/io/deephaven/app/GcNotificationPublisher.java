//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.app;

import com.sun.management.GarbageCollectionNotificationInfo;
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
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Arrays;
import java.util.Objects;

final class GcNotificationPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("Id"),
            ColumnDefinition.ofTime("Start"),
            ColumnDefinition.ofTime("End"),
            ColumnDefinition.ofString("GcName"),
            ColumnDefinition.ofString("GcAction"),
            ColumnDefinition.ofString("GcCause"),
            ColumnDefinition.ofLong("Reclaimed"));
    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    public static Table stats(Table notificationInfo) {
        // Would be great to have a aggBy Exponential Decayed Sum so we could have "GC rate over the last 1, 5, 15 min"
        // Could further extend with updateBy Exponential Decayed Sum and graphs of rates.
        return notificationInfo
                .updateView("Duration=(End-Start)/1000000000")
                .aggBy(Arrays.asList(
                        Aggregation.AggCount("Count"),
                        Aggregation.AggSum("DurationTotal=Duration", "ReclaimedTotal=Reclaimed"),
                        Aggregation.AggMax("DurationMax=Duration"),
                        Aggregation.AggAvg("DurationAvg=Duration", "ReclaimedAvg=Reclaimed"),
                        Aggregation.AggApproxPct("Reclaimed",
                                PercentileOutput.of(0.5, "ReclaimedP_50")),
                        Aggregation.AggApproxPct("Duration",
                                PercentileOutput.of(0.5, "DurationP_50"),
                                PercentileOutput.of(0.9, "DurationP_90"),
                                PercentileOutput.of(0.95, "DurationP_95"),
                                PercentileOutput.of(0.99, "DurationP_99")),
                        Aggregation.AggLast(
                                "LastId=Id",
                                "LastStart=Start",
                                "LastEnd=End",
                                "LastReclaimed=Reclaimed")),
                        "GcName", "GcAction", "GcCause");
    }

    private final long vmStartMillis;
    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    GcNotificationPublisher() {
        this.vmStartMillis = ManagementFactory.getRuntimeMXBean().getStartTime();
        chunks = StreamChunkUtils.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    public synchronized void add(GarbageCollectionNotificationInfo gcNotification) {
        final GcInfo gcInfo = gcNotification.getGcInfo();

        // Note: there's potential for possible further optimization by having a typed field per column (ie, doing the
        // casts only once per flush). Also, potential to use `set` with our own size field instead of `add`.
        chunks[0].asWritableLongChunk().add(gcInfo.getId());
        chunks[1].asWritableLongChunk().add(DateTimeUtils.millisToNanos(vmStartMillis + gcInfo.getStartTime()));
        chunks[2].asWritableLongChunk().add(DateTimeUtils.millisToNanos(vmStartMillis + gcInfo.getEndTime()));

        // Note: there may be value in interning these strings for ring tables if we find they are causing a lot of
        // extra memory usage.
        chunks[3].<String>asWritableObjectChunk().add(gcNotification.getGcName());
        chunks[4].<String>asWritableObjectChunk().add(gcNotification.getGcAction());
        chunks[5].<String>asWritableObjectChunk().add(gcNotification.getGcCause());

        // This is a bit of a de-normalization - arguably, it could be computed by joining against the "pools" table.
        // But this is a very useful summary value, and easy for use to provide here for more convenience.
        final long usedBefore = gcNotification.getGcInfo().getMemoryUsageBeforeGc().values().stream()
                .mapToLong(MemoryUsage::getUsed).sum();
        final long usedAfter = gcNotification.getGcInfo().getMemoryUsageAfterGc().values().stream()
                .mapToLong(MemoryUsage::getUsed).sum();
        // Note: reclaimed *can* be negative
        final long reclaimed = usedBefore - usedAfter;
        chunks[6].asWritableLongChunk().add(reclaimed);

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
