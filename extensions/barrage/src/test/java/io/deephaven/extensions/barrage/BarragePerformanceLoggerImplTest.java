//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.extensions.barrage.BarragePerformanceLog.SnapshotMetricsHelper;
import io.deephaven.extensions.barrage.BarrageSubscriptionPerformanceLogger.StatType;
import io.deephaven.time.DateTimeUtils;
import org.HdrHistogram.Histogram;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the logger implementations hand raw, unscaled values to the integrator sinks, and that the in-memory
 * blink tables they own carry the matching raw-unit schema.
 * <p>
 * The values are deliberately chosen to be far from any millisecond or megabit boundary, so a reintroduced
 * {@code / 1e6} or {@code * 8} would truncate to a visibly different number rather than coincidentally agreeing.
 */
public class BarragePerformanceLoggerImplTest extends RefreshingTableTestCase {

    private static final String TABLE_ID = "cafe1234";
    private static final String TABLE_KEY = "MyTable";

    private static final class SubscriptionEntry {
        private final String tableId;
        private final String tableKey;
        private final String statType;
        private final long timestampEpochNanos;
        private final long count;
        private final long p50;
        private final long max;

        private SubscriptionEntry(final String tableId, final String tableKey, final String statType,
                final long timestampEpochNanos, final Histogram hist) {
            this.tableId = tableId;
            this.tableKey = tableKey;
            this.statType = statType;
            this.timestampEpochNanos = timestampEpochNanos;
            this.count = hist.getTotalCount();
            this.p50 = hist.getValueAtPercentile(50);
            this.max = hist.getMaxValue();
        }
    }

    private static final class RecordingSubscriptionSink implements BarrageSubscriptionPerformanceSink {
        private final List<SubscriptionEntry> entries = new ArrayList<>();

        @Override
        public void log(final String tableId, final String tableKey, final String statType,
                final long timestampEpochNanos, final Histogram hist) {
            entries.add(new SubscriptionEntry(tableId, tableKey, statType, timestampEpochNanos, hist));
        }
    }

    private static final class SnapshotEntry {
        private final String tableId;
        private final String tableKey;
        private final long requestTimeEpochNanos;
        private final long queueNanos;
        private final long snapshotNanos;
        private final long writeNanos;
        private final long bytesWritten;

        private SnapshotEntry(final String tableId, final String tableKey, final long requestTimeEpochNanos,
                final long queueNanos, final long snapshotNanos, final long writeNanos, final long bytesWritten) {
            this.tableId = tableId;
            this.tableKey = tableKey;
            this.requestTimeEpochNanos = requestTimeEpochNanos;
            this.queueNanos = queueNanos;
            this.snapshotNanos = snapshotNanos;
            this.writeNanos = writeNanos;
            this.bytesWritten = bytesWritten;
        }
    }

    private static final class RecordingSnapshotSink implements BarrageSnapshotPerformanceSink {
        private final List<SnapshotEntry> entries = new ArrayList<>();

        @Override
        public void log(final String tableId, final String tableKey, final long requestTimeEpochNanos,
                final long queueNanos, final long snapshotNanos, final long writeNanos, final long bytesWritten) {
            entries.add(new SnapshotEntry(tableId, tableKey, requestTimeEpochNanos, queueNanos, snapshotNanos,
                    writeNanos, bytesWritten));
        }
    }

    public void testSubscriptionSinkReceivesRawNanos() {
        final RecordingSubscriptionSink sink = new RecordingSubscriptionSink();
        final BarrageSubscriptionPerformanceLoggerImpl impl = new BarrageSubscriptionPerformanceLoggerImpl(sink);

        // Three microseconds and change; scaling to millis would round this to zero.
        final Histogram hist = new Histogram(3);
        hist.recordValue(3_123L);
        hist.recordValue(9_876L);

        final Instant now = DateTimeUtils.epochNanosToInstant(1_700_000_000_123_456_789L);
        impl.log(TABLE_ID, TABLE_KEY, StatType.WRITE_NANOS, now, hist);

        assertThat(sink.entries).hasSize(1);
        final SubscriptionEntry entry = sink.entries.get(0);
        assertThat(entry.tableId).isEqualTo(TABLE_ID);
        assertThat(entry.tableKey).isEqualTo(TABLE_KEY);
        assertThat(entry.statType).isEqualTo("WriteNanos");
        assertThat(entry.timestampEpochNanos).isEqualTo(1_700_000_000_123_456_789L);
        assertThat(entry.count).isEqualTo(2L);
        // HdrHistogram quantizes to 3 significant figures, so compare against the histogram's own view rather than
        // the recorded value; the point here is that nothing divided it.
        assertThat(entry.p50).isEqualTo(hist.getValueAtPercentile(50));
        assertThat(entry.max).isEqualTo(hist.getMaxValue());
        assertThat(entry.max).isGreaterThan(9_000L);
    }

    public void testSubscriptionBlinkTableUsesRawLongUnits() {
        final BarrageSubscriptionPerformanceLoggerImpl impl =
                new BarrageSubscriptionPerformanceLoggerImpl(BarrageSubscriptionPerformanceSink.Noop.INSTANCE);
        assertThat(impl.blinkTable().getDefinition())
                .isEqualTo(BarrageSubscriptionPerformanceStreamPublisher.definition());
    }

    public void testSubscriptionSinkFailureIsIsolated() {
        final BarrageSubscriptionPerformanceLoggerImpl impl =
                new BarrageSubscriptionPerformanceLoggerImpl((tableId, tableKey, statType, timestamp, hist) -> {
                    throw new IllegalStateException("boom");
                });

        final Histogram hist = new Histogram(3);
        hist.recordValue(1L);

        // A defective sink must not be able to disrupt the caller; the first failure disables further attempts.
        impl.log(TABLE_ID, TABLE_KEY, StatType.WRITE_NANOS, Instant.EPOCH, hist);
        impl.log(TABLE_ID, TABLE_KEY, StatType.WRITE_NANOS, Instant.EPOCH, hist);
    }

    public void testSnapshotSinkReceivesRawNanosAndBytes() {
        final RecordingSnapshotSink sink = new RecordingSnapshotSink();
        final BarrageSnapshotPerformanceLoggerImpl impl = new BarrageSnapshotPerformanceLoggerImpl(sink);

        final SnapshotMetricsHelper helper = new SnapshotMetricsHelper();
        helper.tableId = TABLE_ID;
        helper.tableKey = TABLE_KEY;
        helper.queueNanos = 4_321L;
        helper.snapshotNanos = 87_654L;

        impl.log(helper, 12_345L, 4_096L);

        assertThat(sink.entries).hasSize(1);
        final SnapshotEntry entry = sink.entries.get(0);
        assertThat(entry.tableId).isEqualTo(TABLE_ID);
        assertThat(entry.tableKey).isEqualTo(TABLE_KEY);
        assertThat(entry.requestTimeEpochNanos).isEqualTo(DateTimeUtils.epochNanos(helper.requestTm));
        assertThat(entry.queueNanos).isEqualTo(4_321L);
        assertThat(entry.snapshotNanos).isEqualTo(87_654L);
        assertThat(entry.writeNanos).isEqualTo(12_345L);
        // A byte count, not the bit count the table used to publish as megabits.
        assertThat(entry.bytesWritten).isEqualTo(4_096L);
    }

    public void testSnapshotBlinkTableUsesRawLongUnits() {
        final BarrageSnapshotPerformanceLoggerImpl impl =
                new BarrageSnapshotPerformanceLoggerImpl(BarrageSnapshotPerformanceSink.Noop.INSTANCE);
        assertThat(impl.blinkTable().getDefinition())
                .isEqualTo(BarrageSnapshotPerformanceStreamPublisher.definition());
    }
}
