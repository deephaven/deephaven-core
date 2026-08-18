//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.time.DateTimeUtils;
import org.HdrHistogram.Histogram;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Publishes barrage subscription statistics to the in-memory blink table, and forwards each entry to the
 * {@link BarrageSubscriptionPerformanceSink} provided by {@link BarrageTableLoggers}.
 */
class BarrageSubscriptionPerformanceLoggerImpl implements BarrageSubscriptionPerformanceLogger {
    private static final Logger log = LoggerFactory.getLogger(BarrageSubscriptionPerformanceLoggerImpl.class);

    private final BarrageSubscriptionPerformanceSink sink;
    private final BarrageSubscriptionPerformanceStreamPublisher publisher;
    // Keep, may eventually want to manage / close
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    private boolean encounteredError = false;

    public BarrageSubscriptionPerformanceLoggerImpl(final BarrageSubscriptionPerformanceSink sink) {
        this.sink = Objects.requireNonNull(sink);
        publisher = new BarrageSubscriptionPerformanceStreamPublisher();
        adapter = new StreamToBlinkTableAdapter(
                BarrageSubscriptionPerformanceStreamPublisher.definition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                BarrageSubscriptionPerformanceLoggerImpl.class.getName(),
                Map.of(
                        Table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE,
                        BarrageSubscriptionPerformanceLogger.getDefaultTableName()));
        blink = adapter.table();
    }

    /**
     * Publish the statistics accumulated in {@code hist}. The values are read, and the
     * {@link BarrageSubscriptionPerformanceSink} is given its chance to read them, before this method returns; neither
     * this class nor the sink is permitted to retain {@code hist}, so the caller is free to {@link Histogram#reset()
     * reset} it as soon as this method returns.
     *
     * @implNote this method is synchronized to guarantee identical ordering of entries between the publisher and the
     *           sink; doing so also relieves the requirement that the sink be thread safe
     */
    @Override
    public synchronized void log(String tableId, String tableKey, String statType, Instant now, Histogram hist) {
        final long timestampEpochNanos = DateTimeUtils.epochNanos(now);

        publisher.add(
                tableId,
                tableKey,
                statType,
                timestampEpochNanos,
                hist.getTotalCount(),
                hist.getValueAtPercentile(50) / 1e6,
                hist.getValueAtPercentile(75) / 1e6,
                hist.getValueAtPercentile(90) / 1e6,
                hist.getValueAtPercentile(95) / 1e6,
                hist.getValueAtPercentile(99) / 1e6,
                hist.getMaxValue() / 1e6);

        if (encounteredError) {
            return;
        }
        try {
            sink.log(tableId, tableKey, statType, timestampEpochNanos, hist);
        } catch (final Exception e) {
            // Catch unchecked failures as well as IOException: a defective sink must not be able to disrupt the
            // in-memory table or the caller. Don't want to log this for every entry.
            log.error().append("Error recording barrage subscription performance for ").append(tableKey)
                    .append("; disabling further attempts, caused by: ").append(e).endl();
            encounteredError = true;
        }
    }

    public Table blinkTable() {
        return blink;
    }
}
