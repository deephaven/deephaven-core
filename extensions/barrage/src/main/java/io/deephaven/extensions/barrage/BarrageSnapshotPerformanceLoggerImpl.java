//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.BarragePerformanceLog.SnapshotMetricsHelper;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.time.DateTimeUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Publishes barrage snapshot statistics to the in-memory blink table, and forwards each entry to the
 * {@link BarrageSnapshotPerformanceSink} provided by {@link BarrageTableLoggers}.
 */
class BarrageSnapshotPerformanceLoggerImpl implements BarrageSnapshotPerformanceLogger {
    private static final Logger log = LoggerFactory.getLogger(BarrageSnapshotPerformanceLoggerImpl.class);

    private final BarrageSnapshotPerformanceSink sink;
    private final BarrageSnapshotPerformanceStreamPublisher publisher;
    // Keep, may eventually want to manage / close
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    private boolean encounteredError = false;

    public BarrageSnapshotPerformanceLoggerImpl(final BarrageSnapshotPerformanceSink sink) {
        this.sink = Objects.requireNonNull(sink);
        publisher = new BarrageSnapshotPerformanceStreamPublisher();
        adapter = new StreamToBlinkTableAdapter(
                BarrageSnapshotPerformanceStreamPublisher.definition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                BarrageSnapshotPerformanceLoggerImpl.class.getName(),
                Map.of(
                        Table.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE,
                        BarrageSnapshotPerformanceLogger.getDefaultTableName()));
        blink = adapter.table();
    }

    /**
     * @implNote this method is synchronized because snapshots are written from arbitrary request-serving threads; doing
     *           so guarantees identical ordering of entries between the publisher and the sink, and relieves the
     *           requirement that the sink be thread safe
     */
    @Override
    public synchronized void log(SnapshotMetricsHelper helper, long writeNanos, long bytesWritten) {
        publisher.add(
                helper.tableId,
                helper.tableKey,
                DateTimeUtils.epochNanos(helper.requestTm),
                helper.queueNanos / 1e6,
                helper.snapshotNanos / 1e6,
                writeNanos / 1e6,
                (8 * bytesWritten) / 1e6);

        if (encounteredError) {
            return;
        }
        try {
            sink.log(helper.tableId, helper.tableKey, helper.requestTm, helper.queueNanos, helper.snapshotNanos,
                    writeNanos, bytesWritten);
        } catch (final IOException e) {
            // Don't want to log this for every entry
            log.error().append("Error recording barrage snapshot performance for ").append(helper.tableKey)
                    .append(" caused by: ").append(e).endl();
            encounteredError = true;
        }
    }

    public Table blinkTable() {
        return blink;
    }
}
