//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.extensions.barrage.BarragePerformanceLog.SnapshotMetricsHelper;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.time.DateTimeUtils;

import java.util.Map;

class BarrageSnapshotPerformanceLoggerImpl implements BarrageSnapshotPerformanceLogger {
    private final BarrageSnapshotPerformanceStreamPublisher publisher;
    // Keep, may eventually want to manage / close
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    public BarrageSnapshotPerformanceLoggerImpl() {
        publisher = new BarrageSnapshotPerformanceStreamPublisher();
        adapter = new StreamToBlinkTableAdapter(
                BarrageSnapshotPerformanceStreamPublisher.definition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                BarrageSnapshotPerformanceLoggerImpl.class.getName(),
                Map.of(
                        BaseTable.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE,
                        BarrageSnapshotPerformanceLogger.getDefaultTableName()));
        blink = adapter.table();
    }

    @Override
    public void log(SnapshotMetricsHelper helper, long writeNanos, long bytesWritten) {
        publisher.add(
                helper.tableId,
                helper.tableKey,
                DateTimeUtils.epochNanos(helper.requestTm),
                helper.queueNanos / 1e6,
                helper.snapshotNanos / 1e6,
                writeNanos / 1e6,
                (8 * bytesWritten) / 1e6);
    }

    public Table blinkTable() {
        return blink;
    }
}
