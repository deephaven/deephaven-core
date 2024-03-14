//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.time.DateTimeUtils;
import org.HdrHistogram.Histogram;

import java.time.Instant;
import java.util.Map;

class BarrageSubscriptionPerformanceLoggerImpl implements BarrageSubscriptionPerformanceLogger {
    private final BarrageSubscriptionPerformanceStreamPublisher publisher;
    // Keep, may eventually want to manage / close
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    public BarrageSubscriptionPerformanceLoggerImpl() {
        publisher = new BarrageSubscriptionPerformanceStreamPublisher();
        adapter = new StreamToBlinkTableAdapter(
                BarrageSubscriptionPerformanceStreamPublisher.definition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                BarrageSubscriptionPerformanceLoggerImpl.class.getName(),
                Map.of(
                        BaseTable.BARRAGE_PERFORMANCE_KEY_ATTRIBUTE,
                        BarrageSubscriptionPerformanceLogger.getDefaultTableName()));
        blink = adapter.table();
    }

    @Override
    public void log(String tableId, String tableKey, String statType, Instant now, Histogram hist) {
        publisher.add(
                tableId,
                tableKey,
                statType,
                DateTimeUtils.epochNanos(now),
                hist.getTotalCount(),
                hist.getValueAtPercentile(50) / 1e6,
                hist.getValueAtPercentile(75) / 1e6,
                hist.getValueAtPercentile(90) / 1e6,
                hist.getValueAtPercentile(95) / 1e6,
                hist.getValueAtPercentile(99) / 1e6,
                hist.getMaxValue() / 1e6);
    }

    public Table blinkTable() {
        return blink;
    }
}
