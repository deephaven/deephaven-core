//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.tablelogger.ProcessMetricsLogLogger;
import io.deephaven.process.ProcessUniqueId;
import io.deephaven.stats.StatsIntradayLogger;
import io.deephaven.stream.StreamToBlinkTableAdapter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

class StatsImpl implements StatsIntradayLogger {
    private final ProcessUniqueId id;
    private final ProcessMetricsLogLogger logger;
    private final StatsStreamPublisher publisher;
    // Keep, may eventually want to manage / close
    @SuppressWarnings("FieldCanBeLocal")
    private final StreamToBlinkTableAdapter adapter;
    private final Table blink;

    public StatsImpl(
            ProcessUniqueId id,
            ProcessMetricsLogLogger logger) {
        this.id = Objects.requireNonNull(id);
        this.logger = Objects.requireNonNull(logger);
        this.publisher = new StatsStreamPublisher();
        adapter = new StreamToBlinkTableAdapter(
                StatsStreamPublisher.definition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                StatsImpl.class.getName());
        blink = adapter.table();
    }

    public Table blinkTable() {
        return blink;
    }

    @Override
    public void log(String intervalName, long now, long appNow, char typeTag, String compactName, long n, long sum,
            long last, long min, long max, long avg, long sum2, long stdev) {
        final String type = type(typeTag);
        publisher.add(id.value(), now, compactName, intervalName, type, n, sum, last, min, max, avg, sum2, stdev);
        try {
            logger.log(now, id.value(), compactName, intervalName, type, n, sum, last, min, max, avg, sum2, stdev);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void log(String intervalName, long now, long appNow, char typeTag, String compactName, long n, long sum,
            long last, long min, long max, long avg, long sum2, long stdev, long[] h) {

    }

    private static final char COUNTER_TAG = 'C'; // Counter.TYPE_TAG;
    private static final char HISTOGRAM_STATE_TAG = 'H'; // HistogramState.TYPE_TAG;
    private static final char STATE_TAG = 'S'; // State.TYPE_TAG;
    private static final char HISTOGRAM2_TAG = 'N'; // HistogramPower2.TYPE_TAG;

    private static String type(char typeTag) {
        switch (typeTag) {
            case COUNTER_TAG:
                return "counter";
            case HISTOGRAM_STATE_TAG:
                return "histogram";
            case STATE_TAG:
                return "state";
            case HISTOGRAM2_TAG:
                return "histogram2";
        }
        throw new IllegalArgumentException("Unexpected typeTag: " + typeTag);
    }
}
