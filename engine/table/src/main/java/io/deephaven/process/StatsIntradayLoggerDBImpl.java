package io.deephaven.process;

import io.deephaven.stats.StatsIntradayLogger;
import io.deephaven.engine.tablelogger.ProcessMetricsLogLogger;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

public class StatsIntradayLoggerDBImpl implements StatsIntradayLogger {

    private static final char COUNTER_TAG = 'C'; //Counter.TYPE_TAG;
    private static final char HISTOGRAM_STATE_TAG = 'H'; // HistogramState.TYPE_TAG;
    private static final char STATE_TAG = 'S'; // State.TYPE_TAG;
    private static final char HISTOGRAM2_TAG = 'N'; // HistogramPower2.TYPE_TAG;

    private final ProcessUniqueId id;
    private final ProcessMetricsLogLogger logger;

    public StatsIntradayLoggerDBImpl(final ProcessUniqueId id, final ProcessMetricsLogLogger logger) {
        this.id = Objects.requireNonNull(id, "id");
        this.logger = Objects.requireNonNull(logger, "logger");
    }

    @Override
    public void log(String intervalName, long now, long appNow, char typeTag, String compactName, long n, long sum, long last, long min, long max, long avg, long sum2, long stdev) {
        try {
            logger.log(now, id.value(), compactName, intervalName, getTagType(typeTag), n, sum, last, min, max, avg, sum2, stdev);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void log(String intervalName, long now, long appNow, char typeTag, String compactName, long n, long sum, long last, long min, long max, long avg, long sum2, long stdev, long[] h) {
        // Unsupported at this time. Ignore these records.
    }

    private static String getTagType(char typeTag) {
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
