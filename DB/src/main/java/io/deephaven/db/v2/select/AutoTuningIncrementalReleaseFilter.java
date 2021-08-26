/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.io.logger.Logger;
import io.deephaven.util.clock.RealTimeClock;
import io.deephaven.util.process.ProcessEnvironment;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.utils.ClockTimeProvider;
import io.deephaven.db.v2.utils.TerminalNotification;
import io.deephaven.db.v2.utils.TimeProvider;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.text.DecimalFormat;

/**
 * Filter that releases the required number of rows from a table to saturate the LTM cycle.
 * <p>
 * The table has an initial size, which can be thought of as the size during query initialization. There is an initial
 * number of rows that are released, which is then used to tune the number of rows to release on the subsequent cycle.
 * <p>
 * The targetFactor parameter is multiplied by the LTM's targetCycle. This allows you to determine how busy you want the
 * LTM to be. For example a factor of 1, will attempt to hit the target cycle exactly. A target of 0.5 should result an
 * LTM ratio of about 50%. A factor of 10 would mean that the system will extend beyond the target cycle time, coalesce
 * updates accordingly and have a ratio that is nearly 100%.
 * <p>
 * The time the rows are released is recorded, and a terminal notification is enqueued to record the end of the cycle.
 * On each cycle, the number of rows per second is computed; and then the number of rows released is the LTM's target
 * cycle multiplied by the rows per second multiplied by the target factor.
 *
 *
 * <p>
 * The AutotuningIncrementalReleaseFilter can be used to benchmark how many rows of data a query can process. In its
 * simplest form we can measure how many rows a lastBy statement can process. For example:
 * </p>
 * 
 * <pre>
 * import io.deephaven.db.v2.select.AutoTuningIncrementalReleaseFilter
 *
 * quotes = db.t("FeedOS", "EquityQuoteL1").where("Date=lastBusinessDateNy()")
 * filter=new AutoTuningIncrementalReleaseFilter(10000, 10000, 1)
 * quotesFiltered = quotes.where(filter)
 * currentQuote = quotesFiltered.lastBy("LocalCodeStr").update("Mid=(Bid + Ask)/2")
 * </pre>
 * 
 * Produces a currentQuote table, and you can view the Log tab to determine how many rows per second were processed. The
 * summary is sent to the WARN level:
 * 
 * <pre>
 * 12:55:49.985 WARN Completed release 6.97 seconds, rows=19630961, rows/second=2,817,053.86
 * </pre>
 * 
 * If verbose mode is enabled, progress is displayed for each cycle at the INFO level.
 * </p>
 *
 * <p>
 * You may specify a StreamLoggerImpl() to send the data to STDOUT, as follows:
 * </p>
 * 
 * <pre>
 * import io.deephaven.db.v2.select.AutoTuningIncrementalReleaseFilter
 *
 * quotes = db.t("FeedOS", "EquityQuoteL1").where("Date=lastBusinessDateNy()")
 * logger = new io.deephaven.io.logger.StreamLoggerImpl()
 * filterQuotes=new AutoTuningIncrementalReleaseFilter(logger, 10000, 10000, 1.0d, true)
 * quotesFiltered = quotes.where(filterQuotes)
 * currentQuote = quotesFiltered.lastBy("LocalCodeStr").update("Mid=(Bid + Ask)/2")
 * </pre>
 * <p>
 * The verbose information and the final report are easily visible on your console.
 * </p>
 *
 * <p>
 * The AutotuningIncrementalReleaseFilter is best suited for queries that have a single source table with arbitrary
 * amounts of processing on that table. Multiple incremental release filters may be combined, and each filter will
 * report the number of rows that were released per second, however the data is not synchronized between tables and it
 * is not possible to differentiate which table is contributing more to the query's load without examining the
 * performance tables. You may need to adjust the initial size parameters so that one table does not complete processing
 * before another.
 * 
 * <pre>
 * import io.deephaven.db.v2.select.AutoTuningIncrementalReleaseFilter
 *
 * quotes = db.t("FeedOS", "EquityQuoteL1").where("Date=lastBusinessDateNy()")
 * trades = db.t("FeedOS", "EquityTradeL1").where("Date=lastBusinessDateNy()")
 * filterQuotes=new AutoTuningIncrementalReleaseFilter(10000, 10000, 1, true)
 * quotesFiltered = quotes.where(filterQuotes)
 * filterTrades=new AutoTuningIncrementalReleaseFilter(10000, 10000, 1, true)
 * tradesFiltered = trades.where(filterTrades)
 *
 * decorated = tradesFiltered.aj(quotesFiltered, "LocalCodeStr,MarketTimestamp", "QuoteTime=MarketTimestamp,Bid,BidSize,Ask,AskSize")
 * </pre>
 */
public class AutoTuningIncrementalReleaseFilter extends BaseIncrementalReleaseFilter {
    @NotNull
    private final TimeProvider timeProvider;
    private final long initialRelease;
    private final boolean verbose;
    private final Logger logger;
    private final double targetFactor;

    private DBDateTime firstCycle;

    private long nextSize = 0;
    private DBDateTime lastRefresh;
    private DBDateTime cycleEnd;
    private boolean releasedAll = false;

    /**
     * Create an auto tuning release filter using a real time clock, without printing on each cycle.
     *
     * @param initialSize the initial table size
     * @param initialRelease the initial incremental update; after the first cycle the rows per second is calculated
     *        based on the duration of the last cycle and the number of rows released by this filter
     * @param targetFactor the multiple of the LTM cycle we should aim for
     */
    @ScriptApi
    public AutoTuningIncrementalReleaseFilter(long initialSize, long initialRelease, double targetFactor) {
        this(initialSize, initialRelease, targetFactor, false);
    }

    /**
     * Create an auto tuning release filter using a real time clock, without printing on each cycle.
     *
     * @param logger the logger the final row/second calculations to
     * @param initialSize the initial table size
     * @param initialRelease the initial incremental update; after the first cycle the rows per second is calculated
     *        based on the duration of the last cycle and the number of rows released by this filter
     * @param targetFactor the multiple of the LTM cycle we should aim for
     */
    @ScriptApi
    public AutoTuningIncrementalReleaseFilter(Logger logger, long initialSize, long initialRelease,
            double targetFactor) {
        this(logger, initialSize, initialRelease, targetFactor, false);
    }

    /**
     * Create an auto tuning release filter using a real time clock.
     *
     * @param initialSize the initial table size
     * @param initialRelease the initial incremental update; after the first cycle the rows per second is calculated
     *        based on the duration of the last cycle and the number of rows released by this filter
     * @param targetFactor the multiple of the LTM cycle we should aim for
     * @param verbose whether information should be printed on each LTM cycle describing the current rate and number of
     *        rows released
     */
    @ScriptApi
    public AutoTuningIncrementalReleaseFilter(long initialSize, long initialRelease, double targetFactor,
            boolean verbose) {
        this(initialSize, initialRelease, targetFactor, verbose, getRealTimeProvider());
    }

    /**
     * Create an auto tuning release filter using the provided {@link TimeProvider}.
     *
     * @param logger the logger to report progress (if verbose is set) and the final row/second calculations
     * @param initialSize the initial table size
     * @param initialRelease the initial incremental update; after the first cycle the rows per second is calculated
     *        based on the duration of the last cycle and the number of rows released by this filter
     * @param targetFactor the multiple of the LTM cycle we should aim for
     * @param verbose whether information should be printed on each LTM cycle describing the current rate and number of
     *        rows released
     */
    @ScriptApi
    public AutoTuningIncrementalReleaseFilter(Logger logger, long initialSize, long initialRelease, double targetFactor,
            boolean verbose) {
        this(logger, initialSize, initialRelease, targetFactor, verbose, getRealTimeProvider());
    }

    @NotNull
    private static ClockTimeProvider getRealTimeProvider() {
        return new ClockTimeProvider(new RealTimeClock());
    }

    /**
     * Create an auto tuning release filter using the provided {@link TimeProvider}.
     *
     * @param initialSize the initial table size
     * @param initialRelease the initial incremental update; after the first cycle the rows per second is calculated
     *        based on the duration of the last cycle and the number of rows released by this filter
     * @param targetFactor the multiple of the LTM cycle we should aim for
     * @param verbose whether information should be printed on each LTM cycle describing the current rate and number of
     *        rows released
     * @param timeProvider the time provider, which is used to determine the start and end of each cycle
     */
    @ScriptApi
    public AutoTuningIncrementalReleaseFilter(long initialSize, long initialRelease, double targetFactor,
            boolean verbose, TimeProvider timeProvider) {
        this(ProcessEnvironment.getDefaultLog(), initialSize, initialRelease, targetFactor, verbose, timeProvider);
    }

    /**
     * Create an auto tuning release filter using the provided {@link TimeProvider}.
     *
     * @param logger the logger to report progress (if verbose is set) and the final row/second calculations
     * @param initialSize the initial table size
     * @param initialRelease the initial incremental update; after the first cycle the rows per second is calculated
     *        based on the duration of the last cycle and the number of rows released by this filter
     * @param targetFactor the multiple of the LTM cycle we should aim for
     * @param verbose whether information should be printed on each LTM cycle describing the current rate and number of
     *        rows released
     * @param timeProvider the time provider, which is used to determine the start and end of each cycle
     */
    @ScriptApi
    public AutoTuningIncrementalReleaseFilter(Logger logger, long initialSize, long initialRelease, double targetFactor,
            boolean verbose, TimeProvider timeProvider) {
        super(initialSize);
        this.logger = logger;
        this.targetFactor = targetFactor;
        this.verbose = verbose;
        this.lastRefresh = timeProvider.currentTime();
        this.initialRelease = Math.max(1, initialRelease);
        this.timeProvider = timeProvider;
    }

    @Override
    long getSizeIncrement() {
        return nextSize;
    }

    @Override
    public void refresh() {
        if (releasedAll) {
            return;
        }
        final DBDateTime now = timeProvider.currentTime();
        if (nextSize == 0) {
            firstCycle = now;
            nextSize = initialRelease;
        } else {
            final long cycleDuration = (cycleEnd.getNanos() - lastRefresh.getNanos());
            final long targetCycle = LiveTableMonitor.DEFAULT.getTargetCycleTime() * 1000 * 1000;
            final double rowsPerNanoSecond = ((double) nextSize) / cycleDuration;
            nextSize = Math.max((long) (rowsPerNanoSecond * targetCycle * targetFactor), 1L);
            if (verbose) {
                final DecimalFormat decimalFormat = new DecimalFormat("###,###.##");
                final long totalRows = getReleasedSize();
                final long remaining = getExpectedSize() - getReleasedSize();
                final double totalNanos = now.getNanos() - firstCycle.getNanos();
                final double totalRowsPerNano = totalRows / totalNanos;
                final double totalRowsPerSecond = totalRowsPerNano * 1_000_000_000L;
                final double eta = (remaining / totalRowsPerSecond);
                logger.info().append("Releasing: ").append(nextSize).append(" rows, last rows/second: ")
                        .append(decimalFormat.format(rowsPerNanoSecond * 1_000_000_000L)).append(", duration=")
                        .append(cycleDuration / 1000000L).append(" ms, total rows/second=")
                        .append(decimalFormat.format(totalRowsPerSecond)).append(", ETA ")
                        .append(decimalFormat.format(eta)).append(" sec").endl();
            }
        }
        LiveTableMonitor.DEFAULT.addNotification(new TerminalNotification() {
            final boolean captureReleasedAll = releasedAll;

            @Override
            public void run() {
                cycleEnd = timeProvider.currentTime();
                if (!captureReleasedAll && releasedAll) {
                    final DecimalFormat decimalFormat = new DecimalFormat("###,###.##");
                    final long durationNanos = cycleEnd.getNanos() - firstCycle.getNanos();
                    final double durationSeconds = (double) durationNanos / (double) 1_000_000_000L;
                    final long rows = getReleasedSize();
                    final double rowsPerSecond = (double) rows / durationSeconds;
                    logger.warn().append("Completed release ").append(decimalFormat.format(durationSeconds))
                            .append(" seconds, rows=").append(rows).append(", rows/second=")
                            .append(decimalFormat.format(rowsPerSecond)).endl();
                }
            }
        });
        lastRefresh = now;
        super.refresh();
    }

    @Override
    void onReleaseAll() {
        releasedAll = true;
    }

    @Override
    public AutoTuningIncrementalReleaseFilter copy() {
        return new AutoTuningIncrementalReleaseFilter(getInitialSize(), initialRelease, targetFactor, verbose,
                timeProvider);
    }
}
