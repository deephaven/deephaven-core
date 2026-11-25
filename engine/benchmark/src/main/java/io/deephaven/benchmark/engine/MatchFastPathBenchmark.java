//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TimeTable;
import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.util.TestClock;
import io.deephaven.engine.util.WindowCheck;
import io.deephaven.time.DateTimeUtils;
import org.openjdk.jmh.annotations.*;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Measures a {@link MatchFilter} on a column whose source answers the match without a scan: the timestamp column of a
 * time table, as an {@link Instant} and reinterpreted as a {@code long}, and the Boolean column of a
 * {@code wouldMatch}. The in-window column of {@link WindowCheck} is measured too, since the in-window source once had
 * its own match implementation. The {@code Range} sources measure a range filter on the time table's timestamp column,
 * which DH-24074 also answers without a scan.
 * <p>
 * The {@code Direct} mode filters the operation's own column. The {@code Materialized} mode filters a {@code select()}
 * copy of the same rows, whose array-backed column always scans, as the baseline a lost fast path would fall back to.
 * <p>
 * The benchmark uses only API common to {@code main} and DH-24074, so the same file measures both.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class MatchFastPathBenchmark {

    private static final long PERIOD_NANOS = 1_000;
    private static final int TIMESTAMP_VALUES = 16;

    @Param({"TimeTable", "TimeTableLong", "TimeTableRange", "TimeTableLongRange", "WouldMatch", "WindowCheck"})
    private String source;

    /** Filter the operation's own column, or a {@code select()} copy that scans. */
    @Param({"Direct", "Materialized"})
    private String mode;

    @Param({"1000000", "10000000", "100000000"})
    private int rows;

    private Table table;
    private WhereFilter filter;

    @Setup(Level.Trial)
    public void setupTrial() {
        TestExecutionContext.createForUnitTests().open();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        QueryTable.setMemoizeResults(false);
        // dh-tests.prop forces two-segment parallel where; measure with the production defaults
        setQueryTableField("FORCE_PARALLEL_WHERE", false);
        setQueryTableField("PARALLEL_WHERE_SEGMENTS", -1);

        final long expectedMatches;
        switch (source) {
            case "TimeTable":
            case "TimeTableLong":
            case "TimeTableRange":
            case "TimeTableLongRange": {
                // a start time makes the constructor add every row up to the clock's time, with no update cycle
                final TimeTable timeTable = new TimeTable(updateGraph, new TestClock((rows - 1) * PERIOD_NANOS),
                        DateTimeUtils.epochNanosToInstant(0), PERIOD_NANOS, false);
                final boolean asLong = source.startsWith("TimeTableLong");
                final ColumnSource<?> timestamps = asLong
                        ? timeTable.getColumnSource("Timestamp").reinterpret(long.class)
                        : timeTable.getColumnSource("Timestamp");
                table = new QueryTable(timeTable.getRowSet().copy().toTracking(), Map.of("Timestamp", timestamps));
                if (source.endsWith("Range")) {
                    // one row in a hundred, in the middle of the table: [rows / 2, rows / 2 + rows / 100)
                    final long lowerNanos = (long) (rows / 2) * PERIOD_NANOS;
                    final long upperNanos = lowerNanos + (long) (rows / 100) * PERIOD_NANOS;
                    filter = asLong
                            ? new LongRangeFilter("Timestamp", lowerNanos, upperNanos, true, false)
                            : new InstantRangeFilter("Timestamp", lowerNanos, upperNanos, true, false);
                    expectedMatches = rows / 100;
                    break;
                }
                final Object[] values = new Object[TIMESTAMP_VALUES];
                for (int ii = 0; ii < TIMESTAMP_VALUES; ++ii) {
                    final long nanos = (long) ii * rows / TIMESTAMP_VALUES * PERIOD_NANOS;
                    values[ii] = asLong ? (Object) nanos : DateTimeUtils.epochNanosToInstant(nanos);
                }
                filter = new MatchFilter(MatchOptions.REGULAR, "Timestamp", values);
                expectedMatches = TIMESTAMP_VALUES;
                break;
            }
            case "WouldMatch":
                // one row in a hundred matches, in runs of ten
                table = TableTools.emptyTable(rows).update("X = ii").wouldMatch("M = X % 1000 < 10");
                filter = new MatchFilter(MatchOptions.REGULAR, "M", true);
                expectedMatches = (rows / 1000) * 10L + Math.min(rows % 1000, 10);
                break;
            case "WindowCheck":
                // every other timestamp is null, so in-window is null; the rest are from 1970, so in-window is false
                table = WindowCheck.addTimeWindow(
                        (QueryTable) TableTools.emptyTable(rows)
                                .update("Timestamp = epochNanosToInstant(ii % 2 == 0 ? NULL_LONG : ii)"),
                        "Timestamp", 1_000_000_000L, "InWindow");
                filter = new MatchFilter(MatchOptions.REGULAR, "InWindow", false);
                expectedMatches = rows / 2;
                break;
            default:
                throw new IllegalArgumentException("Unknown source " + source);
        }
        if (mode.equals("Materialized")) {
            table = table.select();
        }

        final long matched = table.where(filter.copy()).size();
        System.out.println("\nsource=" + source + ", mode=" + mode + ", rows=" + table.size() + ", matched=" + matched);
        if (table.size() != rows || matched != expectedMatches) {
            throw new IllegalStateException("Expected " + rows + " rows and " + expectedMatches + " matches");
        }
    }

    @Benchmark
    public long where() {
        return table.where(filter.copy()).size();
    }

    private static void setQueryTableField(final String name, final Object value) {
        try {
            final Field field = QueryTable.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(null, value);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Unable to set QueryTable." + name, e);
        }
    }

    public static void main(final String[] args) {
        BenchUtil.run(MatchFastPathBenchmark.class);
    }
}
