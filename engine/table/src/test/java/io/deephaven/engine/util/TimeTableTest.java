//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.api.filter.Filter;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.primitive.iterator.CloseablePrimitiveIteratorOfLong;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.NoPushdownColumnSourceWrapper;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateValidator;
import io.deephaven.engine.table.impl.TimeTable;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.LongRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.ReinterpretedColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.sources.FillUnordered;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.iterators.ChunkedLongColumnIterator;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;

public class TimeTableTest extends RefreshingTableTestCase {

    private TestClock clock;
    private UpdateSourceCombiner updateSourceCombiner;
    private QueryTable timeTable;
    private TableUpdateValidator validator;
    private ColumnSource<Long> column;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        clock = new TestClock(0);
        updateSourceCombiner = new UpdateSourceCombiner(ExecutionContext.getContext().getUpdateGraph());
    }

    @Override
    public void tearDown() throws Exception {
        validator.deepValidation();

        clock = null;
        updateSourceCombiner = null;
        timeTable = null;
        validator = null;
        column = null;

        super.tearDown();
    }

    private void build(TimeTable.Builder builder) {
        timeTable = builder
                .registrar(updateSourceCombiner)
                .clock(clock)
                .build();
        column = timeTable.getColumnSource("Timestamp").reinterpret(long.class);
        validator = TableUpdateValidator.make(timeTable);
    }

    private void tick(long tm) {
        clock.now = tm;
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
        validator.validate();
    }

    @Test
    public void testNoStartTimeOnBoundary() {
        build(TimeTable.newBuilder().period(10));

        tick(0);
        Assert.assertEquals(timeTable.size(), 1L);
        Assert.assertEquals(column.getLong(0), 0);

        tick(9);
        Assert.assertEquals(timeTable.size(), 1);

        tick(10);
        Assert.assertEquals(timeTable.size(), 2);
        Assert.assertEquals(column.getLong(1), 10);

        // Check that it will tick multiple rows.
        tick(100);
        Assert.assertEquals(timeTable.size(), 11);
    }

    @Test
    public void testNoStartTimeLowerBounds() {
        build(TimeTable.newBuilder().period(10));

        tick(15);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 10);

        tick(24);
        Assert.assertEquals(timeTable.size(), 2);
        Assert.assertEquals(column.getLong(1), 20);
    }

    @Test
    public void testProvidedStartTimeOnBoundary() {
        build(TimeTable.newBuilder()
                .startTime(DateTimeUtils.epochNanosToInstant(10))
                .period(10));

        tick(9);
        Assert.assertEquals(timeTable.size(), 0);

        tick(10);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 10);
    }

    @Test
    public void testProvidedStartTimeOffsetPeriod() {
        build(TimeTable.newBuilder()
                .startTime(DateTimeUtils.epochNanosToInstant(15))
                .period(10));

        tick(14);
        Assert.assertEquals(timeTable.size(), 0);

        tick(16);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 15);

        tick(34);
        Assert.assertEquals(timeTable.size(), 2);
        Assert.assertEquals(column.getLong(1), 25);
    }

    @Test
    public void testBlinkNoStartTimeOnBoundary() {
        build(TimeTable.newBuilder().blinkTable(true).period(10));

        tick(0);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 0);

        // Check for blink table property that rows exist for a single tick.
        tick(9);
        Assert.assertEquals(timeTable.size(), 0);

        // Ensure multiple rows in one tick.
        tick(100);
        Assert.assertEquals(timeTable.size(), 10);
        Assert.assertEquals(timeTable.getRowSet().firstRowKey(), 1);
        Assert.assertEquals(column.getLong(10), 100);
    }

    @Test
    public void testBlinkNoStartTimeLowerBounds() {
        build(TimeTable.newBuilder().blinkTable(true).period(10));

        tick(15);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 10);

        tick(21);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(timeTable.getRowSet().firstRowKey(), 1);
        Assert.assertEquals(column.getLong(1), 20);
    }

    @Test
    public void testBlinkProvidedStartTimeOnBoundary() {
        build(TimeTable.newBuilder()
                .blinkTable(true)
                .startTime(DateTimeUtils.epochNanosToInstant(10))
                .period(10));

        tick(9);
        Assert.assertEquals(timeTable.size(), 0);

        tick(10);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 10);
    }

    @Test
    public void testBlinkProvidedStartTimeOffsetPeriod() {
        build(TimeTable.newBuilder()
                .blinkTable(true)
                .startTime(DateTimeUtils.epochNanosToInstant(15))
                .period(10));

        tick(14);
        Assert.assertEquals(timeTable.size(), 0);

        tick(16);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(column.getLong(0), 15);

        tick(34);
        Assert.assertEquals(timeTable.size(), 1);
        Assert.assertEquals(timeTable.getRowSet().firstRowKey(), 1);
        Assert.assertEquals(column.getLong(1), 25);
    }

    @Test
    public void testTimestampPushdown() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.assertEquals(timeTable.size(), 201);

        final Long[] longKeys = new Long[] {null, 1000L, 1050L, 1100L, 1025L};
        final Instant[] keys = Arrays.stream(longKeys)
                .map(l -> l == null ? null : DateTimeUtils.epochNanosToInstant(l))
                .toArray(Instant[]::new);
        final RowSet selection = RowSetFactory.fromRange(100, 110);
        final RowSet matching = RowSetFactory.fromKeys(100, 105, 110);
        final RowSet notMatching = RowSetFactory.fromKeys(101, 102, 103, 104, 106, 107, 108, 109);

        assertExactMatch(matching, pushdown(dtColumn, instantFilter(MatchOptions.REGULAR, keys), selection));
        assertExactMatch(matching, pushdown(column, longFilter(MatchOptions.REGULAR, longKeys), selection));
        assertExactMatch(notMatching, pushdown(dtColumn, instantFilter(MatchOptions.INVERTED, keys), selection));
        assertExactMatch(notMatching, pushdown(column, longFilter(MatchOptions.INVERTED, longKeys), selection));

        // a where reaches the pushdown, and agrees with scanning the column
        final MatchFilter regular = instantFilter(MatchOptions.REGULAR, keys);
        final MatchFilter inverted = instantFilter(MatchOptions.INVERTED, keys);
        assertTableEquals(scanned(regular), pushedDown(regular));
        assertTableEquals(scanned(inverted), pushedDown(inverted));
        Assert.assertEquals(3, pushedDown(regular).size());
    }

    /**
     * Before the first refresh sets the start time, the table has no rows, so no value matches and every row of the
     * selection is left by an inverted match.
     */
    @Test
    public void testTimestampPushdownBeforeFirstTick() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        final RowSet selection = RowSetFactory.fromRange(0, 4);
        final Instant[] keys = new Instant[] {DateTimeUtils.epochNanosToInstant(0)};

        assertExactMatch(RowSetFactory.empty(),
                pushdown(dtColumn, instantFilter(MatchOptions.REGULAR, keys), selection));
        assertExactMatch(selection, pushdown(dtColumn, instantFilter(MatchOptions.INVERTED, keys), selection));
    }

    /**
     * A start time before the epoch gives negative timestamps; a value on the period grid matches whichever side of the
     * epoch it lies on, and a value off the grid or before the start time matches nothing.
     */
    @Test
    public void testTimestampPushdownBeforeEpoch() {
        build(TimeTable.newBuilder()
                .startTime(DateTimeUtils.epochNanosToInstant(-25))
                .period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(20);
        // timestamps -25, -15, -5, 5 and 15
        Assert.assertEquals(5, timeTable.size());

        final Long[] longKeys = new Long[] {5L, -15L, 0L, -35L, Long.MAX_VALUE};
        final Instant[] keys = Arrays.stream(longKeys)
                .map(DateTimeUtils::epochNanosToInstant)
                .toArray(Instant[]::new);
        final RowSet selection = timeTable.getRowSet();

        assertExactMatch(RowSetFactory.fromKeys(1, 3),
                pushdown(dtColumn, instantFilter(MatchOptions.REGULAR, keys), selection));
        assertExactMatch(RowSetFactory.fromKeys(1, 3),
                pushdown(column, longFilter(MatchOptions.REGULAR, longKeys), selection));
        assertExactMatch(RowSetFactory.fromKeys(0, 2, 4),
                pushdown(dtColumn, instantFilter(MatchOptions.INVERTED, keys), selection));

        final MatchFilter regular = instantFilter(MatchOptions.REGULAR, keys);
        assertTableEquals(scanned(regular), pushedDown(regular));
    }

    /**
     * An {@link Instant} far enough before the epoch makes {@link DateTimeUtils#epochNanos(Instant)} wrap around, here
     * to exactly 0, the timestamp of row 0. The pushdown matches no row for such a value, as a scan of the
     * {@link Instant} column does.
     */
    @Test
    public void testTimestampPushdownUnderflowingInstant() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(100);
        final Instant underflowing = Instant.ofEpochSecond(-18446744074L, 290448384L);
        Assert.assertEquals(0, DateTimeUtils.epochNanos(underflowing));
        final RowSet selection = timeTable.getRowSet();

        assertExactMatch(RowSetFactory.empty(),
                pushdown(dtColumn, instantFilter(MatchOptions.REGULAR, underflowing), selection));
        assertExactMatch(selection, pushdown(dtColumn, instantFilter(MatchOptions.INVERTED, underflowing), selection));
        final MatchFilter regular = instantFilter(MatchOptions.REGULAR, underflowing);
        final MatchFilter inverted = instantFilter(MatchOptions.INVERTED, underflowing);
        assertTableEquals(scanned(regular), pushedDown(regular));
        assertTableEquals(scanned(inverted), pushedDown(inverted));
    }

    /**
     * A where on either timestamp column answers matches and ranges from the pushdown alone: the engine scans no row
     * with the filter.
     */
    @Test
    public void testTimestampWhereTakesThePushdown() {
        build(TimeTable.newBuilder().period(10));
        tick(0);
        tick(2000);
        final Table instants = new QueryTable(timeTable.getRowSet().copy().toTracking(),
                Map.of("Timestamp", timeTable.getColumnSource("Timestamp")));
        final Table longs = new QueryTable(timeTable.getRowSet().copy().toTracking(), Map.of("Timestamp", column));

        // timestamps 1000, 1050 and 1100 are rows 100, 105 and 110; 1025 lies off the period grid
        final Long[] longValues = {1000L, 1050L, 1100L, 1025L};
        final Object[] instantValues = Arrays.stream(longValues).map(DateTimeUtils::epochNanosToInstant).toArray();
        try (final RowSet matching = RowSetFactory.fromKeys(100, 105, 110);
                final RowSet notMatching = timeTable.getRowSet().minus(matching)) {
            assertWhereTakesThePushdown(matching, instants,
                    new MatchFilter(MatchOptions.REGULAR, "Timestamp", instantValues));
            assertWhereTakesThePushdown(notMatching, instants,
                    new MatchFilter(MatchOptions.INVERTED, "Timestamp", instantValues));
            assertWhereTakesThePushdown(matching, longs,
                    new MatchFilter(MatchOptions.REGULAR, "Timestamp", (Object[]) longValues));
            assertWhereTakesThePushdown(notMatching, longs,
                    new MatchFilter(MatchOptions.INVERTED, "Timestamp", (Object[]) longValues));
        }

        // timestamps 510 through 1540
        try (final RowSet inRange = RowSetFactory.fromRange(51, 154)) {
            assertWhereTakesThePushdown(inRange, instants, new InstantRangeFilter("Timestamp", 505, 1550, true, false));
            assertWhereTakesThePushdown(inRange, longs, new LongRangeFilter("Timestamp", 505, 1550, true, false));
        }
    }

    /**
     * A where on the ticking time table answers the rows each refresh adds from the pushdown too. Each refresh adds at
     * least as many rows as the match has values, so the match pushdown never yields to a scan.
     */
    @Test
    public void testTickingTimestampWhereTakesThePushdown() {
        build(TimeTable.newBuilder().period(10));
        tick(0);
        tick(100);
        try (final RowSetCapturingFilter match = new RowSetCapturingFilter(new MatchFilter(MatchOptions.REGULAR,
                "Timestamp", DateTimeUtils.epochNanosToInstant(1000), DateTimeUtils.epochNanosToInstant(3000)));
                final RowSetCapturingFilter range =
                        new RowSetCapturingFilter(new InstantRangeFilter("Timestamp", 505, 1550, true, false))) {
            final Table matched = timeTable.where(match);
            final Table inRange = timeTable.where(range);
            Assert.assertTrue(matched.isEmpty());
            Assert.assertTrue(inRange.isEmpty());

            tick(2000);
            Assert.assertEquals(RowSetFactory.fromKeys(100), matched.getRowSet());
            Assert.assertEquals(RowSetFactory.fromRange(51, 154), inRange.getRowSet());

            tick(4000);
            Assert.assertEquals(RowSetFactory.fromKeys(100, 300), matched.getRowSet());
            Assert.assertEquals(RowSetFactory.fromRange(51, 154), inRange.getRowSet());

            Assert.assertEquals(0, match.numRowsProcessed());
            Assert.assertEquals(0, range.numRowsProcessed());
        }
    }

    /**
     * A where on {@code table} with {@code filter} selects {@code expected}, and the engine scans no row with the
     * filter.
     */
    private static void assertWhereTakesThePushdown(final RowSet expected, final Table table,
            final WhereFilter filter) {
        try (final RowSetCapturingFilter capturing = new RowSetCapturingFilter(filter)) {
            Assert.assertEquals(filter.toString(), expected, table.where(capturing).getRowSet());
            Assert.assertEquals(filter + " scanned rows", 0, capturing.numRowsProcessed());
        }
    }

    /**
     * The timestamp columns decline a filter that is neither a match nor a range filter (here an inverted range), and a
     * cost ceiling below the cost of computing the row keys. The estimate declines a match with more values than the
     * selection has rows, which a scan answers more cheaply, but a pushdown of such a match still answers exactly.
     */
    @Test
    public void testTimestampPushdownDeclines() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(100);
        final RowSet selection = RowSetFactory.fromRange(0, 1);
        final MatchFilter oneValue =
                instantFilter(MatchOptions.REGULAR, DateTimeUtils.epochNanosToInstant(10));
        final MatchFilter threeValues = instantFilter(MatchOptions.REGULAR, DateTimeUtils.epochNanosToInstant(0),
                DateTimeUtils.epochNanosToInstant(10), DateTimeUtils.epochNanosToInstant(20));
        final WhereFilter invertedRange =
                WhereFilterInvertedImpl.of(WhereFilterFactory.getExpression("Timestamp > '1970-01-01T00:00:00Z'"));
        invertedRange.init(timeTable.getDefinition());

        Assert.assertEquals(PushdownResult.TABLE_KEY_LOOKUP_COST, estimate(dtColumn, oneValue, selection));
        Assert.assertEquals(PushdownResult.UNSUPPORTED_ACTION_COST, estimate(dtColumn, threeValues, selection));
        Assert.assertEquals(PushdownResult.UNSUPPORTED_ACTION_COST, estimate(dtColumn, invertedRange, selection));

        try (final PushdownResult declined =
                pushdown(dtColumn, oneValue, selection, PushdownResult.TABLE_KEY_LOOKUP_COST - 1)) {
            Assert.assertEquals(selection, declined.maybeMatch());
            Assert.assertTrue(declined.match().isEmpty());
        }
        assertExactMatch(selection, pushdown(dtColumn, threeValues, selection));
        try (final PushdownResult declined = pushdown(dtColumn, invertedRange, selection)) {
            Assert.assertEquals(selection, declined.maybeMatch());
            Assert.assertTrue(declined.match().isEmpty());
        }
    }

    /**
     * A range filter on either timestamp column matches the run of row keys whose timestamps lie in the range, the same
     * rows a scan selects, for every combination of bounds on and off the period grid, before the first row, after the
     * last row, at the extremes of {@code long}, and with inclusive and exclusive ends.
     */
    @Test
    public void testTimestampRangePushdown() {
        build(TimeTable.newBuilder().period(10));
        tick(0);
        tick(2000);
        Assert.assertEquals(201, timeTable.size());
        assertRangePushdownMatchesScan(new long[] {Long.MIN_VALUE, -10, 0, 5, 10, 995, 1000, 1005, 2000, 2010,
                Long.MAX_VALUE - 5, Long.MAX_VALUE});
    }

    /**
     * A start time before the epoch gives negative timestamps, and a bound far from the start time makes the offset
     * from the start time overflow a {@code long}.
     */
    @Test
    public void testTimestampRangePushdownBeforeEpoch() {
        build(TimeTable.newBuilder()
                .startTime(DateTimeUtils.epochNanosToInstant(-25))
                .period(10));
        tick(20);
        // timestamps -25, -15, -5, 5 and 15
        Assert.assertEquals(5, timeTable.size());
        assertRangePushdownMatchesScan(new long[] {Long.MIN_VALUE, Long.MIN_VALUE + 5, -26, -25, -20, -15, -1, 0, 5,
                15, 16, Long.MAX_VALUE - 5, Long.MAX_VALUE});
    }

    /** Before the first refresh sets the start time, the table has no rows, so a range matches nothing. */
    @Test
    public void testTimestampRangePushdownBeforeFirstTick() {
        build(TimeTable.newBuilder().period(10));
        final RowSet selection = RowSetFactory.fromRange(0, 4);
        assertExactMatch(RowSetFactory.empty(), pushdown(timeTable.getColumnSource("Timestamp"),
                instantRange(Long.MIN_VALUE, Long.MAX_VALUE, true, true), selection));
        assertExactMatch(RowSetFactory.empty(),
                pushdown(column, longRange(Long.MIN_VALUE, Long.MAX_VALUE, true, true), selection));
    }

    /**
     * A where with range expressions on the timestamp column reaches the pushdown, and agrees with scanning the column.
     */
    @Test
    public void testTimestampRangeWhere() {
        build(TimeTable.newBuilder().period(10));
        tick(0);
        tick(2000);
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        for (final String expression : new String[] {
                "Timestamp > '1970-01-01T00:00:00.000000995Z'",
                "Timestamp >= '1970-01-01T00:00:00.000001Z'",
                "Timestamp < '1970-01-01T00:00:00.000001005Z'",
                "Timestamp <= '1970-01-01T00:00:00.00000001Z'",
                "Timestamp > '2000-01-01T00:00:00Z'",
                "Timestamp < '1960-01-01T00:00:00Z'"}) {
            final WhereFilter filter = WhereFilterFactory.getExpression(expression);
            filter.init(timeTable.getDefinition());
            Assert.assertEquals(expression, PushdownResult.TABLE_KEY_LOOKUP_COST,
                    estimate(dtColumn, filter, timeTable.getRowSet()));
            assertTableEquals(scanned(filter), pushedDown(filter));
        }
        final WhereFilter lower = WhereFilterFactory.getExpression("Timestamp >= '1970-01-01T00:00:00.000000505Z'");
        final WhereFilter upper = WhereFilterFactory.getExpression("Timestamp < '1970-01-01T00:00:00.00000155Z'");
        final Table both = new QueryTable(timeTable.getRowSet().copy().toTracking(),
                Map.of("Timestamp", timeTable.getColumnSource("Timestamp"))).where(Filter.and(lower, upper));
        // timestamps 510 through 1540
        Assert.assertEquals(RowSetFactory.fromRange(51, 154), both.getRowSet());
    }

    /**
     * For every pair of bounds and every combination of inclusive ends, the range pushdown on the {@link Instant}
     * column and on its {@code long} reinterpretation selects exactly the rows a scan selects, from the whole table and
     * from a selection that excludes some rows.
     */
    private void assertRangePushdownMatchesScan(final long[] bounds) {
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        final RowSet whole = timeTable.getRowSet();
        try (final RowSet partial = whole.subSetByPositionRange(1, Math.max(2, whole.size() - 1))) {
            for (final RowSet selection : new RowSet[] {whole, partial}) {
                for (final long lowerBound : bounds) {
                    for (final long upperBound : bounds) {
                        for (int ends = 0; ends < 4; ++ends) {
                            final boolean lowerInclusive = (ends & 1) != 0;
                            final boolean upperInclusive = (ends & 2) != 0;
                            final WhereFilter instantRange =
                                    instantRange(lowerBound, upperBound, lowerInclusive, upperInclusive);
                            final WhereFilter longRange =
                                    longRange(lowerBound, upperBound, lowerInclusive, upperInclusive);
                            try (final RowSet expected = scannedRows(instantRange, selection, dtColumn)) {
                                final String description = instantRange + " over " + selection;
                                assertExactMatch(description, expected, pushdown(dtColumn, instantRange, selection));
                                assertExactMatch(description, expected, pushdown(column, longRange, selection));
                            }
                        }
                    }
                }
            }
        }
    }

    private static WhereFilter instantRange(
            final long lower, final long upper, final boolean lowerInclusive, final boolean upperInclusive) {
        final WhereFilter filter = new InstantRangeFilter("Timestamp", lower, upper, lowerInclusive, upperInclusive);
        filter.init(TableDefinition.of(ColumnDefinition.ofTime("Timestamp")));
        return filter;
    }

    private static WhereFilter longRange(
            final long lower, final long upper, final boolean lowerInclusive, final boolean upperInclusive) {
        final WhereFilter filter = new LongRangeFilter("Timestamp", lower, upper, lowerInclusive, upperInclusive);
        filter.init(TableDefinition.of(ColumnDefinition.ofLong("Timestamp")));
        return filter;
    }

    /** The rows of {@code selection} that {@code filter} selects when scanning {@code source}. */
    private RowSet scannedRows(final WhereFilter filter, final RowSet selection, final ColumnSource<?> source) {
        final Table scanTable = new QueryTable(timeTable.getRowSet().copy().toTracking(),
                Map.of("Timestamp", new NoPushdownColumnSourceWrapper<>(source)));
        return filter.filter(selection, scanTable.getRowSet(), scanTable, false);
    }

    private static MatchFilter instantFilter(final MatchOptions options, final Instant... values) {
        final MatchFilter filter = new MatchFilter(options, "Timestamp", (Object[]) values);
        filter.init(TableDefinition.of(ColumnDefinition.ofTime("Timestamp")));
        return filter;
    }

    private static MatchFilter longFilter(final MatchOptions options, final Long... values) {
        final MatchFilter filter = new MatchFilter(options, "Timestamp", (Object[]) values);
        filter.init(TableDefinition.of(ColumnDefinition.ofLong("Timestamp")));
        return filter;
    }

    private static void assertExactMatch(final RowSet expected, final PushdownResult result) {
        assertExactMatch("", expected, result);
    }

    private static void assertExactMatch(final String description, final RowSet expected,
            final PushdownResult result) {
        try (result) {
            Assert.assertEquals(description, expected, result.match());
            Assert.assertTrue(description, result.maybeMatch().isEmpty());
        }
    }

    private static PushdownResult pushdown(
            final ColumnSource<?> source,
            final WhereFilter filter,
            final RowSet selection) {
        return pushdown(source, filter, selection, Long.MAX_VALUE);
    }

    private static PushdownResult pushdown(
            final ColumnSource<?> source,
            final WhereFilter filter,
            final RowSet selection,
            final long costCeiling) {
        final MutableObject<PushdownResult> result = new MutableObject<>();
        ((AbstractColumnSource<?>) source).pushdownFilter(filter, selection, false,
                PushdownFilterContext.NO_PUSHDOWN_CONTEXT, costCeiling, new ImmediateJobScheduler(),
                result::setValue, failure -> {
                    throw new AssertionError(failure);
                });
        return result.getValue();
    }

    private static long estimate(final ColumnSource<?> source, final WhereFilter filter, final RowSet selection) {
        final MutableLong cost = new MutableLong();
        ((AbstractColumnSource<?>) source).estimatePushdownFilterCost(filter, selection, false,
                PushdownFilterContext.NO_PUSHDOWN_CONTEXT, new ImmediateJobScheduler(), cost::setValue, failure -> {
                    throw new AssertionError(failure);
                });
        return cost.longValue();
    }

    /**
     * Filter a static table of the time table's current rows, reading the time table's own timestamp column.
     */
    private Table pushedDown(final WhereFilter filter) {
        return new QueryTable(timeTable.getRowSet().copy().toTracking(),
                Map.of("Timestamp", timeTable.getColumnSource("Timestamp"))).where(filter.copy());
    }

    /**
     * Filter a static table of the time table's current rows, through a timestamp column that declines pushdown.
     */
    private Table scanned(final WhereFilter filter) {
        return new QueryTable(timeTable.getRowSet().copy().toTracking(),
                Map.of("Timestamp", new NoPushdownColumnSourceWrapper<>(timeTable.getColumnSource("Timestamp"))))
                .where(filter.copy());
    }

    @Test
    public void testIndexing() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.assertEquals(timeTable.size(), 201);

        final DataIndexer dataIndexer = DataIndexer.of(timeTable.getRowSet());

        // Create the index for this table and column.
        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true);
                final RowSet intersectRowSet = RowSetFactory.fromRange(100, 109)) {
            DataIndexer.getOrCreateDataIndex(timeTable, "Timestamp");

            final BasicDataIndex dataIndex =
                    dataIndexer.getDataIndex(dtColumn).transform(
                            DataIndexTransformer.builder()
                                    .intersectRowSet(intersectRowSet)
                                    .build());
            final Table indexTable = dataIndex.table();

            Assert.assertEquals(indexTable.size(), 10);
            try (final CloseableIterator<Instant> keyIt = indexTable.columnIterator(dataIndex.keyColumnNames().get(0));
                    final CloseableIterator<RowSet> rsIt = indexTable.columnIterator(dataIndex.rowSetColumnName())) {
                while (keyIt.hasNext()) {
                    final Instant key = keyIt.next();
                    final RowSet rs = rsIt.next();
                    Assert.assertEquals(rs.size(), 1);
                    Assert.assertEquals(dtColumn.get(rs.firstRowKey()), key);
                }
            }

            final Table riTable = timeTable.updateView(List.of(
                    new ReinterpretedColumn<>("Timestamp", Instant.class, "longTimestamp", long.class)));

            // Create the index for this table and column.
            DataIndexer.getOrCreateDataIndex(riTable, "longTimestamp");

            final BasicDataIndex longDataIndex =
                    Objects.requireNonNull(DataIndexer.getDataIndex(riTable, "longTimestamp")).transform(
                            DataIndexTransformer.builder()
                                    .intersectRowSet(intersectRowSet)
                                    .build());
            final Table longIndexTable = longDataIndex.table();

            Assert.assertEquals(longIndexTable.size(), 10);
            try (final CloseablePrimitiveIteratorOfLong keyIt = new ChunkedLongColumnIterator(
                    longIndexTable.getColumnSource(longDataIndex.keyColumnNames().get(0), long.class),
                    longIndexTable.getRowSet());
                    final CloseableIterator<RowSet> rsIt =
                            longIndexTable.columnIterator(longDataIndex.rowSetColumnName())) {
                while (keyIt.hasNext()) {
                    final long key = keyIt.nextLong();
                    final RowSet rs = rsIt.next();
                    Assert.assertEquals(rs.size(), 1);
                    Assert.assertEquals(column.getLong(rs.firstRowKey()), key);
                }
            }
        }
    }

    @Test
    public void testFillChunkUnordered() {
        build(TimeTable.newBuilder().period(10));
        final ColumnSource<Instant> dtColumn = timeTable.getColumnSource("Timestamp");
        tick(0);
        tick(2000);
        Assert.assertEquals(timeTable.size(), 201);

        // noinspection unchecked
        final FillUnordered<Values> fillDtColumn = (FillUnordered<Values>) dtColumn;
        Assert.assertTrue(fillDtColumn.providesFillUnordered());
        // noinspection unchecked
        final FillUnordered<Values> fillLongColumn = (FillUnordered<Values>) column;
        Assert.assertTrue(fillLongColumn.providesFillUnordered());

        try (final WritableLongChunk<RowKeys> keys = WritableLongChunk.makeWritableChunk(10)) {
            keys.setSize(0);
            keys.add(109);
            keys.add(100);
            keys.add(103);
            keys.add(102);
            keys.add(109);
            keys.add(106);
            keys.add(100);

            // curr Instant
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableObjectChunk<Instant, Any> dest = WritableObjectChunk.makeWritableChunk(10)) {
                fillDtColumn.fillChunkUnordered(context, dest, keys);
                Assert.assertEquals(dest.size(), keys.size());
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.assertEquals(dest.get(ii), dtColumn.get(keys.get(ii)));
                }
            }

            // prev Instant
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableObjectChunk<Instant, Any> dest = WritableObjectChunk.makeWritableChunk(10)) {
                fillDtColumn.fillPrevChunkUnordered(context, dest, keys);
                Assert.assertEquals(dest.size(), keys.size());
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.assertEquals(dest.get(ii), dtColumn.get(keys.get(ii)));
                }
            }

            // curr long
            try (final ChunkSource.FillContext context = column.makeFillContext(10);
                    final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(10)) {
                fillLongColumn.fillChunkUnordered(context, dest, keys);
                Assert.assertEquals(dest.size(), keys.size());
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.assertEquals(dest.get(ii), column.getLong(keys.get(ii)));
                }
            }

            // prev long
            try (final ChunkSource.FillContext context = dtColumn.makeFillContext(10);
                    final WritableLongChunk<Values> dest = WritableLongChunk.makeWritableChunk(10)) {
                fillLongColumn.fillPrevChunkUnordered(context, dest, keys);
                Assert.assertEquals(dest.size(), keys.size());
                for (int ii = 0; ii < keys.size(); ++ii) {
                    Assert.assertEquals(dest.get(ii), column.getLong(keys.get(ii)));
                }
            }
        }
    }

    @Test
    public void testConcurrentConstruction() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests(false);
        build(TimeTable.newBuilder().period(10));
        clock.now = 1;
        try (final SafeCloseable ignored = updateGraph::completeCycleForUnitTests) {
            updateGraph.refreshUpdateSourceForUnitTests(updateSourceCombiner);
            updateGraph.markSourcesRefreshedForUnitTests();
        }
    }
}
