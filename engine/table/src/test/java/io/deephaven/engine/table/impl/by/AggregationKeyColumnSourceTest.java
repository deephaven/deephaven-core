//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.NoPushdownColumnSourceWrapper;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;
import java.util.function.UnaryOperator;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.removeRows;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AggregationKeyColumnSourceTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final Instant T0 = DateTimeUtils.parseInstant("2026-01-01T00:00:00 NY");
    private static final ZoneId UTC = ZoneId.of("UTC");

    @Test
    public void testSingleKeyColumnOnly() {
        final Table source = TableTools.emptyTable(100).update("A = (int) (ii % 7)", "B = (int) (ii % 3)");
        assertTrue(source.selectDistinct("A").getColumnSource("A") instanceof AggregationKeyColumnSource);
        assertTrue(source.lastBy("A").getColumnSource("A") instanceof AggregationKeyColumnSource);
        final Table twoKeys = source.selectDistinct("A", "B");
        assertFalse(twoKeys.getColumnSource("A") instanceof AggregationKeyColumnSource);
        assertFalse(twoKeys.getColumnSource("B") instanceof AggregationKeyColumnSource);
    }

    @Test
    public void testStaticKeyTypes() {
        checkStatic(newTable(intCol("Key", 5, 3, NULL_INT, 9, 3, 1)), 3, 4, null, 9, 9);
        checkStatic(newTable(longCol("Key", 5L, 3L, Long.MIN_VALUE, 9L)), 3L, 4L, Long.MIN_VALUE);
        checkStatic(newTable(charCol("Key", 'a', 'b', 'c')), 'b', 'z');
        checkStatic(newTable(stringCol("Key", "a", "b", null, "c", "b")), "b", "q", null);
        checkStatic(newTable(doubleCol("Key", 1.5, -0.0, Double.NaN, 2.5)), 0.0, 2.5, 7.0);
        checkStatic(newTable(col("Key", new BigDecimal("1.0"), new BigDecimal("1.00"), BigDecimal.ONE)),
                new BigDecimal("1"));
    }

    @Test
    public void testStaticReinterpretedKeyTypes() {
        final Table base = TableTools.emptyTable(12).update("Base = (int) (ii % 5)");
        checkStatic(reinterpreted(true, withKeyFormula(base, "Key = Base == 3 ? null : DateTimeUtils.plus(T0, "
                + "(long) Base)")), instant(1), instant(4), null, instant(9));
        checkStatic(reinterpreted(true, withZonedKey(base, 3)), zoned(1), zoned(4), null, zoned(9));
        checkStatic(reinterpreted(true, withKeyFormula(base, "Key = Base == 3 ? null : Base % 2 == 0")), true, null);
        checkStatic(reinterpreted(true, withKeyFormula(base, "Key = Base % 2 == 0")), false);
        // a ZonedDateTime column without a fixed time zone is hashed as objects
        checkStatic(reinterpreted(false, withKeyFormula(base, "Key = Base == 3 ? null : DateTimeUtils.toZonedDateTime("
                + "DateTimeUtils.plus(T0, (long) Base), UTC)")), zoned(1), zoned(4), null, zoned(9));
    }

    @Test
    public void testNanMatch() {
        final Table distinct = newTable(doubleCol("Key", 1.5, Double.NaN, 2.5)).selectDistinct("Key");
        final MatchFilter filter =
                new MatchFilter(MatchOptions.builder().nanMatch(true).build(), "Key", Double.NaN, 2.5);
        final Table pushedDown = distinct.where(filter);
        assertEquals(2, pushedDown.size());
        assertTableEquals(scanned(distinct, filter), pushedDown);
    }

    @Test
    public void testCaseInsensitive() {
        final Table distinct = newTable(stringCol("Key", "a", "B", "c")).selectDistinct("Key");
        final MatchFilter filter =
                new MatchFilter(MatchOptions.builder().caseInsensitive(true).build(), "Key", "A", "b");
        assertEquals(2, distinct.where(filter).size());
    }

    @Test
    public void testRefreshingInt() {
        checkRefreshing(table -> withKeyFormula(table, "Key = Base"), false, base -> base);
    }

    @Test
    public void testRefreshingInstant() {
        checkRefreshing(table -> withKeyFormula(table, "Key = DateTimeUtils.plus(T0, (long) Base)"), true,
                AggregationKeyColumnSourceTest::instant);
    }

    @Test
    public void testRefreshingZonedDateTime() {
        checkRefreshing(table -> withZonedKey(table, NULL_INT), true, AggregationKeyColumnSourceTest::zoned);
    }

    @Test
    public void testRefreshingBoolean() {
        checkRefreshing(table -> withKeyFormula(table, "Key = Base == 11 ? null : Base % 3 == 0"), true,
                base -> base == 11 ? null : base % 3 == 0);
    }

    @Test
    public void testPreviousValuesDuringCycle() throws Exception {
        final QueryTable source = testRefreshingTable(i(2, 4).toTracking(),
                intCol("Key", 3, 5), intCol("Value", 1, 2));
        final QueryTable lastBy = (QueryTable) source.lastBy("Key");
        final MatchFilter filter = new MatchFilter(MatchOptions.REGULAR, "Key", 3, 11);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutionContext context = ExecutionContext.getContext();
        final ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            updateGraph.startCycleForUnitTests(false);
            addToTable(source, i(6), intCol("Key", 11), intCol("Value", 3));
            // before the aggregation's update, a where on another thread filters previous values
            final Table filtered = pool.submit(() -> {
                try (final SafeCloseable ignored = context.open()) {
                    return lastBy.where(filter.copy());
                }
            }).get(30, TimeUnit.SECONDS);
            assertEquals(1, filtered.size());

            source.notifyListeners(i(6), i(), i());
            updateGraph.markSourcesRefreshedForUnitTests();
            final long step = updateGraph.clock().currentStep();
            while (!lastBy.satisfied(step)) {
                assertTrue(updateGraph.flushOneNotificationForUnitTests());
            }

            // the aggregation has changed its hash table this cycle, so a probe of previous values is abandoned
            final AggregationKeyColumnSource<?> keySource =
                    (AggregationKeyColumnSource<?>) lastBy.getColumnSource("Key");
            filter.init(lastBy.getDefinition());
            try (final RowSet prevRows = lastBy.getRowSet().copyPrev();
                    final PushdownResult result = pushdown(keySource, filter, prevRows, true)) {
                assertEquals(prevRows, result.maybeMatch());
                assertTrue(result.match().isEmpty());
            }
            // the update is complete, so current values are probed; key 11 has the third state
            try (final PushdownResult result = pushdown(keySource, filter, lastBy.getRowSet(), false)) {
                assertEquals(i(0, 2), result.match());
                assertTrue(result.maybeMatch().isEmpty());
            }

            updateGraph.completeCycleForUnitTests();
            assertEquals(2, filtered.size());
            assertTableEquals(scanned(lastBy, filter), filtered);
        } finally {
            pool.shutdownNow();
        }
    }

    private static PushdownResult pushdown(
            final AggregationKeyColumnSource<?> keySource,
            final MatchFilter filter,
            final RowSet selection,
            final boolean usePrev) {
        final MutableObject<PushdownResult> result = new MutableObject<>();
        keySource.pushdownFilter(filter, selection, usePrev, PushdownFilterContext.NO_PUSHDOWN_CONTEXT,
                Long.MAX_VALUE, new ImmediateJobScheduler(), result::setValue, e -> {
                    throw new AssertionError(e);
                });
        return result.getValue();
    }

    /**
     * Follow regular and inverted matches on the last-by of a ticking table, whose key {@code addKey} computes from its
     * {@code Base} column as {@code toKey} does, through new keys, a key whose rows are all removed, and that key's
     * return.
     */
    private static void checkRefreshing(
            final UnaryOperator<Table> addKey,
            final boolean reinterpretedKey,
            final IntFunction<Object> toKey) {
        final QueryTable source = testRefreshingTable(i(2, 4, 6, 8).toTracking(),
                intCol("Base", 3, 5, 7, 3), intCol("Value", 1, 2, 3, 4));
        final Table lastBy = reinterpreted(reinterpretedKey, addKey.apply(source)).lastBy("Key");
        final MatchFilter matchFilter =
                new MatchFilter(MatchOptions.REGULAR, "Key", toKey.apply(3), toKey.apply(7), toKey.apply(11));
        final MatchFilter invertedFilter =
                new MatchFilter(MatchOptions.INVERTED, "Key", toKey.apply(3), toKey.apply(11));
        final Table matched = lastBy.where(matchFilter);
        final Table notMatched = lastBy.where(invertedFilter);
        final Runnable check = () -> {
            assertTableEquals(scanned(lastBy, matchFilter), matched);
            assertTableEquals(scanned(lastBy, invertedFilter), notMatched);
        };
        check.run();

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        // a new key, and a new row for an existing key
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(source, i(10, 12), intCol("Base", 11, 7), intCol("Value", 5, 6));
            source.notifyListeners(i(10, 12), i(), i());
        });
        check.run();

        // every row of base 3 is removed
        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(source, i(2, 8));
            source.notifyListeners(i(), i(2, 8), i());
        });
        check.run();

        // base 3 returns
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(source, i(14), intCol("Base", 3), intCol("Value", 7));
            source.notifyListeners(i(14), i(), i());
        });
        check.run();
    }

    private static Table withKeyFormula(final Table table, final String keyFormula) {
        ExecutionContext.getContext().getQueryScope().putParam("T0", T0);
        ExecutionContext.getContext().getQueryScope().putParam("UTC", UTC);
        return table.update(keyFormula);
    }

    /**
     * Add a {@code Key} column of {@code ZonedDateTime}s in a fixed time zone, {@code Base} nanoseconds after
     * {@link #T0}, or null where {@code Base} is {@code nullBase}.
     */
    private static Table withZonedKey(final Table table, final int nullBase) {
        ExecutionContext.getContext().getQueryScope().putParam("T0", T0);
        ExecutionContext.getContext().getQueryScope().putParam("NullBase", nullBase);
        final QueryTable withNanos = (QueryTable) table.update(
                "Nanos = Base == NullBase ? NULL_LONG : DateTimeUtils.epochNanos(T0) + Base");
        final QueryTable zoned = withNanos.withAdditionalColumns(Map.of("Key",
                ReinterpretUtils.longToZonedDateTimeSource(withNanos.getColumnSource("Nanos", long.class), UTC)));
        if (withNanos.isRefreshing()) {
            zoned.setRefreshing(true);
            withNanos.addUpdateListener(new BaseTable.ListenerImpl("zoned key", withNanos, zoned));
        }
        return zoned;
    }

    /**
     * Check whether aggregations hash {@code table}'s {@code Key} column reinterpreted as a primitive.
     */
    private static Table reinterpreted(final boolean expected, final Table table) {
        final ColumnSource<?> keySource = table.getColumnSource("Key");
        assertEquals(expected, keySource != ReinterpretUtils.maybeConvertToPrimitive(keySource));
        return table;
    }

    /**
     * Check a regular and an inverted match of {@code values} on the distinct and last-by results of {@code source}
     * against the same filters scanning the key column.
     */
    private static void checkStatic(final Table source, final Object... values) {
        final Table withValues = source.update("Value = i");
        for (final Table aggregated : new Table[] {source.selectDistinct("Key"), withValues.lastBy("Key")}) {
            for (final MatchOptions options : new MatchOptions[] {MatchOptions.REGULAR, MatchOptions.INVERTED}) {
                final MatchFilter filter = new MatchFilter(options, "Key", values);
                assertTableEquals(scanned(aggregated, filter), aggregated.where(filter));
            }
        }
    }

    /**
     * Filter a static copy of {@code table}'s current rows, through sources that decline pushdown.
     */
    private static Table scanned(final Table table, final MatchFilter filter) {
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>();
        table.getColumnSourceMap()
                .forEach((name, source) -> sources.put(name, new NoPushdownColumnSourceWrapper<>(source)));
        return new QueryTable(table.getRowSet().copy().toTracking(), sources).where(filter.copy());
    }

    private static Instant instant(final int base) {
        return DateTimeUtils.plus(T0, base);
    }

    private static Object zoned(final int base) {
        return DateTimeUtils.toZonedDateTime(instant(base), UTC);
    }
}
