//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.SortedClockFilter;
import io.deephaven.engine.table.impl.select.UnsortedClockFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.testutil.StepClock;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

import static io.deephaven.engine.util.TableTools.instantCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PushdownFilterMatcher#getPushdownFilterMatcher(WhereFilter, List)}, in particular the requirement
 * that {@code filterSources} be parallel to {@code filter.getColumns()} (DH-23106), and for
 * {@link PushdownFilterMatcher#canPushdownFilter(WhereFilter)} (DH-23750).
 */
public class PushdownFilterMatcherTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private Table table;
    private WhereFilter singleColumnFilter;
    private WhereFilter multiColumnFilter;

    @Before
    public void setUp() {
        table = TableTools.newTable(
                stringCol("X", "A", "B", "C"),
                stringCol("Y", "D", "E", "F"));
        singleColumnFilter = new MatchFilter(MatchOptions.REGULAR, "X", "A");
        multiColumnFilter = ConjunctiveFilter.of(
                new MatchFilter(MatchOptions.REGULAR, "X", "A"),
                new MatchFilter(MatchOptions.REGULAR, "Y", "D"));
        singleColumnFilter.init(table.getDefinition());
        multiColumnFilter.init(table.getDefinition());
    }

    @Test
    public void testParallelSourcesAccepted() {
        final ColumnSource<?> csX = table.getColumnSource("X");
        final ColumnSource<?> csY = table.getColumnSource("Y");
        // Correctly-parallel lists must not throw, whatever matcher (or null) they resolve to.
        PushdownFilterMatcher.getPushdownFilterMatcher(singleColumnFilter, List.of(csX));
        PushdownFilterMatcher.getPushdownFilterMatcher(multiColumnFilter, List.of(csX, csY));
    }

    @Test
    public void testSingleColumnFilterWithEmptySourcesThrows() {
        // Before DH-23106 this threw IndexOutOfBoundsException from filterSources.get(0)
        assertThrows(AssertionFailure.class, () -> PushdownFilterMatcher.getPushdownFilterMatcher(
                singleColumnFilter, Collections.emptyList()));
    }

    @Test
    public void testMultiColumnFilterWithPartialSourcesThrows() {
        // Before DH-23106 a partial source list was silently passed to PushdownPredicateManager.getSharedPPM,
        // potentially selecting a shared PPM that does not cover all of the filter's columns
        final ColumnSource<?> csX = table.getColumnSource("X");
        assertThrows(AssertionFailure.class, () -> PushdownFilterMatcher.getPushdownFilterMatcher(
                multiColumnFilter, List.of(csX)));
    }

    // DH-23750: canPushdownFilter must reject filters that use virtual row variables anywhere (PD-033) and reindexing
    // filters (PD-034), however they are composed or wrapped.

    private static final Table ROW_VARIABLE_TABLE = TableTools.newTable(intCol("A", 0, 1, 2, 0, 1, 2));

    private static final Table CLOCK_TABLE = TableTools.newTable(
            instantCol("Timestamp", Instant.ofEpochMilli(1), Instant.ofEpochMilli(2), Instant.ofEpochMilli(3)));

    private static final List<UnaryOperator<WhereFilter>> WRAPPERS = List.of(
            WhereFilterInvertedImpl::of,
            WhereFilter::withSerial,
            f -> f.withDeclaredBarriers(new Object()),
            f -> f.withRespectedBarriers(new Object()));

    private static WhereFilter initialized(final Filter filter, final Table table) {
        final WhereFilter whereFilter = WhereFilter.of(filter);
        whereFilter.init(table.getDefinition());
        return whereFilter;
    }

    private static WhereFilter initialized(final WhereFilter filter, final Table table) {
        filter.init(table.getDefinition());
        return filter;
    }

    @Test
    public void testPlainFilterCanBePushedDown() {
        assertTrue(PushdownFilterMatcher.canPushdownFilter(
                initialized(RawString.of("A = 1"), ROW_VARIABLE_TABLE)));
    }

    @Test
    public void testVirtualRowVariableFilterIsRejected() {
        assertFalse(PushdownFilterMatcher.canPushdownFilter(
                initialized(RawString.of("ii % 2 == 0"), ROW_VARIABLE_TABLE)));
    }

    @Test
    public void testComposedVirtualRowVariableFilterIsRejected() {
        final Filter rowVariable = RawString.of("ii % 2 == 0");
        final Filter plain = RawString.of("A = 1");
        assertFalse("or", PushdownFilterMatcher.canPushdownFilter(
                initialized(Filter.or(plain, rowVariable), ROW_VARIABLE_TABLE)));
        assertFalse("and", PushdownFilterMatcher.canPushdownFilter(
                initialized(ConjunctiveFilter.of(WhereFilter.of(plain), WhereFilter.of(rowVariable)),
                        ROW_VARIABLE_TABLE)));
    }

    @Test
    public void testWrappedVirtualRowVariableFilterIsRejected() {
        for (final UnaryOperator<WhereFilter> wrapper : WRAPPERS) {
            final WhereFilter wrapped = wrapper.apply(WhereFilter.of(RawString.of("ii % 2 == 0")));
            assertFalse(wrapped.toString(), PushdownFilterMatcher.canPushdownFilter(
                    initialized(wrapped, ROW_VARIABLE_TABLE)));
            // and a wrapped composition
            final WhereFilter wrappedOr = wrapper.apply(
                    WhereFilter.of(Filter.or(RawString.of("A = 1"), RawString.of("ii % 2 == 0"))));
            assertFalse(wrappedOr.toString(), PushdownFilterMatcher.canPushdownFilter(
                    initialized(wrappedOr, ROW_VARIABLE_TABLE)));
        }
    }

    @Test
    public void testReindexingFilterIsRejected() {
        final StepClock clock = new StepClock(1000L, 2000L);
        assertFalse("unsorted", PushdownFilterMatcher.canPushdownFilter(
                initialized(new UnsortedClockFilter("Timestamp", clock, true), CLOCK_TABLE)));
        assertFalse("sorted", PushdownFilterMatcher.canPushdownFilter(
                initialized(new SortedClockFilter("Timestamp", clock, true), CLOCK_TABLE)));
    }

    @Test
    public void testWrappedReindexingFilterIsRejected() {
        final StepClock clock = new StepClock(1000L, 2000L);
        for (final UnaryOperator<WhereFilter> wrapper : List.<UnaryOperator<WhereFilter>>of(
                WhereFilter::withSerial,
                f -> f.withDeclaredBarriers(new Object()),
                f -> f.withRespectedBarriers(new Object()))) {
            final WhereFilter wrapped = wrapper.apply(new UnsortedClockFilter("Timestamp", clock, true));
            assertFalse(wrapped.toString(), PushdownFilterMatcher.canPushdownFilter(
                    initialized(wrapped, CLOCK_TABLE)));
        }
    }
}
