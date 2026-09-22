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
import io.deephaven.engine.table.impl.select.DynamicWhereFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.ReindexingFilter;
import io.deephaven.engine.table.impl.select.UnsortedClockFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterDelegatingBase;
import io.deephaven.engine.testutil.filters.RowSetCapturingFilter;
import io.deephaven.engine.testutil.StepClock;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.time.DateTimeUtils.epochNanosToInstant;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link PushdownFilterMatcher}: the {@link PushdownFilterMatcher#canPushdownFilter(WhereFilter)} eligibility
 * gate, and {@link PushdownFilterMatcher#getPushdownFilterMatcher(WhereFilter, List)}, in particular the requirement
 * that {@code filterSources} be parallel to {@code filter.getColumns()} (DH-23106).
 */
public class PushdownFilterMatcherTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private Table table;
    private WhereFilter singleColumnFilter;
    private WhereFilter multiColumnFilter;

    /** A table with a clock column and an int column, for the eligibility-gate tests. */
    private Table clockTable;
    private StepClock clock;

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

        clockTable = TableTools.newTable(
                col("Timestamp", epochNanosToInstant(1000L), epochNanosToInstant(2000L), epochNanosToInstant(3000L)),
                intCol("Int", 1, 2, 3));
        clock = new StepClock(1000L, 2000L, 3000L);
    }

    /**
     * A {@link ReindexingFilter} must not be pushdown-eligible: pushdown may satisfy it entirely and skip the
     * {@code filter()} call it depends on for initialization (see {@link ReindexingFilter#canPushdown()}).
     */
    @Test
    public void testCanPushdownFilterRejectsReindexingFilter() {
        final UnsortedClockFilter filter = new UnsortedClockFilter("Timestamp", clock, true);
        filter.init(clockTable.getDefinition());

        assertFalse("a ReindexingFilter must not be pushed down: " + filter.getClass().getSimpleName(),
                PushdownFilterMatcher.canPushdownFilter(filter));
    }

    /**
     * The same gate reached through the wrappers {@code withDeclaredBarriers}, {@code withRespectedBarriers} and
     * {@code withSerial} produce. Unlike {@code ComposedFilter} and {@code WhereFilterInvertedImpl} -- both of which
     * reject {@link ReindexingFilter} components outright -- these wrappers accept one, so the gate must see through
     * them; {@link WhereFilterDelegatingBase} answers {@link WhereFilter#canPushdown()} from the wrapped filter.
     */
    @Test
    public void testCanPushdownFilterRejectsWrappedReindexingFilter() {
        for (final UnaryOperator<WhereFilter> wrapper : List.<UnaryOperator<WhereFilter>>of(
                f -> f.withDeclaredBarriers("BARRIER"),
                f -> f.withRespectedBarriers("BARRIER"),
                WhereFilter::withSerial)) {
            final WhereFilter filter = wrapper.apply(new UnsortedClockFilter("Timestamp", clock, true));
            filter.init(clockTable.getDefinition());

            assertFalse("a wrapped ReindexingFilter must not be pushed down: " + filter.getClass().getSimpleName(),
                    PushdownFilterMatcher.canPushdownFilter(filter));
        }
    }

    /**
     * {@link DynamicWhereFilter} performs its own data-index-driven evaluation and binds its set-table subscription to
     * the instance, so it opts out through {@link WhereFilter#canPushdown()}.
     */
    @Test
    public void testCanPushdownFilterRejectsDynamicWhereFilter() {
        final Table setTable = TableTools.newTable(intCol("Int", 1, 2));
        final WhereFilter filter = new DynamicWhereFilter(setTable, true, new MatchPair("Int", "Int"));
        filter.init(clockTable.getDefinition());

        assertFalse("a DynamicWhereFilter must not be pushed down", PushdownFilterMatcher.canPushdownFilter(filter));
    }

    /**
     * A composed filter is pushable only if every component is. The plain condition filter passes the gate on its own,
     * so the rejection below is attributable to the {@link DynamicWhereFilter} component.
     */
    @Test
    public void testCanPushdownFilterRejectsComposedFilterWithNonPushableComponent() {
        final WhereFilter pushable = WhereFilter.of(RawString.of("Int > 1"));
        pushable.init(clockTable.getDefinition());
        assertTrue("control: a plain condition filter is pushable", PushdownFilterMatcher.canPushdownFilter(pushable));

        final Table setTable = TableTools.newTable(intCol("Int", 1, 2));
        final WhereFilter composed = ConjunctiveFilter.of(
                WhereFilter.of(RawString.of("Int > 1")),
                new DynamicWhereFilter(setTable, true, new MatchPair("Int", "Int")));
        composed.init(clockTable.getDefinition());

        assertFalse("a composed filter with a non-pushable component must not be pushed down",
                PushdownFilterMatcher.canPushdownFilter(composed));
    }

    /** {@code Int = 1 || ii % 2 == 0}: a disjunction, which {@code where()} cannot split into separate filters. */
    private static Filter virtualRowVariableDisjunction() {
        return Filter.or(RawString.of("Int = 1"), RawString.of("ii % 2 == 0"));
    }

    /**
     * A composed filter must report that it uses virtual row variables when any component does. Without this, a
     * disjunction containing an {@code ii} filter passes the eligibility gate and is evaluated against a data index
     * table, where {@code ii} means index-table positions rather than source positions.
     */
    @Test
    public void testComposedFilterReportsVirtualRowVariables() {
        final WhereFilter filter = WhereFilter.of(virtualRowVariableDisjunction());
        filter.init(clockTable.getDefinition());

        assertTrue("a disjunction containing an `ii` filter uses virtual row variables",
                filter.hasVirtualRowVariables());
        assertFalse("a filter using `ii` must not be pushed down", PushdownFilterMatcher.canPushdownFilter(filter));
    }

    /**
     * The same through the delegating wrappers, which must answer {@link WhereFilter#hasVirtualRowVariables()} from the
     * wrapped filter.
     */
    @Test
    public void testCanPushdownFilterRejectsWrappedVirtualRowVariableDisjunction() {
        for (final UnaryOperator<WhereFilter> wrapper : List.<UnaryOperator<WhereFilter>>of(
                f -> f.withDeclaredBarriers("BARRIER"),
                f -> f.withRespectedBarriers("BARRIER"),
                WhereFilter::withSerial,
                RowSetCapturingFilter::new)) {
            final WhereFilter filter = wrapper.apply(WhereFilter.of(virtualRowVariableDisjunction()));
            filter.init(clockTable.getDefinition());

            assertTrue("wrapper must report the wrapped filter's virtual row variables: "
                    + filter.getClass().getSimpleName(), filter.hasVirtualRowVariables());
            assertFalse("a wrapped filter using `ii` must not be pushed down: " + filter.getClass().getSimpleName(),
                    PushdownFilterMatcher.canPushdownFilter(filter));
        }
    }

    /**
     * The test-utility wrapper {@link RowSetCapturingFilter} is a {@code WhereFilterDelegating} implementation outside
     * {@link WhereFilterDelegatingBase}, so it must delegate {@link WhereFilter#canPushdown()} itself.
     */
    @Test
    public void testCanPushdownFilterRejectsRowSetCapturingWrappedNonPushableFilter() {
        final WhereFilter filter = new RowSetCapturingFilter(new UnsortedClockFilter("Timestamp", clock, true));
        filter.init(clockTable.getDefinition());

        assertFalse("RowSetCapturingFilter must not make a non-pushable filter pushable",
                PushdownFilterMatcher.canPushdownFilter(filter));
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
}
