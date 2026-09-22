//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.api.RawString;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.NoPushdownColumnSourceWrapper;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterMatcher;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.TableTimeConversions;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the pushdown path through {@link UnionSourceManager}: a merged table whose constituents differ in whether
 * they support pushdown.
 */
public class UnionSourcePushdownTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final int ROWS_PER_CONSTITUENT = 100;

    /** A single-column filter routes through {@link UnionColumnSource} to the manager. */
    private static final String UNION_FILTER = "Z == 42";

    private static final String BARRIER = "narrow";

    /**
     * A constituent whose {@code Z} column is a single-value source, which supports pushdown at
     * {@link PushdownResult#TABLE_SINGLE_VALUE_COLUMN_COST}.
     */
    private static Table pushdownConstituent() {
        return TableTools.emptyTable(ROWS_PER_CONSTITUENT).update("X = (int) ii", "Z = 42");
    }

    /**
     * A constituent whose column sources are wrapped so that they are not {@code AbstractColumnSource}s, and so get no
     * pushdown matcher. Half of its rows pass {@link #UNION_FILTER}.
     */
    private static Table nonPushdownConstituent() {
        final QueryTable inMemory = (QueryTable) TableTools.emptyTable(ROWS_PER_CONSTITUENT)
                .update("X = (int) ii", "Z = ii % 2 == 0 ? 42 : 7");
        final Map<String, ColumnSource<?>> wrapped = new LinkedHashMap<>();
        inMemory.getColumnSourceMap().forEach(
                (name, source) -> wrapped.put(name, new NoPushdownColumnSourceWrapper<>(source)));
        return new QueryTable(inMemory.getRowSet(), wrapped);
    }

    private static Table mergedTable() {
        return TableTools.merge(pushdownConstituent(), nonPushdownConstituent());
    }

    /** A pushdown-capable source that records the ceiling and executed cost each pushdown call is given. */
    private static final class RecordingSingleValueSource extends IntegerSingleValueSource {
        final List<long[]> calls = new ArrayList<>();

        @Override
        public void pushdownFilter(
                final WhereFilter filter,
                final RowSet selection,
                final boolean usePrev,
                final PushdownFilterContext context,
                final long costCeiling,
                final JobScheduler jobScheduler,
                final Consumer<PushdownResult> onComplete,
                final Consumer<Exception> onError) {
            calls.add(new long[] {costCeiling, context.executedFilterCost()});
            super.pushdownFilter(filter, selection, usePrev, context, costCeiling, jobScheduler, onComplete, onError);
        }
    }

    /** Like {@link #pushdownConstituent()}, with {@code Z} backed by the given recording source. */
    private static Table recordingConstituent(final RecordingSingleValueSource z) {
        z.set(42);
        final Table x = TableTools.emptyTable(ROWS_PER_CONSTITUENT).update("X = (int) ii");
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>();
        sources.put("X", x.getColumnSource("X"));
        sources.put("Z", z);
        return new QueryTable(x.getRowSet(), sources);
    }

    private static WhereFilter initializedFilter(final Table table, final String expression) {
        final WhereFilter filter = WhereFilter.of(RawString.of(expression));
        filter.init(table.getDefinition());
        return filter;
    }

    private static List<ColumnSource<?>> filterSources(final Table table, final WhereFilter filter) {
        return filter.getColumns().stream().map(table::getColumnSource).collect(Collectors.toList());
    }

    private static PushdownFilterMatcher unionMatcher(final Table merged, final WhereFilter filter) {
        final PushdownFilterMatcher matcher =
                PushdownFilterMatcher.getPushdownFilterMatcher(filter, filterSources(merged, filter));
        assertThat(matcher).isNotNull();
        return matcher;
    }

    private static long estimateCost(
            final PushdownFilterMatcher matcher,
            final WhereFilter filter,
            final RowSet selection,
            final PushdownFilterContext context) {
        return estimateCost(matcher, filter, selection, false, context);
    }

    private static long estimateCost(
            final PushdownFilterMatcher matcher,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final PushdownFilterContext context) {
        final AtomicLong cost = new AtomicLong(-1);
        final AtomicReference<Exception> error = new AtomicReference<>();
        matcher.estimatePushdownFilterCost(filter, selection, usePrev, context, new ImmediateJobScheduler(),
                cost::set, error::set);
        assertThat(error.get()).isNull();
        return cost.get();
    }

    private static PushdownResult pushdown(
            final PushdownFilterMatcher matcher,
            final WhereFilter filter,
            final RowSet selection,
            final PushdownFilterContext context) {
        return pushdown(matcher, filter, selection, false, Long.MAX_VALUE, context);
    }

    private static PushdownResult pushdown(
            final PushdownFilterMatcher matcher,
            final WhereFilter filter,
            final RowSet selection,
            final boolean usePrev,
            final long costCeiling,
            final PushdownFilterContext context) {
        final AtomicReference<PushdownResult> result = new AtomicReference<>();
        final AtomicReference<Exception> error = new AtomicReference<>();
        matcher.pushdownFilter(filter, selection, usePrev, context, costCeiling, new ImmediateJobScheduler(),
                result::set, error::set);
        assertThat(error.get()).isNull();
        assertThat(result.get()).isNotNull();
        return result.get();
    }

    /**
     * The driver estimates every filter's cost against the full input before any filter runs, then pushes down against
     * whatever earlier filters left. The result must describe only rows in the selection it was asked about.
     */
    @Test
    public void resultIsSubsetOfSelectionWhenSelectionNarrows() {
        final Table merged = mergedTable();
        final WhereFilter filter = initializedFilter(merged, UNION_FILTER);
        final PushdownFilterMatcher matcher = unionMatcher(merged, filter);

        final RowSet full = merged.getRowSet();
        try (final PushdownFilterContext context =
                matcher.makePushdownFilterContext(filter, filterSources(merged, filter));
                // The last ten rows of the pushdown constituent and the first ten of the other.
                final RowSet narrowed =
                        full.subSetByPositionRange(ROWS_PER_CONSTITUENT - 10, ROWS_PER_CONSTITUENT + 10);
                final RowSet narrowedPushdownRows =
                        full.subSetByPositionRange(ROWS_PER_CONSTITUENT - 10, ROWS_PER_CONSTITUENT);
                final RowSet narrowedNonPushdownRows =
                        full.subSetByPositionRange(ROWS_PER_CONSTITUENT, ROWS_PER_CONSTITUENT + 10)) {
            assertThat(estimateCost(matcher, filter, full, context)).isLessThan(PushdownResult.UNSUPPORTED_ACTION_COST);

            try (final PushdownResult result = pushdown(matcher, filter, narrowed, context)) {
                // Every row of the pushdown constituent has Z == 42.
                assertThat(result.match()).isEqualTo(narrowedPushdownRows);
                // The other constituent's rows are "maybe", but only those that were selected.
                assertThat(result.maybeMatch()).isEqualTo(narrowedNonPushdownRows);
            }

            // A repeat call against a different narrowing sees only its own selection.
            try (final RowSet tail =
                    full.subSetByPositionRange(2 * ROWS_PER_CONSTITUENT - 10, 2 * ROWS_PER_CONSTITUENT);
                    final PushdownResult result = pushdown(matcher, filter, tail, context)) {
                assertThat(result.match().isEmpty()).isTrue();
                assertThat(result.maybeMatch()).isEqualTo(tail);
            }
        }
    }

    /**
     * The driver runs a pushdown under the ceiling of the next filter, records that ceiling on the context, and may run
     * the same filter again under a higher one. Each constituent must see the ceiling of the current call and, on its
     * own context, the cost already executed, so that it can skip the steps it has taken.
     */
    @Test
    public void constituentsSeeEachCallsCeilingAndTheExecutedCost() {
        final RecordingSingleValueSource z = new RecordingSingleValueSource();
        final Table merged = TableTools.merge(recordingConstituent(z), nonPushdownConstituent());
        final WhereFilter filter = initializedFilter(merged, UNION_FILTER);
        final PushdownFilterMatcher matcher = unionMatcher(merged, filter);

        final RowSet full = merged.getRowSet();
        final long firstCeiling = PushdownResult.TABLE_SINGLE_VALUE_COLUMN_COST + 1;
        try (final PushdownFilterContext context =
                matcher.makePushdownFilterContext(filter, filterSources(merged, filter))) {
            estimateCost(matcher, filter, full, context);

            pushdown(matcher, filter, full, false, firstCeiling, context).close();
            context.updateExecutedFilterCost(firstCeiling);
            pushdown(matcher, filter, full, false, Long.MAX_VALUE, context).close();
        }

        assertThat(z.calls).hasSize(2);
        assertThat(z.calls.get(0)).containsExactly(firstCeiling, 0L);
        assertThat(z.calls.get(1)).containsExactly(Long.MAX_VALUE, firstCeiling);
    }

    /**
     * A concurrent {@code where()} on a refreshing merged table filters previous values. Mid-cycle, after the merged
     * table has processed an append to a constituent, the previous selection must be resolved against the previous
     * constituent layout.
     */
    @Test
    public void previousValuesAreFilteredAgainstThePreviousLayout() {
        final QueryTable refreshing = TstUtils.testRefreshingTable(
                RowSetFactory.flat(ROWS_PER_CONSTITUENT).toTracking(),
                TableTools.intCol("X", IntStream.range(0, ROWS_PER_CONSTITUENT).toArray()),
                TableTools.intCol("Z", IntStream.range(0, ROWS_PER_CONSTITUENT).map(ii -> ii % 2 == 0 ? 42 : 7)
                        .toArray()));
        final Table merged = TableTools.merge(pushdownConstituent(), refreshing);
        final WhereFilter filter = initializedFilter(merged, UNION_FILTER);
        final PushdownFilterMatcher matcher = unionMatcher(merged, filter);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.startCycleForUnitTests();
        try {
            final RowSet added = RowSetFactory.fromRange(ROWS_PER_CONSTITUENT, ROWS_PER_CONSTITUENT + 9);
            TstUtils.addToTable(refreshing, added, TableTools.intCol("X", new int[10]),
                    TableTools.intCol("Z", new int[10]));
            refreshing.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
            updateGraph.flushAllNormalNotificationsForUnitTests();

            final TrackingRowSet current = merged.getRowSet();
            final RowSet previous = current.prev();
            assertThat(current.size()).isEqualTo(2 * ROWS_PER_CONSTITUENT + 10);
            assertThat(previous.size()).isEqualTo(2 * ROWS_PER_CONSTITUENT);

            try (final PushdownFilterContext context =
                    matcher.makePushdownFilterContext(filter, filterSources(merged, filter));
                    final RowSet previousPushdownRows = previous.subSetByPositionRange(0, ROWS_PER_CONSTITUENT);
                    final RowSet previousOtherRows =
                            previous.subSetByPositionRange(ROWS_PER_CONSTITUENT, 2 * ROWS_PER_CONSTITUENT)) {
                assertThat(estimateCost(matcher, filter, previous, true, context))
                        .isLessThan(PushdownResult.UNSUPPORTED_ACTION_COST);
                try (final PushdownResult result = pushdown(matcher, filter, previous, true, Long.MAX_VALUE, context)) {
                    assertThat(result.match()).isEqualTo(previousPushdownRows);
                    assertThat(result.maybeMatch()).isEqualTo(previousOtherRows);
                }
            }
        } finally {
            updateGraph.completeCycleForUnitTests();
        }
    }

    /**
     * The driver constructs every filter's context before estimating any of their costs, and closes them all if
     * anything fails in between. A context that was never initialized must therefore be closeable.
     */
    @Test
    public void unInitializedContextIsCloseable() {
        final Table merged = mergedTable();
        final WhereFilter filter = initializedFilter(merged, UNION_FILTER);
        final PushdownFilterMatcher matcher = unionMatcher(merged, filter);

        final PushdownFilterContext context = matcher.makePushdownFilterContext(filter, filterSources(merged, filter));
        assertThat(context).isInstanceOf(UnionSourceManager.UnionSourcePushdownFilterContext.class);
        context.close();
    }

    /**
     * The driver records the cost ceiling a pushdown ran under on the context it handed out, so that a repeat
     * invocation skips the steps already taken. The union context must pass that on to its constituents' contexts,
     * which are the ones that consult it.
     */
    @Test
    public void executedFilterCostReachesConstituentContexts() {
        final Table merged = mergedTable();
        final WhereFilter filter = initializedFilter(merged, UNION_FILTER);
        final PushdownFilterMatcher matcher = unionMatcher(merged, filter);

        try (final PushdownFilterContext context =
                matcher.makePushdownFilterContext(filter, filterSources(merged, filter))) {
            estimateCost(matcher, filter, merged.getRowSet(), context);
            final UnionSourceManager.UnionSourcePushdownFilterContext unionContext =
                    (UnionSourceManager.UnionSourcePushdownFilterContext) context;
            assertThat(unionContext.contexts).isNotEmpty();

            context.updateExecutedFilterCost(1234L);

            assertThat(context.executedFilterCost()).isEqualTo(1234L);
            for (final PushdownFilterContext constituentContext : unionContext.contexts) {
                assertThat(constituentContext.executedFilterCost()).isEqualTo(1234L);
            }
        }
    }

    /**
     * The manager knows the merged table's sources by name; a reinterpretation of a union source is not among them. A
     * filter over one must decline pushdown rather than fail the {@code where()}.
     */
    @Test
    public void reinterpretedUnionSourceDeclinesPushdown() {
        final Table constituent = TableTools.emptyTable(ROWS_PER_CONSTITUENT)
                .update("T = DateTimeUtils.epochNanosToInstant(ii)");
        final Table merged = TableTools.merge(constituent, constituent);
        final ColumnSource<?> reinterpreted = merged.getColumnSource("T").reinterpret(long.class);
        assertThat(reinterpreted).isInstanceOf(UnionColumnSource.class);

        final Map<String, ColumnSource<?>> columnSources = new LinkedHashMap<>();
        columnSources.put("T", reinterpreted);
        final Table view = new QueryTable(merged.getRowSet(), columnSources);

        final WhereFilter filter = initializedFilter(view, "T > 50");
        final PushdownFilterMatcher matcher =
                PushdownFilterMatcher.getPushdownFilterMatcher(filter, filterSources(view, filter));
        assertThat(matcher).isSameAs(reinterpreted);
        try (final PushdownFilterContext context =
                matcher.makePushdownFilterContext(filter, filterSources(view, filter))) {
            assertThat(context).isSameAs(PushdownFilterContext.NO_PUSHDOWN_CONTEXT);
            assertThat(estimateCost(matcher, filter, view.getRowSet(), context))
                    .isEqualTo(PushdownResult.UNSUPPORTED_ACTION_COST);
        }

        assertTableEquals(view.select().where("T > 50"), view.where("T > 50"));
    }

    /**
     * The user-facing route to {@link UnionColumnSource}'s reinterpretation: the time conversions are
     * {@code updateView}s of a {@link io.deephaven.engine.table.impl.select.ReinterpretedColumn ReinterpretedColumn},
     * whose data view is {@code unionSource.reinterpret(long.class)} and becomes the result's column source directly. A
     * {@code where()} over the converted column then resolves its filter source to that reinterpretation.
     */
    @Test
    public void reinterpretedUnionSourceViaTimeConversionIsCorrect() {
        final Table constituent = TableTools.emptyTable(ROWS_PER_CONSTITUENT)
                .update("T = DateTimeUtils.epochNanosToInstant(ii)", "X = (long) (ii / 2)");
        final Table merged = TableTools.merge(constituent, constituent);
        final Table nanos = TableTimeConversions.asEpochNanos(merged, "Nanos = T");

        // The conversion is what produces a reinterpreted union source; without this the test proves nothing.
        assertThat(nanos.getColumnSource("Nanos")).isInstanceOf(UnionColumnSource.class);
        assertThat(nanos.getColumnSource("Nanos")).isNotSameAs(merged.getColumnSource("T"));

        // Pre-fix this threw IllegalArgumentException from makePushdownFilterContext, failing the whole where().
        assertTableEquals(nanos.select().where("Nanos > 50"), nanos.where("Nanos > 50"));

        // A multi-column filter is routed through the sources' shared manager instead, which must also decline.
        final WhereFilter twoColumn = initializedFilter(nanos, "Nanos > X + 25");
        assertThat(PushdownFilterMatcher.getPushdownFilterMatcher(twoColumn, filterSources(nanos, twoColumn)))
                .isNull();
        assertTableEquals(nanos.select().where("Nanos > X + 25"), nanos.where("Nanos > X + 25"));
    }

    /**
     * End to end: a selective filter runs first, so the union filter pushes down against a narrowed selection. Rows the
     * first filter eliminated must not come back.
     */
    @Test
    public void narrowingPreFilterIsCorrect() {
        final Table merged = mergedTable();
        // select() copies the columns into plain in-memory sources, so the oracle does not use union pushdown.
        final Table oracle = merged.select();

        final Filter query = Filter.and(
                RawString.of("X > 90").withDeclaredBarriers(BARRIER),
                RawString.of(UNION_FILTER).withRespectedBarriers(BARRIER));

        final Table expected = oracle.where(query);
        assertThat(expected.size()).isEqualTo(9 + 4);
        assertTableEquals(expected, merged.where(query));
    }
}
