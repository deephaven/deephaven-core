//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

/**
 * Tests that copies of a {@link DynamicWhereFilter} share one {@link SharedSetKernel}, rather than each rebuilding the
 * set and subscribing its own listener to the set table.
 */
public class TestSharedSetKernel {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static MatchPair[] pairs() {
        return MatchPairFactory.getExpressions("Z");
    }

    private static QueryTable refreshingSet() {
        return TstUtils.testRefreshingTable(i(0, 1).toTracking(), intCol("Z", 1, 2));
    }

    /**
     * Copying a filter must reuse the set table, kernel and listener, since pushdown copies a filter once per location
     * and rebuilding per copy is what this sharing exists to avoid.
     */
    @Test
    public void testCopiesShareTheSet() {
        final DynamicWhereFilter filter = new DynamicWhereFilter(refreshingSet(), true, pairs());
        final SharedSetKernel shared = filter.sharedSet();

        for (int ci = 0; ci < 5; ++ci) {
            final DynamicWhereFilter copy = filter.copy();
            assertSame("copy " + ci + " must share the set", shared, copy.sharedSet());
            assertSame("copy " + ci + " must share the kernel", shared.kernel(), copy.sharedSet().kernel());
        }
    }

    /**
     * A copy of an exclusion filter shares the set with its original: the kernel is inclusion-agnostic, and each filter
     * supplies its own inclusion when matching.
     */
    @Test
    public void testExclusionCopiesShareTheSet() {
        final DynamicWhereFilter filter = new DynamicWhereFilter(refreshingSet(), false, pairs());
        assertSame(filter.sharedSet(), filter.copy().sharedSet());
    }

    /**
     * Two independently constructed filters do not share, so one is not exposed to the other's lifetime.
     */
    @Test
    public void testIndependentFiltersDoNotShare() {
        final QueryTable setTable = refreshingSet();
        assertNotSame(
                new DynamicWhereFilter(setTable, true, pairs()).sharedSet(),
                new DynamicWhereFilter(setTable, true, pairs()).sharedSet());
    }

    /**
     * A filter nested inside a composed filter must still share its set when the composed filter is copied. Composed
     * filters are copied wholesale, so without this the sharing would be lost exactly where filters are combined.
     */
    @Test
    public void testCopiesThroughAComposedFilterShareTheSet() {
        final DynamicWhereFilter filter = new DynamicWhereFilter(refreshingSet(), true, pairs());
        final WhereFilter composed = ConjunctiveFilter.of(
                new MatchFilter(MatchOptions.REGULAR, "Other", (Object) 1), filter);

        final WhereFilter composedCopy = composed.copy();
        assertSame(filter.sharedSet(), nestedDynamicFilter(composedCopy).sharedSet());

        // And again from the copy, since pushdown copies repeatedly.
        assertSame(filter.sharedSet(), nestedDynamicFilter(composedCopy.copy()).sharedSet());
    }

    private static DynamicWhereFilter nestedDynamicFilter(final WhereFilter filter) {
        for (final WhereFilter component : ((ComposedFilter) filter).getComponentFilters()) {
            if (component instanceof DynamicWhereFilter) {
                return (DynamicWhereFilter) component;
            }
        }
        throw new AssertionError("No DynamicWhereFilter found in " + filter);
    }

    /**
     * Only filters that are actually driving a result register for set change notifications, and they deregister when
     * that result is released, so the shared set does not accumulate dead filters.
     */
    @Test
    public void testRegistrationFollowsResultLifetime() {
        final QueryTable source = TstUtils.testRefreshingTable(i(0, 1, 2).toTracking(), intCol("Z", 1, 2, 3));
        final DynamicWhereFilter filter = new DynamicWhereFilter(refreshingSet(), true, pairs());
        final SharedSetKernel shared = filter.sharedSet();

        // A filter that has never been given a recompute listener has nothing to recompute.
        assertEquals(0, shared.registeredFilterCount());

        // Each result is built in its own releasable scope, which is the only thing keeping its filter copy alive,
        // so releasing that scope must take the registration with it.
        final LivenessScope firstScope = new LivenessScope();
        final Table first;
        try (final SafeCloseable ignored = LivenessScopeStack.open(firstScope, false)) {
            first = source.where(filter.copy());
        }
        assertEquals(1, shared.registeredFilterCount());

        // Additional operations register their own copies.
        final LivenessScope secondScope = new LivenessScope();
        final Table second;
        try (final SafeCloseable ignored = LivenessScopeStack.open(secondScope, false)) {
            second = source.where(filter.copy());
        }
        assertEquals(2, shared.registeredFilterCount());

        // The results still track the set correctly.
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
        });
        assertEquals(2, first.size());
        assertEquals(2, second.size());

        // Releasing a result deregisters its filter, so the shared set does not accumulate filters whose results are
        // gone and which therefore have nothing to recompute.
        firstScope.release();
        assertEquals(1, shared.registeredFilterCount());
        secondScope.release();
        assertEquals(0, shared.registeredFilterCount());
    }

    /**
     * Registration is idempotent. A filter is given its recompute listener once per snapshot attempt, and a retried
     * instantiation would otherwise leave one registration per attempt for the same filter, each producing a redundant
     * recompute request.
     */
    @Test
    public void testRegistrationIsIdempotent() {
        final DynamicWhereFilter filter = new DynamicWhereFilter(refreshingSet(), true, pairs());
        final SharedSetKernel shared = filter.sharedSet();

        shared.addFilter(filter);
        assertEquals(1, shared.registeredFilterCount());
        shared.addFilter(filter);
        assertEquals(1, shared.registeredFilterCount());

        shared.removeFilter(filter);
        assertEquals(0, shared.registeredFilterCount());
    }
}
