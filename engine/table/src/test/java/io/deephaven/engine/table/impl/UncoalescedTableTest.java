//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.util.Optional;
import java.util.function.Supplier;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link UncoalescedTable#coalescedIfAvailable()} and its {@link UncoalescedTableImpl} override.
 */
public class UncoalescedTableTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    private static final TableDefinition DEFINITION = TableDefinition.of(ColumnDefinition.ofInt("X"));

    @Test
    public void testDefaultIsAlwaysEmpty() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final NonMemoizingUncoalescedTable uncoalesced = new NonMemoizingUncoalescedTable();
            assertFalse(uncoalesced.coalescedIfAvailable().isPresent());
            // The base implementation memoizes nothing, so there is never anything to reuse
            uncoalesced.coalesce();
            assertFalse(uncoalesced.coalescedIfAvailable().isPresent());
        }
    }

    @Test
    public void testEmptyBeforeCoalesce() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final TestUncoalescedTable uncoalesced = new TestUncoalescedTable(true, this::refreshingResult);
            assertFalse(uncoalesced.coalescedIfAvailable().isPresent());
            // The whole point is to avoid forcing the deferred work
            assertEquals(0, uncoalesced.coalesceCount);
        }
    }

    @Test
    public void testPresentAfterCoalesce() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final TestUncoalescedTable uncoalesced = new TestUncoalescedTable(true, this::refreshingResult);
            final Table coalesced = uncoalesced.coalesce();
            assertEquals(1, uncoalesced.coalesceCount);

            final Optional<Table> available = uncoalesced.coalescedIfAvailable();
            assertTrue(available.isPresent());
            assertSame(coalesced, available.get());
            assertEquals(1, uncoalesced.coalesceCount);
        }
    }

    @Test
    public void testPresentAfterSetCoalesced() {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final TestUncoalescedTable uncoalesced = new TestUncoalescedTable(true, this::refreshingResult);
            final Table result = refreshingResult();
            uncoalesced.setCoalescedForTest(result);

            assertSame(result, uncoalesced.coalescedIfAvailable().orElseThrow(AssertionError::new));
            assertEquals(0, uncoalesced.coalesceCount);
        }
    }

    @Test
    public void testStaticResultNeedsNoScope() {
        final TestUncoalescedTable uncoalesced;
        final Table coalesced;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            uncoalesced = new TestUncoalescedTable(false, UncoalescedTableTest::staticResult);
            coalesced = uncoalesced.coalesce();
        }
        // Static results require no liveness management, so they remain available after the creating scope is released
        assertSame(coalesced, uncoalesced.coalescedIfAvailable().orElseThrow(AssertionError::new));
    }

    @Test
    public void testResultIsManagedByEnclosingScope() {
        final TestUncoalescedTable uncoalesced;
        final Table coalesced;
        final SingletonLivenessManager tableHolder;
        final SingletonLivenessManager resultHolder;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            uncoalesced = new TestUncoalescedTable(true, this::refreshingResult);
            coalesced = uncoalesced.coalesce();
            // Keep the result (and the uncoalesced table) alive past the creating scope, without using a scope
            tableHolder = new SingletonLivenessManager(uncoalesced);
            resultHolder = new SingletonLivenessManager(coalesced);
        }
        try {
            assertLive(coalesced);

            final LivenessScope callerScope = new LivenessScope();
            try (final SafeCloseable ignored = LivenessScopeStack.open(callerScope, false)) {
                assertSame(coalesced, uncoalesced.coalescedIfAvailable().orElseThrow(AssertionError::new));
            }

            // callerScope must now be the sole owner of the result: dropping the standalone holder must not kill it...
            resultHolder.release();
            assertLive(coalesced);
            // ...but releasing callerScope must.
            callerScope.release();
            assertNotLive(coalesced);
        } finally {
            tableHolder.release();
        }
    }

    @Test
    public void testEmptyAfterResultReleased() {
        final TestUncoalescedTable uncoalesced;
        final Table coalesced;
        final SingletonLivenessManager holder;
        final LivenessScope resultScope = new LivenessScope();
        try (final SafeCloseable ignored = LivenessScopeStack.open(resultScope, false)) {
            uncoalesced = new TestUncoalescedTable(true, this::refreshingResult);
            coalesced = uncoalesced.coalesce();
            holder = new SingletonLivenessManager(uncoalesced);
        }
        try {
            resultScope.release();
            assertNotLive(coalesced);

            // The hard reference to the memoized result survives, but it is no longer usable
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                assertFalse(uncoalesced.coalescedIfAvailable().isPresent());
            }
            assertEquals(1, uncoalesced.coalesceCount);
        } finally {
            holder.release();
        }
    }

    private static void assertLive(@NotNull final Table table) {
        assertTrue(table.tryRetainReference());
        table.dropReference();
    }

    private static void assertNotLive(@NotNull final Table table) {
        assertFalse(table.tryRetainReference());
    }

    private Table refreshingResult() {
        return TstUtils.testRefreshingTable(i(0, 1, 2).toTracking(), intCol("X", 1, 2, 3));
    }

    private static Table staticResult() {
        return TableTools.newTable(DEFINITION, intCol("X", 1, 2, 3));
    }

    /**
     * A direct {@link UncoalescedTable} that does not memoize its coalesced result, and thus inherits the default
     * {@link UncoalescedTable#coalescedIfAvailable()}.
     */
    private static final class NonMemoizingUncoalescedTable extends UncoalescedTable<NonMemoizingUncoalescedTable> {

        private NonMemoizingUncoalescedTable() {
            super(DEFINITION, "NonMemoizingUncoalescedTable");
        }

        @Override
        public Table coalesce() {
            return staticResult();
        }

        @Override
        protected NonMemoizingUncoalescedTable copy() {
            return new NonMemoizingUncoalescedTable();
        }
    }

    private static final class TestUncoalescedTable extends UncoalescedTableImpl<TestUncoalescedTable> {

        private final boolean refreshing;
        private final Supplier<Table> resultSupplier;

        private int coalesceCount;

        private TestUncoalescedTable(final boolean refreshing, @NotNull final Supplier<Table> resultSupplier) {
            super(DEFINITION, "TestUncoalescedTable");
            this.refreshing = refreshing;
            this.resultSupplier = resultSupplier;
            setRefreshing(refreshing);
        }

        @Override
        protected Table doCoalesce() {
            ++coalesceCount;
            final Table result = resultSupplier.get();
            copyAttributes((BaseTable<?>) result, CopyAttributeOperation.Coalesce);
            return result;
        }

        @Override
        protected TestUncoalescedTable copy() {
            return new TestUncoalescedTable(refreshing, resultSupplier);
        }

        private void setCoalescedForTest(final Table coalesced) {
            setCoalesced(coalesced);
        }
    }
}
