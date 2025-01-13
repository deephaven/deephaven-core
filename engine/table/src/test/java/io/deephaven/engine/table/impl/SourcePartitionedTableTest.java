//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.testutil.locations.DependentRegistrar;
import io.deephaven.engine.testutil.locations.TableBackedTableLocationKey;
import io.deephaven.engine.testutil.locations.TableBackedTableLocationProvider;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.FindExceptionCause;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashSet;
import java.util.Set;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class SourcePartitionedTableTest extends RefreshingTableTestCase {

    private CapturingUpdateGraph updateGraph;
    private SafeCloseable contextCloseable;

    @Override
    public void setUp() throws Exception {
        if (null == ProcessEnvironment.tryGet()) {
            ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                    "SourcePartitionedTableTest", new StreamLoggerImpl());
        }
        super.setUp();
        setExpectError(false);

        updateGraph = new CapturingUpdateGraph(ExecutionContext.getContext().getUpdateGraph().cast());
        contextCloseable = updateGraph.getContext().open();
    }

    @Override
    public void tearDown() throws Exception {
        contextCloseable.close();
        super.tearDown();
    }

    private QueryTable p1;
    private QueryTable p2;
    private QueryTable p3;
    private QueryTable p4;

    private DependentRegistrar registrar;
    private TableBackedTableLocationProvider tlp;

    private SourcePartitionedTable setUpData() {
        p1 = testRefreshingTable(i(0, 1, 2, 3).toTracking(),
                stringCol("Sym", "aa", "bb", "aa", "bb"),
                intCol("intCol", 10, 20, 40, 60),
                doubleCol("doubleCol", 0.1, 0.2, 0.4, 0.6));
        p1.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);

        p2 = testRefreshingTable(i(0, 1, 2, 3).toTracking(),
                stringCol("Sym", "cc", "dd", "cc", "dd"),
                intCol("intCol", 100, 200, 400, 600),
                doubleCol("doubleCol", 0.1, 0.2, 0.4, 0.6));
        p2.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);

        p3 = testRefreshingTable(i(0, 1, 2, 3).toTracking(),
                stringCol("Sym", "ee", "ff", "ee", "ff"),
                intCol("intCol", 1000, 2000, 4000, 6000),
                doubleCol("doubleCol", 0.1, 0.2, 0.4, 0.6));
        p3.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);

        p4 = testRefreshingTable(i(0, 1, 2, 3).toTracking(),
                stringCol("Sym", "gg", "hh", "gg", "hh"),
                intCol("intCol", 10000, 20000, 40000, 60000),
                doubleCol("doubleCol", 0.1, 0.2, 0.4, 0.6));
        p4.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);

        registrar = new DependentRegistrar();
        tlp = new TableBackedTableLocationProvider(
                registrar,
                true,
                TableUpdateMode.ADD_REMOVE,
                TableUpdateMode.ADD_REMOVE,
                p1, p2);

        return new SourcePartitionedTable(p1.getDefinition(),
                t -> t,
                tlp,
                true,
                true,
                l -> true);
    }

    private void verifyStringColumnContents(Table table, String columnName, String... expectedValues) {
        final ColumnSource<String> columnSource = table.getColumnSource(columnName);
        final Set<String> expectedSym = Set.of(expectedValues);

        final Set<String> actualSym = new HashSet<>();
        try (final CloseableIterator<String> symIterator = ChunkedColumnIterator.make(
                columnSource, table.getRowSet(), 1024)) {
            symIterator.forEachRemaining(actualSym::add);
        }
        assertEquals(expectedSym, actualSym);
    }

    @Test
    public void testAddAndRemoveLocations() {
        final SourcePartitionedTable spt = setUpData();

        final Table partitionTable = spt.table();

        final Table ptSummary = spt.merge().selectDistinct("Sym");

        assertEquals(2, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p1);
            assertTableEquals(tableIt.next(), p2);
        }

        // Verify the contents of the downstream summary table
        verifyStringColumnContents(ptSummary, "Sym", "aa", "bb", "cc", "dd");

        ////////////////////////////////////////////
        // Remove the p1 location
        ////////////////////////////////////////////

        ImmutableTableLocationKey[] tlks = tlp.getTableLocationKeys()
                .stream().sorted().toArray(ImmutableTableLocationKey[]::new);
        final ImmutableTableLocationKey tlk0 = tlks[0];

        tlp.removeTableLocationKey(tlk0);
        tlp.refresh();

        // We've removed location 0, should be gone from the location key list
        assertFalse(new HashSet<>(tlp.getTableLocationKeys()).contains(tlk0));

        // Since we haven't been through an update cycle, we should still be able to retrieve the location for this key.
        assertTrue(tlp.hasTableLocationKey(tlk0));
        assertNotNull(tlp.getTableLocation(tlk0));

        // Verify the contents of the downstream summary table haven't changed yet
        verifyStringColumnContents(ptSummary, "Sym", "aa", "bb", "cc", "dd");

        updateGraph.getDelegate().startCycleForUnitTests(false);
        updateGraph.refreshSources();
        updateGraph.markSourcesRefreshedForUnitTests();
        registrar.run();

        // Verify the contents of the downstream summary table haven't changed yet
        verifyStringColumnContents(ptSummary, "Sym", "aa", "bb", "cc", "dd");

        // flush the notifications and verify changes are now visible
        updateGraph.getDelegate().flushAllNormalNotificationsForUnitTests();
        verifyStringColumnContents(ptSummary, "Sym", "cc", "dd");

        // Finish the cycle and retest
        updateGraph.getDelegate().completeCycleForUnitTests();
        assertFalse(tlp.hasTableLocationKey(tlk0));

        assertEquals(1, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p2);
        }

        ////////////////////////////////////////////
        // Add a new location (p3)
        ////////////////////////////////////////////

        tlp.add(p3);

        updateGraph.getDelegate().startCycleForUnitTests(false);
        updateGraph.refreshSources();
        updateGraph.markSourcesRefreshedForUnitTests();
        registrar.run();
        // Verify the contents of the downstream summary table haven't changed yet
        verifyStringColumnContents(ptSummary, "Sym", "cc", "dd");

        // flush the notifications and verify changes are now visible
        updateGraph.getDelegate().flushAllNormalNotificationsForUnitTests();
        verifyStringColumnContents(ptSummary, "Sym", "cc", "dd", "ee", "ff");

        // Finish the cycle
        updateGraph.getDelegate().completeCycleForUnitTests();

        assertEquals(2, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p2);
            assertTableEquals(tableIt.next(), p3);
        }

        ////////////////////////////////////////////
        // Add a new location (p4) and remove p2
        ////////////////////////////////////////////

        tlks = tlp.getTableLocationKeys().stream().sorted().toArray(ImmutableTableLocationKey[]::new);
        tlp.removeTableLocationKey(tlks[0]);
        tlp.add(p4);

        updateGraph.getDelegate().startCycleForUnitTests(false);
        updateGraph.refreshSources();
        updateGraph.markSourcesRefreshedForUnitTests();
        registrar.run();
        // Verify the contents of the downstream summary table haven't changed yet
        verifyStringColumnContents(ptSummary, "Sym", "cc", "dd", "ee", "ff");

        // flush the notifications and verify changes are now visible
        updateGraph.getDelegate().flushAllNormalNotificationsForUnitTests();
        verifyStringColumnContents(ptSummary, "Sym", "ee", "ff", "gg", "hh");

        // Finish the cycle
        updateGraph.getDelegate().completeCycleForUnitTests();

        assertEquals(2, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p3);
            assertTableEquals(tableIt.next(), p4);
        }

        /*
         * Set up a complicated table location management test, where we create a new table under a scope, add it to the
         * SPT, then drop the table and verify that the table location is destroyed only after the scope is released.
         */
        final TableLocation location5;
        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(), true)) {
            final QueryTable p5 = testRefreshingTable(i(0, 1, 2, 3).toTracking(),
                    stringCol("Sym", "ii", "jj", "ii", "jj"),
                    intCol("intCol", 10000, 20000, 40000, 60000),
                    doubleCol("doubleCol", 0.1, 0.2, 0.4, 0.6));
            p5.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
            tlp.add(p5);

            updateGraph.getDelegate().startCycleForUnitTests(false);
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            registrar.run();
            // Verify the contents of the downstream summary table haven't changed yet
            verifyStringColumnContents(ptSummary, "Sym", "ee", "ff", "gg", "hh");

            // flush the notifications and verify changes are now visible
            updateGraph.getDelegate().flushAllNormalNotificationsForUnitTests();
            verifyStringColumnContents(ptSummary, "Sym", "ee", "ff", "gg", "hh", "ii", "jj");

            // Finish the cycle
            updateGraph.getDelegate().completeCycleForUnitTests();

            assertEquals(3, partitionTable.size());
            try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
                assertTableEquals(tableIt.next(), p3);
                assertTableEquals(tableIt.next(), p4);
                assertTableEquals(tableIt.next(), p5);
            }

            tlks = tlp.getTableLocationKeys().stream().sorted().toArray(ImmutableTableLocationKey[]::new);
            final ImmutableTableLocationKey tlk_p5 = tlks[2];
            location5 = tlp.getTableLocation(tlk_p5);
            assertTrue(location5.getRowSet() != null && location5.getRowSet().size() == 4);

            ////////////////////////////////////////////
            // remove the p5 key from the SPT
            ////////////////////////////////////////////

            tlp.removeTableLocationKey(tlk_p5);
            tlp.refresh();

            // We've removed location 5, should be gone from the location key list
            assertFalse(new HashSet<>(tlp.getTableLocationKeys()).contains(tlk_p5));

            // Since we haven't been through an update cycle, we can still retrieve the location for this key.
            assertTrue(tlp.hasTableLocationKey(tlk_p5));
            assertNotNull(tlp.getTableLocation(tlk_p5));

            updateGraph.getDelegate().startCycleForUnitTests(false);
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            registrar.run();
            // Verify the contents of the downstream summary table haven't changed yet
            verifyStringColumnContents(ptSummary, "Sym", "ee", "ff", "gg", "hh", "ii", "jj");

            // flush the notifications and verify changes are now visible
            updateGraph.getDelegate().flushAllNormalNotificationsForUnitTests();
            verifyStringColumnContents(ptSummary, "Sym", "ee", "ff", "gg", "hh");

            // Finish the cycle
            updateGraph.getDelegate().completeCycleForUnitTests();

            // After the cycle cleanup, this location should not be available
            assertFalse(tlp.hasTableLocationKey(tlk_p5));

            // The location associated with p5 should still be valid, because it is held by p5 RCSM and p5 is in scope
            assertTrue(location5.getRowSet() != null && location5.getRowSet().size() == 4);

            assertEquals(2, partitionTable.size());
            try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
                assertTableEquals(tableIt.next(), p3);
                assertTableEquals(tableIt.next(), p4);
            }
        }

        // The scope has been released, p5 should be dead so verify that the location associated with p5 has been
        // cleaned up
        assertNull(location5.getRowSet());

        // Prove that we propagate normal errors. This is a little tricky, we can't test for errors.size == 1 because
        // The TableBackedTableLocation has a copy() of the p3 table which is itself a leaf. Erroring P3 will
        // cause one error to come from the copied table, and one from the merged() table. We just need to validate
        // that the exceptions we see are a ConstituentTableException and an ISE
        allowingError(() -> updateGraph.getDelegate().runWithinUnitTestCycle(() -> {
            p3.notifyListenersOnError(new IllegalStateException("This is a test error"), null);
            updateGraph.markSourcesRefreshedForUnitTests();
        }, false), errors -> errors.size() == 1 &&
                FindExceptionCause.isOrCausedBy(errors.get(0), IllegalStateException.class).isPresent());
    }

    /**
     * This is a test for PR 4537, where SourceTable removes itself from the wrong refresh provider
     */
    @Test
    public void testRemoveAndFail() {
        final SourcePartitionedTable spt = setUpData();

        final Table partitionTable = spt.table();
        assertEquals(2, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p1);
            assertTableEquals(tableIt.next(), p2);
        }

        TableBackedTableLocationKey[] tlks = tlp.getTableLocationKeys().stream()
                .sorted()
                .map(k -> (TableBackedTableLocationKey) k)
                .toArray(TableBackedTableLocationKey[]::new);

        final RowSet rowSet = p1.getRowSet().copy();
        removeRows(tlks[0].table(), rowSet);
        tlp.getTableLocation(tlks[0]).refresh();

        // First cause the location to fail. for example size -> 0 because "someone deleted my data"
        // We expect an error here because the table itself is going to fail.
        allowingError(() -> updateGraph.getDelegate().runWithinUnitTestCycle(() -> {
            // This should process the pending update from the refresh above.
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            registrar.run();
        }, false), errors -> errors.size() == 1 &&
                FindExceptionCause.isOrCausedBy(errors.get(0), AssertionFailure.class).isPresent());
        getUpdateErrors().clear();

        // Then delete it for real
        tlp.removeTableLocationKey(tlks[0]);
        tlp.refresh();

        // We should NOT get an error here because the failed table should have removed itself from the registrar.
        updateGraph.getDelegate().runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            registrar.run();
        }, false);

        assertEquals(1, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p2);
        }
    }

    /**
     * This test verifies that after a location is removed any attempt to read from it, current or previous values will
     * fail.
     */
    @Test
    public void testCantReadPrev() {
        final SourcePartitionedTable spt = setUpData();

        final Table merged = spt.merge();
        final Table aggs = merged.sumBy("Sym");

        Table expected = TableTools.merge(p1, p2).sumBy("Sym");
        assertTableEquals(expected, aggs);

        ImmutableTableLocationKey[] tlks = tlp.getTableLocationKeys()
                .stream().sorted().toArray(ImmutableTableLocationKey[]::new);
        tlp.removeTableLocationKey(tlks[0]);
        tlp.refresh();

        allowingError(() -> updateGraph.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            registrar.run();
        }, false), errors -> errors.stream().anyMatch(e -> FindExceptionCause.isOrCausedBy(e,
                InvalidatedRegionException.class).isPresent()) &&
                errors.stream().anyMatch(e -> FindExceptionCause.isOrCausedBy(e,
                        TableLocationRemovedException.class).isPresent()));
        getUpdateErrors().clear();
    }
}
