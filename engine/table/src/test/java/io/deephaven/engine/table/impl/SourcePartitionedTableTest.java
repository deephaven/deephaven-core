//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.InvalidatedRegionException;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationRemovedException;
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
                p1, p2);

        return new SourcePartitionedTable(p1.getDefinition(),
                t -> t,
                tlp,
                true,
                true,
                l -> true);
    }

    @Test
    public void testAddAndRemoveLocations() {
        final SourcePartitionedTable spt = setUpData();

        final Table partitionTable = spt.table();

        assertEquals(2, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p1);
            assertTableEquals(tableIt.next(), p2);
        }

        ImmutableTableLocationKey[] tlks = tlp.getTableLocationKeys()
                .stream().sorted().toArray(ImmutableTableLocationKey[]::new);
        tlp.removeTableLocationKey(tlks[0]);
        tlp.refresh();

        allowingError(() -> updateGraph.getDelegate().runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            registrar.run();
        }, false), errors -> errors.size() == 1 &&
                FindExceptionCause.isOrCausedBy(errors.get(0), TableLocationRemovedException.class).isPresent());
        getUpdateErrors().clear();

        assertEquals(1, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p2);
        }

        tlp.addPending(p3);
        tlp.refresh();
        updateGraph.getDelegate().runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            registrar.run();
        });

        assertEquals(2, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p2);
            assertTableEquals(tableIt.next(), p3);
        }

        tlks = tlp.getTableLocationKeys().stream().sorted().toArray(ImmutableTableLocationKey[]::new);
        tlp.addPending(p4);
        tlp.removeTableLocationKey(tlks[0]);
        tlp.refresh();

        allowingError(() -> updateGraph.getDelegate().runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            updateGraph.markSourcesRefreshedForUnitTests();
            registrar.run();
        }, false), errors -> errors.size() == 1 &&
                FindExceptionCause.isOrCausedBy(errors.get(0), TableLocationRemovedException.class).isPresent());
        getUpdateErrors().clear();

        assertEquals(2, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p3);
            assertTableEquals(tableIt.next(), p4);
        }

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
                FindExceptionCause.isOrCausedBy(errors.get(0), TableDataException.class).isPresent());
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
