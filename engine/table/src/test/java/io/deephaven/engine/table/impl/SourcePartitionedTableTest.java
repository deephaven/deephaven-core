package io.deephaven.engine.table.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationRemovedException;
import io.deephaven.engine.table.impl.sources.ConstituentTableException;
import io.deephaven.engine.testutil.locations.DependentRegistrar;
import io.deephaven.engine.testutil.locations.TableBackedTableLocationProvider;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.FindExceptionCause;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class SourcePartitionedTableTest extends RefreshingTableTestCase {

    private ExtraControlledUpdateGraph updateGraph;
    private SafeCloseable myContext;

    private static final class ExtraControlledUpdateGraph extends PeriodicUpdateGraph {
        final List<Runnable> sources = new ArrayList<>();

        private ExtraControlledUpdateGraph() {
            super("TEST", true, 1000, 25, -1);
        }

        @Override
        public void addSource(@NotNull Runnable updateSource) {
            super.addSource(updateSource);
            sources.add(updateSource);
        }

        public void refreshSources() {
            sources.forEach(Runnable::run);
        }
    }

    @Override
    public void setUp() throws Exception {
        if (null == ProcessEnvironment.tryGet()) {
            ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                    "SourcePartitionedTableTest", new StreamLoggerImpl());
        }
        super.setUp();
        setExpectError(false);

        updateGraph = new ExtraControlledUpdateGraph();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
        updateGraph.setSerialTableOperationsSafe(true);
        myContext = ExecutionContext.getContext().withUpdateGraph(updateGraph).open();
    }

    @Override
    public void tearDown() throws Exception {
        myContext.close();
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

        final Table merged = spt.merge();
        final Table aggs = merged.countBy("Count", "Sym");

        Table expected = TableTools.merge(p1, p2).countBy("Count", "Sym");
        assertTableEquals(expected, aggs);

        ImmutableTableLocationKey[] tlks = tlp.getTableLocationKeys()
                .stream().sorted().toArray(ImmutableTableLocationKey[]::new);
        tlp.removeTableLocationKey(tlks[0]);
        tlp.refresh();

        allowingError(() -> updateGraph.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            registrar.run();
        }), errors -> errors.size() == 1 &&
                FindExceptionCause.findCause(errors.get(0),
                        TableLocationRemovedException.class) instanceof TableLocationRemovedException);
        getUpdateErrors().clear();

        expected = p2.countBy("Count", "Sym");
        assertTableEquals(expected, aggs);

        tlp.addPending(p3);
        tlp.refresh();
        updateGraph.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            registrar.run();
        });

        expected = TableTools.merge(p2, p3).countBy("Count", "Sym");
        assertTableEquals(expected, aggs);

        tlks = tlp.getTableLocationKeys().stream().sorted().toArray(ImmutableTableLocationKey[]::new);
        tlp.addPending(p4);
        tlp.removeTableLocationKey(tlks[0]);
        tlp.refresh();

        allowingError(() -> updateGraph.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            registrar.run();
        }), errors -> errors.size() == 1 &&
                FindExceptionCause.findCause(errors.get(0),
                        TableLocationRemovedException.class) instanceof TableLocationRemovedException);
        getUpdateErrors().clear();

        expected = TableTools.merge(p3, p4).countBy("Count", "Sym");
        assertTableEquals(expected, aggs);

        // Prove that we propagate normal errors. This is a little tricky, we can't test for errors.size == 1 because
        // The TableBackedTableLocation has a copy() of the p3 table which is itself a leaf. Erroring P3 will
        // cause one error to come from the copied table, and one from the merged() table. We just need to validate
        // that the exceptions we see are a ConstituentTableException and an ISE
        allowingError(() -> updateGraph.runWithinUnitTestCycle(
                () -> p3.notifyListenersOnError(new IllegalStateException("This is a test error"), null)),
                errors -> errors.stream()
                        .anyMatch(e -> FindExceptionCause.findCause(e,
                                IllegalStateException.class) instanceof IllegalStateException)
                        &&
                        errors.stream().anyMatch(e -> FindExceptionCause.findCause(e,
                                ConstituentTableException.class) instanceof ConstituentTableException));
    }

    /**
     * This test verifies that after a location is removed any attempt to read from it, current or previous
     * valueswill fail.
     */
    public void testCantReadPrev() {
        final SourcePartitionedTable spt = setUpData();

        final Table merged = spt.merge();
        final Table aggs = merged.sumBy("Sym");

        Table expected = TableTools.merge(p1, p2).sumBy("Count", "Sym");
        assertTableEquals(expected, aggs);

        ImmutableTableLocationKey[] tlks = tlp.getTableLocationKeys()
                .stream().sorted().toArray(ImmutableTableLocationKey[]::new);
        tlp.removeTableLocationKey(tlks[0]);
        tlp.refresh();

        allowingError(() -> updateGraph.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            registrar.run();
        }), errors -> errors.size() == 1 &&
                FindExceptionCause.findCause(errors.get(0),
                        TableLocationRemovedException.class) instanceof TableLocationRemovedException);
        getUpdateErrors().clear();
    }
}
