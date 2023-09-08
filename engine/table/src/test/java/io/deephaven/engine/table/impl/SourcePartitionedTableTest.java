package io.deephaven.engine.table.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.InvalidatedRegionException;
import io.deephaven.engine.table.impl.locations.TableLocationRemovedException;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.locations.DependentRegistrar;
import io.deephaven.engine.testutil.locations.TableBackedTableLocationProvider;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.updategraph.NotificationAdapter;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.TableTools;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.FindExceptionCause;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.function.ThrowingRunnable;
import io.deephaven.util.locks.AwareFunctionalLock;
import io.deephaven.util.process.ProcessEnvironment;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class SourcePartitionedTableTest extends RefreshingTableTestCase {

    private CapturingUpdateGraph updateGraph;
    private SafeCloseable contextCloseable;

    private static final class CapturingUpdateGraph implements UpdateGraph {

        private final ControlledUpdateGraph delegate;

        private final ExecutionContext context;

        private final List<Runnable> sources = new ArrayList<>();

        private CapturingUpdateGraph(@NotNull final ControlledUpdateGraph delegate) {
            this.delegate = delegate;
            context = ExecutionContext.getContext().withUpdateGraph(this);
        }

        @Override
        public void addSource(@NotNull Runnable updateSource) {
            delegate.addSource(updateSource);
            sources.add(updateSource);
        }

        private void refreshSources() {
            sources.forEach(Runnable::run);
        }

        @Override
        public LogOutput append(LogOutput logOutput) {
            return logOutput.append("CapturingUpdateGraph of ").append(delegate);
        }

        @Override
        public boolean satisfied(final long step) {
            return delegate.satisfied(step);
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return this;
        }

        private Notification wrap(@NotNull final Notification notification) {
            return new NotificationAdapter(notification) {
                @Override
                public void run() {
                    try (final SafeCloseable ignored = context.open()) {
                        super.run();
                    }
                }
            };
        }

        @Override
        public void addNotification(@NotNull final Notification notification) {
            delegate.addNotification(wrap(notification));
        }

        @Override
        public void addNotifications(@NotNull final Collection<? extends Notification> notifications) {
            delegate.addNotifications(notifications.stream().map(this::wrap).collect(Collectors.toList()));
        }

        @Override
        public boolean maybeAddNotification(@NotNull final Notification notification, final long deliveryStep) {
            return delegate.maybeAddNotification(wrap(notification), deliveryStep);
        }

        @Override
        public String getName() {
            return "CapturingUpdateGraph";
        }

        @Override
        public AwareFunctionalLock sharedLock() {
            return delegate.sharedLock();
        }

        @Override
        public AwareFunctionalLock exclusiveLock() {
            return delegate.exclusiveLock();
        }

        @Override
        public LogicalClock clock() {
            return delegate.clock();
        }

        @Override
        public int parallelismFactor() {
            return delegate.parallelismFactor();
        }

        @Override
        public LogEntry logDependencies() {
            return delegate.logDependencies();
        }

        @Override
        public boolean currentThreadProcessesUpdates() {
            return delegate.currentThreadProcessesUpdates();
        }

        @Override
        public boolean serialTableOperationsSafe() {
            return delegate.serialTableOperationsSafe();
        }

        @Override
        public boolean setSerialTableOperationsSafe(final boolean newValue) {
            return delegate.serialTableOperationsSafe();
        }

        @Override
        public boolean supportsRefreshing() {
            return delegate.supportsRefreshing();
        }

        @Override
        public void requestRefresh() {
            delegate.requestRefresh();
        }

        @Override
        public void removeSource(@NotNull Runnable updateSource) {
            sources.remove(updateSource);
            delegate.removeSource(updateSource);
        }

        public <T extends Exception> void runWithinUnitTestCycle(@NotNull final ThrowingRunnable<T> runnable) throws T {
            delegate.runWithinUnitTestCycle(runnable);
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

        updateGraph = new CapturingUpdateGraph(ExecutionContext.getContext().getUpdateGraph().cast());
        contextCloseable = updateGraph.context.open();
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

        allowingError(() -> updateGraph.delegate.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            registrar.run();
        }), errors -> errors.size() == 1 &&
                FindExceptionCause.isOrCausedBy(errors.get(0),
                        TableLocationRemovedException.class).isPresent());
        getUpdateErrors().clear();

        assertEquals(1, partitionTable.size());
        try (final CloseableIterator<Table> tableIt = partitionTable.columnIterator("LocationTable")) {
            assertTableEquals(tableIt.next(), p2);
        }

        tlp.addPending(p3);
        tlp.refresh();
        updateGraph.delegate.runWithinUnitTestCycle(() -> {
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

        allowingError(() -> updateGraph.delegate.runWithinUnitTestCycle(() -> {
            updateGraph.refreshSources();
            registrar.run();
        }), errors -> errors.size() == 1 &&
                FindExceptionCause.isOrCausedBy(errors.get(0),
                        TableLocationRemovedException.class).isPresent());
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
        allowingError(() -> updateGraph.delegate.runWithinUnitTestCycle(
                () -> p3.notifyListenersOnError(new IllegalStateException("This is a test error"), null)),
                errors -> errors.size() == 1 &&
                        FindExceptionCause.isOrCausedBy(errors.get(0), IllegalStateException.class).isPresent());
    }

    /**
     * This test verifies that after a location is removed any attempt to read from it, current or previous values will
     * fail.
     */
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
            registrar.run();
        }), errors -> errors.stream().anyMatch(e -> FindExceptionCause.isOrCausedBy(e,
                InvalidatedRegionException.class).isPresent()) &&
                errors.stream().anyMatch(e -> FindExceptionCause.isOrCausedBy(e,
                        TableLocationRemovedException.class).isPresent()));
        getUpdateErrors().clear();
    }
}
