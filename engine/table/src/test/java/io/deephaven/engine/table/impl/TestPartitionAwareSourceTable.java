//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TestErrorNotification;
import io.deephaven.engine.testutil.TestNotification;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.SimpleTableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.qst.column.Column;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.vector.ObjectVector;
import org.jetbrains.annotations.NotNull;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.lang.ref.WeakReference;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.assertRowSetEquals;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link PartitionAwareSourceTable}.
 */
@SuppressWarnings({"AutoBoxing", "JUnit4AnnotatedMethodInJUnit3TestCase", "AnonymousInnerClassMayBeStatic"})
public class TestPartitionAwareSourceTable extends RefreshingTableTestCase {

    private static class TestKeySupplier extends ReferenceCountedLivenessNode
            implements LiveSupplier<ImmutableTableLocationKey> {

        private final ImmutableTableLocationKey key;

        private TableLocation tableLocation;

        TestKeySupplier(
                final ImmutableTableLocationKey key) {
            super(false);
            this.key = key;
        }

        @Override
        public ImmutableTableLocationKey get() {
            return key;
        }

        public synchronized void setTableLocation(final TableLocation tableLocation) {
            Assert.eqNull(this.tableLocation, "this.tableLocation");
            manage(tableLocation);
            this.tableLocation = tableLocation;
        }

        @OverridingMethodsMustInvokeSuper
        @Override
        protected synchronized void destroy() {
            super.destroy();
            tableLocation = null;
        }
    }

    private static final int NUM_COLUMNS = 5;
    private static final ColumnDefinition<String> PARTITIONING_COLUMN_DEFINITION =
            ColumnDefinition.ofString("Date").withPartitioning();
    private static final ColumnDefinition<Boolean> BOOLEAN_COLUMN_DEFINITION = ColumnDefinition.ofBoolean("Active");
    private static final ColumnDefinition<Character> CHARACTER_COLUMN_DEFINITION = ColumnDefinition.ofChar("Type");
    private static final ColumnDefinition<Integer> INTEGER_COLUMN_DEFINITION = ColumnDefinition.ofInt("Size");
    private static final ColumnDefinition<Double> DOUBLE_COLUMN_DEFINITION = ColumnDefinition.ofDouble("Price");

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            PARTITIONING_COLUMN_DEFINITION,
            BOOLEAN_COLUMN_DEFINITION,
            CHARACTER_COLUMN_DEFINITION,
            INTEGER_COLUMN_DEFINITION,
            DOUBLE_COLUMN_DEFINITION);

    private static final String[] INTERNAL_PARTITIONS = {"0", "1", "2", "1", "0", "1"};
    private static final String[] COLUMN_PARTITIONS = {"D0", "D1", "D0", "D3", "D2", "D0"};

    private static final long INDEX_INCREMENT = 1000;

    private SourceTableComponentFactory componentFactory;
    private ColumnSourceManager columnSourceManager;

    private ColumnSource<?>[] columnSources;

    private TableLocationProvider locationProvider;
    private ImmutableTableLocationKey[] tableLocationKeys;
    private TableLocation[] tableLocations;

    private TableLocationSubscriptionBuffer subscriptionBuffer;

    private Table coalesced;
    private TableUpdateListener listener;
    private final TestNotification notification = new TestNotification();
    private final TestErrorNotification errorNotification = new TestErrorNotification();

    private WritableRowSet expectedRowSet;

    private PartitionAwareSourceTable SUT;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        componentFactory = mock(SourceTableComponentFactory.class);
        columnSourceManager = mock(ColumnSourceManager.class);
        columnSources = TABLE_DEFINITION.getColumnStream().map(cd -> {
            final ColumnSource<?> mocked = mock(ColumnSource.class, cd.getName());
            checking(new Expectations() {
                {
                    allowing(mocked).getType();
                    will(returnValue(cd.getDataType()));
                    allowing(mocked).getComponentType();
                    will(returnValue(cd.getComponentType()));
                    allowing(mocked).getChunkType();
                    will(returnValue(ChunkType.fromElementType(cd.getDataType())));
                }
            });
            return mocked;
        }).toArray(ColumnSource[]::new);
        locationProvider = mock(TableLocationProvider.class);
        checking(new Expectations() {
            {
                allowing(locationProvider).getUpdateMode();
                will(returnValue(TableUpdateMode.ADD_REMOVE));
                allowing(locationProvider).getLocationUpdateMode();
                will(returnValue(TableUpdateMode.ADD_REMOVE));
            }
        });

        tableLocationKeys = IntStream.range(0, 6).mapToObj(tlki -> {
            final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
            partitions.put(PARTITIONING_COLUMN_DEFINITION.getName(), COLUMN_PARTITIONS[tlki]);
            partitions.put("__IP__", INTERNAL_PARTITIONS[tlki]);
            return new SimpleTableLocationKey(partitions);
        }).toArray(ImmutableTableLocationKey[]::new);
        tableLocations = new TableLocation[] {
                mock(TableLocation.class, "TL0"),
                mock(TableLocation.class, "TL1"),
                mock(TableLocation.class, "TL2"),
                mock(TableLocation.class, "TL3"),
                mock(TableLocation.class, "TL4"),
                mock(TableLocation.class, "TL5")
        };
        checking(new Expectations() {
            {
                allowing(locationProvider).supportsSubscriptions();
                will(returnValue(true));
                for (int li = 0; li < tableLocations.length; ++li) {
                    final TableLocation tableLocation = tableLocations[li];
                    allowing(locationProvider).getTableLocation(tableLocationKeys[li]);
                    will(returnValue(tableLocation));
                    allowing(tableLocation).getKey();
                    will(returnValue(tableLocationKeys[li]));
                    allowing(tableLocation).supportsSubscriptions();
                    will(returnValue(true));
                }
            }
        });
        listener = mock(TableUpdateListener.class);

        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(with(true), with(ColumnToCodecMappings.EMPTY),
                        with(equal(TABLE_DEFINITION.getColumns())));
                will(returnValue(columnSourceManager));
                allowing(columnSourceManager).tryRetainReference();
                will(returnValue(true));
                allowing(columnSourceManager).getWeakReference();
                will(returnValue(new WeakReference<>(columnSourceManager)));
                allowing(columnSourceManager).dropReference();
                allowing(columnSourceManager).getTableAttributes(with(any(TableUpdateMode.class)),
                        with(any(TableUpdateMode.class)));
                will(returnValue(Collections.EMPTY_MAP));

            }
        });

        expectedRowSet = RowSetFactory.empty();

        SUT = new PartitionAwareSourceTable(TABLE_DEFINITION, "", componentFactory, locationProvider,
                ExecutionContext.getContext().getUpdateGraph());
        assertIsSatisfied();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            allowLivenessRelease();
            super.tearDown();
        } finally {
            if (coalesced != null) {
                coalesced.dropReference();
                coalesced = null;
            }
        }
    }

    private void allowLivenessRelease() {
        checking(new Expectations() {
            {
                allowing(locationProvider).supportsSubscriptions();
                allowing(locationProvider).unsubscribe(with(any(TableLocationProvider.Listener.class)));
                will(returnValue(true));
                for (int li = 0; li < tableLocations.length; ++li) {
                    final TableLocation tableLocation = tableLocations[li];
                    allowing(tableLocation).supportsSubscriptions();
                    will(returnValue(true));
                    allowing(tableLocation).unsubscribe(with(any(TableLocation.Listener.class)));
                }
            }
        });
    }

    private Map<String, ? extends ColumnSource<?>> getIncludedColumnsMap(final int... indices) {
        return IntStream.of(indices)
                .mapToObj(ci -> new Pair<>(TABLE_DEFINITION.getColumns().get(ci).getName(), columnSources[ci]))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, Assert::neverInvoked, LinkedHashMap::new));
    }

    private ImmutableTableLocationKey[] locationKeysSlice(final int... indexes) {
        final ImmutableTableLocationKey[] slice = new ImmutableTableLocationKey[indexes.length];
        for (int ii = 0; ii < indexes.length; ++ii) {
            slice[ii] = tableLocationKeys[indexes[ii]];
        }
        return slice;
    }

    private Set<TableLocation> makePassingLocations(final int... indexes) {
        return Arrays.stream(indexes).mapToObj(li -> tableLocations[li])
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Test
    public void testInitialize() {
        doInitializeCheck(locationKeysSlice(1, 3), makePassingLocations(1, 3), false, true);
    }

    @Test
    public void testInitializeException() {
        doInitializeCheck(locationKeysSlice(1, 3), makePassingLocations(1, 3), true, true);
    }

    @Test
    public void testRefreshUnchanged() {
        doInitializeCheck(locationKeysSlice(1, 3), makePassingLocations(1, 3), false, true);
        doRefreshUnchangedCheck();
    }

    @Test
    public void testRefreshChanged() {
        doInitializeCheck(locationKeysSlice(1, 3), makePassingLocations(1, 3), false, true);
        doRefreshChangedCheck();
        doAddLocationsRefreshCheck(locationKeysSlice(5), makePassingLocations(5));
    }

    @Test
    public void testRefreshException() {
        try (final ErrorExpectation ignored = new ErrorExpectation()) {
            doInitializeCheck(locationKeysSlice(1, 3), makePassingLocations(1, 3), false, true);
            doRefreshExceptionCheck();
        }
    }

    private enum ConcurrentInstantiationType {
        Idle, UpdatingClosed, UpdatingOpen
    }

    private void doInitializeCheck(final ImmutableTableLocationKey[] tableLocationKeys,
            final Set<TableLocation> expectPassFilters, final boolean throwException, final boolean coalesceAndListen) {
        doInitializeCheck(tableLocationKeys, expectPassFilters, throwException, coalesceAndListen,
                ConcurrentInstantiationType.Idle);
    }

    private void doInitializeCheck(final ImmutableTableLocationKey[] tableLocationKeys,
            final Set<TableLocation> expectPassFilters, final boolean throwException, final boolean coalesceAndListen,
            @NotNull final ConcurrentInstantiationType ciType) {
        Assert.assertion(!(throwException && !coalesceAndListen), "!(throwException && !listen)");
        final TableDataException exception = new TableDataException("test");
        final RowSet toAdd =
                RowSetFactory.fromRange(expectedRowSet.lastRowKey() + 1,
                        expectedRowSet.lastRowKey() + INDEX_INCREMENT).toTracking();

        checking(new Expectations() {
            {
                oneOf(locationProvider).subscribe(with(any(TableLocationProvider.Listener.class)));
                will(new CustomAction("Supply locations") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        subscriptionBuffer = (TableLocationSubscriptionBuffer) invocation.getParameter(0);
                        Arrays.stream(tableLocationKeys).map(TestKeySupplier::new)
                                .forEach(subscriptionBuffer::handleTableLocationKeyAdded);
                        return null;
                    }
                });
                oneOf(columnSourceManager).initialize();
                if (throwException) {
                    will(throwException(exception));
                } else {
                    will(returnValue(toAdd));
                    oneOf(columnSourceManager).getColumnSources();
                    will(returnValue(getIncludedColumnsMap(0, 1, 2, 3, 4)));
                }
            }
        });
        expectPassFilters.forEach(tl -> checking(new Expectations() {
            {
                oneOf(columnSourceManager).addLocation(tl);
            }
        }));

        expectedRowSet.insert(toAdd);
        if (coalesceAndListen) {
            if (ciType == ConcurrentInstantiationType.UpdatingClosed
                    || ciType == ConcurrentInstantiationType.UpdatingOpen) {
                ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
            }
            try {
                coalesced = SUT.coalesce();
                coalesced.retainReference();
                ((QueryTable) coalesced).addUpdateListener(listener);
                if (throwException) {
                    fail("Expected exception");
                }
            } catch (TableDataException e) {
                if (throwException) {
                    return;
                } else {
                    throw exception;
                }
            }
            assertIsSatisfied();
            assertRowSetEquals(expectedRowSet, SUT.getRowSet());
            if (ciType == ConcurrentInstantiationType.UpdatingClosed) {
                ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast()
                        .completeCycleForUnitTests();
            }
        }
    }

    @Test
    public void testConcurrentInstantiationUpdating() {
        doInitializeCheck(locationKeysSlice(1, 3), makePassingLocations(1, 3), false, true,
                ConcurrentInstantiationType.UpdatingClosed);
        doRefreshChangedCheck();
    }

    @Test
    public void testConcurrentInstantiationUpdatingWithInitialCycleRefresh() {
        doInitializeCheck(locationKeysSlice(1, 3), makePassingLocations(1, 3), false, true,
                ConcurrentInstantiationType.UpdatingOpen);
        doRefreshChangedCheck();
    }

    private void doRefreshChangedCheck() {
        final RowSet toAdd =
                RowSetFactory.fromRange(expectedRowSet.lastRowKey() + 1,
                        expectedRowSet.lastRowKey() + INDEX_INCREMENT);
        checking(new Expectations() {
            {
                oneOf(columnSourceManager).refresh();
                will(returnValue(new TableUpdateImpl(toAdd.copy(), RowSetFactory.empty(), RowSetFactory.empty(),
                        RowSetShiftData.EMPTY, ModifiedColumnSet.ALL)));
                checking(new Expectations() {
                    {
                        oneOf(listener).getNotification(with(any(TableUpdateImpl.class)));
                        will(new CustomAction("check added") {
                            @Override
                            public Object invoke(Invocation invocation) {
                                final TableUpdate update =
                                        (TableUpdate) invocation.getParameter(0);
                                assertRowSetEquals(toAdd, update.added());
                                assertRowSetEquals(RowSetFactory.empty(), update.removed());
                                assertRowSetEquals(RowSetFactory.empty(), update.modified());
                                assertTrue(update.shifted().empty());
                                return notification;
                            }
                        });
                    }
                });
            }
        });

        notification.reset();
        if (ExecutionContext.getContext().getUpdateGraph().clock().currentState() == LogicalClock.State.Idle) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        }
        try {
            SUT.refresh();
        } finally {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
        }
        assertIsSatisfied();
        notification.assertInvoked();
        expectedRowSet.insert(toAdd);
        assertRowSetEquals(expectedRowSet, SUT.getRowSet());
    }

    private void doRefreshUnchangedCheck() {
        checking(new Expectations() {
            {
                oneOf(columnSourceManager).refresh();
                will(returnValue(
                        new TableUpdateImpl(RowSetFactory.empty(), RowSetFactory.empty(), RowSetFactory.empty(),
                                RowSetShiftData.EMPTY, ModifiedColumnSet.ALL)));
            }
        });

        notification.reset();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(SUT::refresh);
        assertIsSatisfied();
        notification.assertNotInvoked();

        assertRowSetEquals(expectedRowSet, SUT.getRowSet());
    }

    private void doRefreshExceptionCheck() {
        final TableDataException exception = new TableDataException("test");
        checking(new Expectations() {
            {
                oneOf(columnSourceManager).refresh();
                will(throwException(exception));
                oneOf(listener).getErrorNotification(with(any(TableDataException.class)),
                        with(any(PerformanceEntry.class)));
                will(new CustomAction("check exception") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        assertEquals(exception, invocation.getParameter(0));
                        return errorNotification;
                    }
                });
                oneOf(columnSourceManager).deliverError(with(any(TableDataException.class)),
                        with(any(PerformanceEntry.class)));
            }
        });

        errorNotification.reset();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            allowLivenessRelease();
            SUT.refresh();
            updateGraph.markSourcesRefreshedForUnitTests();
        }, false);
        assertIsSatisfied();
        errorNotification.assertInvoked();

        assertRowSetEquals(expectedRowSet, SUT.getRowSet());
    }

    private void doAddLocationsRefreshCheck(final ImmutableTableLocationKey[] tableLocationKeys,
            final Set<TableLocation> expectPassFilters) {
        Arrays.stream(tableLocationKeys).map(TestKeySupplier::new)
                .forEach(subscriptionBuffer::handleTableLocationKeyAdded);

        expectPassFilters.forEach(tl -> checking(new Expectations() {
            {
                oneOf(columnSourceManager).addLocation(tl);
            }
        }));

        doRefreshChangedCheck();
    }

    @Test
    public void testRedefinition() {
        ExecutionContext.getContext().getUpdateGraph().exclusiveLock().doLocked(this::doTestRedefinition);
    }

    private void doTestRedefinition() {
        // Note: We expect redefinition to make a new CSM, but no work until we force a coalesce by asking for column
        // sources
        final List<ColumnDefinition<?>> includedColumns1 = List.of(
                PARTITIONING_COLUMN_DEFINITION,
                CHARACTER_COLUMN_DEFINITION,
                INTEGER_COLUMN_DEFINITION,
                DOUBLE_COLUMN_DEFINITION);

        final Map<Class, ColumnSource> dataTypeToColumnSource = new HashMap<>();
        includedColumns1.forEach((final ColumnDefinition columnDefinition) -> {
            final ColumnSource columnSource =
                    mock(ColumnSource.class, "_CS_" + columnDefinition.getDataType().getSimpleName());
            dataTypeToColumnSource.put(columnDefinition.getDataType(), columnSource);
            checking(new Expectations() {
                {
                    allowing(columnSource).getType();
                    will(returnValue(columnDefinition.getDataType()));
                    allowing(columnSource).getComponentType();
                    will(returnValue(columnDefinition.getComponentType()));
                    allowing(columnSource).isStateless();
                    will(returnValue(true));
                }
            });
        });

        // Test 1: Drop a column
        // Setup the table
        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(with(true), with(ColumnToCodecMappings.EMPTY),
                        with(equal(includedColumns1)));
                will(returnValue(columnSourceManager));
            }
        });
        final Table dropColumnsResult1 = SUT.dropColumns(BOOLEAN_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(dropColumnsResult1 instanceof PartitionAwareSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {
            {
                oneOf(locationProvider).subscribe(with(any(TableLocationProvider.Listener.class)));
                will(new CustomAction("Supply no locations") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        return null;
                    }
                });
                oneOf(columnSourceManager).initialize();
                will(returnValue(RowSetFactory.empty().toTracking()));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(includedColumns1.stream()
                        .collect(Collectors.toMap(ColumnDefinition::getName,
                                cd -> dataTypeToColumnSource.get(cd.getDataType()), Assert::neverInvoked,
                                LinkedHashMap::new))));
            }
        });
        assertEquals(NUM_COLUMNS - 1, dropColumnsResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(dropColumnsResult1.getColumnSource(CHARACTER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult1.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult1.getColumnSource(DOUBLE_COLUMN_DEFINITION.getName()));

        // Test 2: Drop another column
        // Setup the table
        final List<ColumnDefinition<?>> includedColumns2 = List.of(
                PARTITIONING_COLUMN_DEFINITION,
                INTEGER_COLUMN_DEFINITION,
                DOUBLE_COLUMN_DEFINITION);
        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(with(true), with(ColumnToCodecMappings.EMPTY),
                        with(equal(includedColumns2)));
                will(returnValue(columnSourceManager));
            }
        });
        final Table dropColumnsResult2 = dropColumnsResult1.dropColumns(CHARACTER_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(dropColumnsResult2 instanceof PartitionAwareSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {
            {
                oneOf(locationProvider).subscribe(with(any(TableLocationProvider.Listener.class)));
                will(new CustomAction("Supply no locations") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        return null;
                    }
                });
                oneOf(columnSourceManager).initialize();
                will(returnValue(RowSetFactory.empty().toTracking()));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(includedColumns2.stream()
                        .collect(Collectors.toMap(ColumnDefinition::getName,
                                cd -> dataTypeToColumnSource.get(cd.getDataType()), Assert::neverInvoked,
                                LinkedHashMap::new))));
            }
        });
        assertEquals(NUM_COLUMNS - 2, dropColumnsResult2.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(dropColumnsResult2.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult2.getColumnSource(DOUBLE_COLUMN_DEFINITION.getName()));

        // Test 3: Rename a column
        // Nothing to setup for the table - the rename is deferred
        final Table renameColumnsResult1 = dropColumnsResult2.renameColumns("A=" + INTEGER_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(renameColumnsResult1 instanceof DeferredViewTable);
        // This will not force a coalesce, as dropColumnsResult2 is already coalesced.
        assertEquals(NUM_COLUMNS - 2, renameColumnsResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(renameColumnsResult1.getColumnSource("A"));
        assertNotNull(renameColumnsResult1.getColumnSource(DOUBLE_COLUMN_DEFINITION.getName()));

        // Test 4: Use view to slice us down to one column
        // Setup the table
        final List<ColumnDefinition<?>> includedColumns3 = List.of(
                INTEGER_COLUMN_DEFINITION,
                PARTITIONING_COLUMN_DEFINITION);
        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(with(true), with(ColumnToCodecMappings.EMPTY),
                        with(equal(includedColumns3)));
                will(returnValue(columnSourceManager));
            }
        });
        final Table viewResult1 = dropColumnsResult2.view(INTEGER_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(viewResult1 instanceof DeferredViewTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {
            {
                oneOf(locationProvider).subscribe(with(any(TableLocationProvider.Listener.class)));
                will(new CustomAction("Supply no locations") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        return null;
                    }
                });
                oneOf(columnSourceManager).initialize();
                will(returnValue(RowSetFactory.empty().toTracking()));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(includedColumns3.stream()
                        .collect(Collectors.toMap(ColumnDefinition::getName,
                                cd -> dataTypeToColumnSource.get(cd.getDataType()), Assert::neverInvoked,
                                LinkedHashMap::new))));
            }
        });
        assertEquals(NUM_COLUMNS - 4, viewResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(viewResult1.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));

        // Test 5: Add a new derived column on
        // Setup the table
        final Table viewResult2 = viewResult1.updateView(
                "SizeSquared=" + INTEGER_COLUMN_DEFINITION.getName() + '*' + INTEGER_COLUMN_DEFINITION.getName());
        assertTrue(viewResult2 instanceof DeferredViewTable);
        assertEquals(NUM_COLUMNS - 3, viewResult2.getColumnSources().size());
        assertNotNull(viewResult2.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));
        assertNotNull(viewResult2.getColumnSource("SizeSquared"));
        assertIsSatisfied();

        final Table viewResult3 = viewResult2.view("Result=SizeSquared");
        assertTrue(viewResult3 instanceof DeferredViewTable);
        assertEquals(NUM_COLUMNS - 4, viewResult3.getColumnSources().size());
        assertNotNull(viewResult3.getColumnSource("Result"));
        assertIsSatisfied();

        final Table viewResult4 = viewResult2.view("SizeSquared");
        assertTrue(viewResult4 instanceof DeferredViewTable);
        assertEquals(NUM_COLUMNS - 4, viewResult4.getColumnSources().size());
        assertNotNull(viewResult4.getColumnSource("SizeSquared"));
        assertIsSatisfied();
    }

    @Test
    public void testSelectDistinctDate() {
        final Set<TableLocation> passedLocations = makePassingLocations(1, 3, 5);
        final String[] expectedDistinctDates =
                IntStream.of(1, 3, 5).mapToObj(li -> COLUMN_PARTITIONS[li]).distinct().toArray(String[]::new);
        doInitializeCheck(locationKeysSlice(1, 3, 5), passedLocations, false, true);
        checking(new Expectations() {
            {
                oneOf(columnSourceManager).locationTable();
                will(returnValue(TableFactory.newTable(
                        Column.of(PARTITIONING_COLUMN_DEFINITION.getName(),
                                IntStream.of(1, 3, 5).mapToObj(li -> COLUMN_PARTITIONS[li]).toArray(String[]::new)))));
            }
        });
        final Table result = SUT.selectDistinct(PARTITIONING_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        final String columnName = PARTITIONING_COLUMN_DEFINITION.getName();
        final ObjectVector<String> distinctDatesVector = ColumnVectors.ofObject(result, columnName, String.class);
        assertEquals(expectedDistinctDates.length, distinctDatesVector.size());
        final String[] distinctDates = distinctDatesVector.toArray();
        Arrays.sort(expectedDistinctDates);
        Arrays.sort(distinctDates);
        assertArrayEquals(expectedDistinctDates, distinctDates);
    }

    @Test
    public void testSelectDistinctOther() {
        checking(new org.jmock.Expectations() {
            {
                oneOf(locationProvider).subscribe(with(any(TableLocationSubscriptionBuffer.class)));
                // noinspection resource
                oneOf(columnSourceManager).initialize();
                will(returnValue(RowSetFactory.empty().toTracking()));
                allowing(columnSourceManager).getColumnSources();
                will(returnValue(getIncludedColumnsMap(0, 1, 2, 3, 4)));
            }
        });
        final Table result =
                SUT.selectDistinct(PARTITIONING_COLUMN_DEFINITION.getName(), INTEGER_COLUMN_DEFINITION.getName());
        assertRowSetEquals(expectedRowSet, result.getRowSet());
    }

    @Test
    public void testWhereDate() {
        doInitializeCheck(locationKeysSlice(0, 2, 5), makePassingLocations(0, 2, 5), false, false);
        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(true, ColumnToCodecMappings.EMPTY,
                        TABLE_DEFINITION.getColumns());
                will(returnValue(columnSourceManager));
            }
        });
        assertRowSetEquals(expectedRowSet, SUT.where(PARTITIONING_COLUMN_DEFINITION.getName() + "=`D0`").getRowSet());
        assertIsSatisfied();
    }

    private static class DummyContext implements ColumnSource.GetContext, ColumnSource.FillContext {

        private final WritableChunk<Values> sourceChunk;

        private DummyContext(@NotNull final Class<?> dataType, final int chunkCapacity) {
            sourceChunk = ChunkType.fromElementType(dataType).makeWritableChunk(chunkCapacity);
        }

        @Override
        public void close() {
            sourceChunk.close();
        }
    }

    @Test
    public void testWhereSize() {
        doInitializeCheck(locationKeysSlice(1, 3), makePassingLocations(1, 3), false, true);
        checking(new Expectations() {
            {
                allowing(columnSources[3]).getInt(with(any(long.class)));
                will(returnValue(1));
                allowing(columnSources[3]).makeGetContext(with(any(Integer.class)));
                will(new CustomAction("Make dummy context") {
                    @Override
                    public Object invoke(@NotNull final Invocation invocation) {
                        return new DummyContext(int.class, (int) invocation.getParameter(0));
                    }
                });
                allowing(columnSources[3]).getChunk(with(any(DummyContext.class)), with(any(RowSequence.class)));
                will(new CustomAction("Fill dummy chunk") {
                    @Override
                    public Object invoke(@NotNull final Invocation invocation) {
                        final WritableIntChunk<Values> destination =
                                ((DummyContext) invocation.getParameter(0)).sourceChunk.asWritableIntChunk();
                        final int length = ((RowSequence) invocation.getParameter(1)).intSize();
                        destination.fillWithValue(0, length, 1);
                        destination.setSize(length);
                        return destination;
                    }
                });
            }
        });
        assertRowSetEquals(expectedRowSet, SUT.where(INTEGER_COLUMN_DEFINITION.getName() + ">0")
                .where(ArrayTypeUtils.EMPTY_STRING_ARRAY).getRowSet());
        assertIsSatisfied();
    }

    @Test
    public void testWhereDateSize() {
        doInitializeCheck(tableLocationKeys, makePassingLocations(0, 2, 5), false, false);
        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(true, ColumnToCodecMappings.EMPTY,
                        TABLE_DEFINITION.getColumns());
                will(returnValue(columnSourceManager));
                allowing(columnSources[3]).getInt(with(any(long.class)));
                will(returnValue(1));
                allowing(columnSources[3]).makeGetContext(with(any(Integer.class)));
                will(new CustomAction("Make dummy context") {
                    @Override
                    public Object invoke(@NotNull final Invocation invocation) {
                        return new DummyContext(int.class, (int) invocation.getParameter(0));
                    }
                });
                allowing(columnSources[3]).getChunk(with(any(DummyContext.class)), with(any(RowSequence.class)));
                will(new CustomAction("Fill dummy chunk") {
                    @Override
                    public Object invoke(@NotNull final Invocation invocation) {
                        final WritableIntChunk<Values> destination =
                                ((DummyContext) invocation.getParameter(0)).sourceChunk.asWritableIntChunk();
                        final int length = ((RowSequence) invocation.getParameter(1)).intSize();
                        destination.fillWithValue(0, length, 1);
                        destination.setSize(length);
                        return destination;
                    }
                });
            }
        });
        assertRowSetEquals(expectedRowSet, SUT
                .where(PARTITIONING_COLUMN_DEFINITION.getName() + "=`D0`", INTEGER_COLUMN_DEFINITION.getName() + ">0")
                .getRowSet());
        assertIsSatisfied();
    }
}
