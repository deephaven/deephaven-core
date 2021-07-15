/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.locations.TableLocationSubscriptionBuffer;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.DeferredGroupingColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import org.jetbrains.annotations.NotNull;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.db.v2.TstUtils.assertIndexEquals;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link PartitionAwareSourceTable}.
 */
@SuppressWarnings({"AutoBoxing", "JUnit4AnnotatedMethodInJUnit3TestCase", "AnonymousInnerClassMayBeStatic"})
public class TestPartitionAwareSourceTable extends LiveTableTestCase {

    private static final int NUM_COLUMNS = 5;
    private static final ColumnDefinition<String> PARTITIONING_COLUMN_DEFINITION = ColumnDefinition.ofString("Date").withPartitioning();
    private static final ColumnDefinition<Boolean> BOOLEAN_COLUMN_DEFINITION = ColumnDefinition.ofBoolean("Active");
    private static final ColumnDefinition<Character> CHARACTER_COLUMN_DEFINITION = ColumnDefinition.ofChar("Type").withGrouping();
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
    private static final Set<String> INCLUDED_INTERNAL_PARTITIONS = Collections.singleton("1");

    private static final long INDEX_INCREMENT = 1000;

    private SourceTableComponentFactory componentFactory;
    private ColumnSourceManager columnSourceManager;

    private DeferredGroupingColumnSource<?>[] columnSources;

    private TableLocationProvider locationProvider;
    private TableLocation[] tableLocations;

    private TableLocationSubscriptionBuffer subscriptionBuffer;

    private Table coalesced;
    private ShiftAwareListener listener;
    private final TstUtils.TstNotification notification = new TstUtils.TstNotification();

    private Index expectedIndex;

    private PartitionAwareSourceTable SUT;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        componentFactory = mock(SourceTableComponentFactory.class);
        columnSourceManager = mock(ColumnSourceManager.class);
        columnSources = TABLE_DEFINITION.getColumnStream().map(cd -> {
            final DeferredGroupingColumnSource<?> mocked = mock(DeferredGroupingColumnSource.class, cd.getName());
            checking(new Expectations() {{
                allowing(mocked).getType();
                will(returnValue(cd.getDataType()));
                allowing(mocked).getComponentType();
                will(returnValue(cd.getComponentType()));
            }});
            return mocked;
        }).toArray(DeferredGroupingColumnSource[]::new);
        locationProvider = mock(TableLocationProvider.class);
        tableLocations = new TableLocation[]{
                mock(TableLocation.class, "TL0"),
                mock(TableLocation.class, "TL1"),
                mock(TableLocation.class, "TL2"),
                mock(TableLocation.class, "TL3"),
                mock(TableLocation.class, "TL4"),
                mock(TableLocation.class, "TL5")
        };
        checking(new Expectations() {{
            allowing(locationProvider).supportsSubscriptions();
            will(returnValue(true));
            for (int li = 0; li < tableLocations.length; ++li) {
                final TableLocation tableLocation = tableLocations[li];
                allowing(tableLocation).supportsSubscriptions();
                will(returnValue(true));
                allowing(tableLocation).getInternalPartition();
                will(returnValue(INTERNAL_PARTITIONS[li]));
                allowing(tableLocation).getColumnPartition();
                will(returnValue(COLUMN_PARTITIONS[li]));
            }
        }});
        listener = mock(ShiftAwareListener.class);

        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(with(true), with(ColumnToCodecMappings.EMPTY), with(equal(TABLE_DEFINITION.getColumns())));
            will(returnValue(columnSourceManager));
            oneOf(columnSourceManager).disableGrouping();
        }});

        expectedIndex = Index.FACTORY.getEmptyIndex();

        SUT = new PartitionAwareSourceTable(TABLE_DEFINITION, "", componentFactory, locationProvider, LiveTableMonitor.DEFAULT, INCLUDED_INTERNAL_PARTITIONS);
        assertIsSatisfied();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (coalesced != null) {
                coalesced.dropReference();
                coalesced = null;
            }
        }
    }

    private static ColumnDefinition<?>[] getIncludedColumnDefs(final int... indices) {
        return IntStream.of(indices).mapToObj(ci -> TABLE_DEFINITION.getColumns()[ci]).toArray(ColumnDefinition[]::new);
    }

    private static String[] getIncludedColumnNames(final int... indices) {
        return IntStream.of(indices).mapToObj(ci -> TABLE_DEFINITION.getColumns()[ci].getName()).toArray(String[]::new);
    }

    private static String[] getExcludedColumnNames(final TableDefinition currentDef, final int... indices) {
        final Set<String> includedNames = IntStream.of(indices).mapToObj(ci -> TABLE_DEFINITION.getColumns()[ci].getName()).collect(Collectors.toSet());
        return currentDef.getColumnStream().map(ColumnDefinition::getName).filter(n -> !includedNames.contains(n)).toArray(String[]::new);
    }

    private Map<String, ? extends DeferredGroupingColumnSource<?>> getIncludedColumnsMap(final int... indices) {
        return IntStream.of(indices).mapToObj(ci -> new Pair<>(TABLE_DEFINITION.getColumns()[ci].getName(), columnSources[ci])).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, Assert::neverInvoked, LinkedHashMap::new));
    }

    private TableLocation[] locationsSlice(final int... indexes) {
        TableLocation[] slice = new TableLocation[indexes.length];
        for (int ii = 0; ii < indexes.length; ++ii) {
            slice[ii] = tableLocations[indexes[ii]];
        }
        return slice;
    }

    private Set<TableLocation> makePassingLocations(final int... indexes) {
        return Arrays.stream(indexes).mapToObj(li -> tableLocations[li]).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Test
    public void testInitialize() {
        doInitializeCheck(locationsSlice(0, 1, 2, 3, 4), makePassingLocations(1, 3), false, true);
    }

    @Test
    public void testInitializeException() {
        doInitializeCheck(locationsSlice(0, 1, 2, 3, 4), makePassingLocations(1, 3), true, true);
    }

    @Test
    public void testRefreshUnchanged() {
        doInitializeCheck(locationsSlice(0, 1, 2, 3, 4), makePassingLocations(1, 3), false, true);
        doRefreshUnchangedCheck();
    }

    @Test
    public void testRefreshChanged() {
        doInitializeCheck(locationsSlice(0, 1, 2, 3, 4), makePassingLocations(1, 3), false, true);
        doRefreshChangedCheck();
        doAddLocationsRefreshCheck(locationsSlice(5), makePassingLocations(5));
    }

    @Test
    public void testRefreshException() {
        try (final ErrorExpectation ignored = new ErrorExpectation()) {
            doInitializeCheck(locationsSlice(0, 1, 2, 3, 4), makePassingLocations(1, 3), false, true);
            doRefreshExceptionCheck();
        }
    }

    private enum ConcurrentInstantiationType {
        Idle,
        UpdatingClosed,
        UpdatingOpen
    }

    private void doInitializeCheck(final TableLocation[] tableLocations, final Set<TableLocation> expectPassFilters, final boolean throwException, final boolean coalesceAndListen) {
        doInitializeCheck(tableLocations, expectPassFilters, throwException, coalesceAndListen, ConcurrentInstantiationType.Idle);
    }

    private void doInitializeCheck(final TableLocation[] tableLocations, final Set<TableLocation> expectPassFilters, final boolean throwException, final boolean coalesceAndListen,
                                   @NotNull final ConcurrentInstantiationType ciType) {
        Assert.assertion(!(throwException && !coalesceAndListen), "!(throwException && !listen)");
        final TableDataException exception = new TableDataException("test");
        final Index toAdd = Index.FACTORY.getIndexByRange(expectedIndex.lastKey() + 1, expectedIndex.lastKey() + INDEX_INCREMENT);

        checking(new Expectations() {{
            oneOf(locationProvider).subscribe(with(any(TableLocationProvider.Listener.class)));
            will(new CustomAction("Supply locations") {
                @Override
                public Object invoke(Invocation invocation) {
                    subscriptionBuffer = (TableLocationSubscriptionBuffer) invocation.getParameter(0);
                    Arrays.stream(tableLocations).forEach(subscriptionBuffer::handleTableLocation);
                    return null;
                }
            });
            oneOf(columnSourceManager).refresh();
            if (throwException) {
                will(throwException(exception));
            } else {
                will(returnValue(toAdd));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(getIncludedColumnsMap(0, 1, 2, 3, 4)));
            }
        }});
        expectPassFilters.forEach(tl ->
                checking(new Expectations() {{
                    oneOf(columnSourceManager).addLocation(tl);
                }}));

        expectedIndex.insert(toAdd);
        if (coalesceAndListen) {
            if (ciType == ConcurrentInstantiationType.UpdatingClosed || ciType == ConcurrentInstantiationType.UpdatingOpen) {
                LiveTableMonitor.DEFAULT.startCycleForUnitTests();
            }
            try {
                coalesced = SUT.coalesce();
                coalesced.retainReference();
                ((QueryTable)coalesced).listenForUpdates(listener);
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
            assertIndexEquals(expectedIndex, SUT.getIndex());
            if (ciType == ConcurrentInstantiationType.UpdatingClosed) {
                LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
            }
        }
    }

    @Test
    public void testConcurrentInstantiationUpdating() {
        doInitializeCheck(locationsSlice(0, 1, 2, 3, 4), makePassingLocations(1, 3), false, true, ConcurrentInstantiationType.UpdatingClosed);
        doRefreshChangedCheck();
    }

    @Test
    public void testConcurrentInstantiationUpdatingWithInitialCycleRefresh() {
        doInitializeCheck(locationsSlice(0, 1, 2, 3, 4), makePassingLocations(1, 3), false, true, ConcurrentInstantiationType.UpdatingOpen);
        doRefreshChangedCheck();
    }

    private void doRefreshChangedCheck() {
        final Index toAdd = Index.FACTORY.getIndexByRange(expectedIndex.lastKey() + 1, expectedIndex.lastKey() + INDEX_INCREMENT);
        checking(new Expectations() {{
            oneOf(columnSourceManager).refresh();
            will(returnValue(toAdd));
            checking(new Expectations() {{
                oneOf(listener).getNotification(with(any(ShiftAwareListener.Update.class)));
                will(new CustomAction("check added") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        final ShiftAwareListener.Update update = (ShiftAwareListener.Update) invocation.getParameter(0);
                        assertIndexEquals(toAdd, update.added);
                        assertIndexEquals(Index.FACTORY.getEmptyIndex(), update.removed);
                        assertIndexEquals(Index.FACTORY.getEmptyIndex(), update.modified);
                        assertTrue(update.shifted.empty());
                        return notification;
                    }
                });
            }});
        }});

        notification.reset();
        if (LogicalClock.DEFAULT.currentState() == LogicalClock.State.Idle) {
            LiveTableMonitor.DEFAULT.startCycleForUnitTests();
        }
        try {
            SUT.refresh();
        } finally {
            LiveTableMonitor.DEFAULT.completeCycleForUnitTests();
        }
        assertIsSatisfied();
        notification.assertInvoked();
        expectedIndex.insert(toAdd);
        assertIndexEquals(expectedIndex, SUT.getIndex());
    }

    private void doRefreshUnchangedCheck() {
        checking(new Expectations() {{
            oneOf(columnSourceManager).refresh();
            will(returnValue(Index.FACTORY.getEmptyIndex()));
        }});

        notification.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(SUT::refresh);
        assertIsSatisfied();
        notification.assertNotInvoked();

        assertIndexEquals(expectedIndex, SUT.getIndex());
    }

    private void doRefreshExceptionCheck() {
        final TableDataException exception = new TableDataException("test");
        checking(new Expectations() {{
            oneOf(columnSourceManager).refresh();
            will(throwException(exception));
            oneOf(listener).getErrorNotification(with(any(TableDataException.class)), with(any(UpdatePerformanceTracker.Entry.class)));
            will(new CustomAction("check exception") {
                @Override
                public Object invoke(Invocation invocation) {
                    assertEquals(exception, ((Exception) invocation.getParameter(0)).getCause());
                    return notification;
                }
            });
        }});

        notification.reset();
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(SUT::refresh);
        assertIsSatisfied();
        notification.assertInvoked();

        assertIndexEquals(expectedIndex, SUT.getIndex());
    }

    private void doAddLocationsRefreshCheck(final TableLocation[] tableLocations,
                                            final Set<TableLocation> expectPassFilters) {
        Arrays.stream(tableLocations).forEach(subscriptionBuffer::handleTableLocation);

        expectPassFilters.forEach(tl ->
                checking(new Expectations() {{
                    oneOf(columnSourceManager).addLocation(tl);
                }}));

        doRefreshChangedCheck();
    }

    @Test
    public void testRedefinition() {
        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(this::doTestRedefinition);
    }

    private void doTestRedefinition() {
        // Note: We expect redefinition to make a new CSM, but no work until we force a coalesce by asking for column sources
        final ColumnDefinition[] includedColumns1 = new ColumnDefinition[]{
                PARTITIONING_COLUMN_DEFINITION,
                CHARACTER_COLUMN_DEFINITION,
                INTEGER_COLUMN_DEFINITION,
                DOUBLE_COLUMN_DEFINITION
        };

        final Map<Class, ColumnSource> dataTypeToColumnSource = new HashMap<>();
        IntStream.range(0, includedColumns1.length).forEach(ci -> {
            final ColumnDefinition columnDefinition = includedColumns1[ci];
            final ColumnSource columnSource = mock(ColumnSource.class, "_CS_" + columnDefinition.getDataType().getSimpleName());
            dataTypeToColumnSource.put(columnDefinition.getDataType(), columnSource);
            checking(new Expectations() {{
                allowing(columnSource).getType();
                will(returnValue(columnDefinition.getDataType()));
                allowing(columnSource).getComponentType();
                will(returnValue(columnDefinition.getComponentType()));
            }});
        });

        // Test 1: Drop a column
        // Setup the table
        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(with(true), with(ColumnToCodecMappings.EMPTY), with(equal(includedColumns1)));
            will(returnValue(columnSourceManager));
            oneOf(columnSourceManager).disableGrouping();
        }});
        final Table dropColumnsResult1 = SUT.dropColumns(BOOLEAN_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(dropColumnsResult1 instanceof PartitionAwareSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {{
            oneOf(locationProvider).subscribe(with(any(TableLocationProvider.Listener.class)));
            will(new CustomAction("Supply no locations") {
                @Override
                public Object invoke(Invocation invocation) {
                    return null;
                }
            });
            oneOf(columnSourceManager).refresh();
            will(returnValue(Index.FACTORY.getEmptyIndex()));
            oneOf(columnSourceManager).getColumnSources();
            will(returnValue(
                    Arrays.stream(includedColumns1).collect(Collectors.toMap(ColumnDefinition::getName, cd -> dataTypeToColumnSource.get(cd.getDataType()), Assert::neverInvoked, LinkedHashMap::new))));
        }});
        assertEquals(NUM_COLUMNS - 1, dropColumnsResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(dropColumnsResult1.getColumnSource(CHARACTER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult1.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult1.getColumnSource(DOUBLE_COLUMN_DEFINITION.getName()));

        // Test 2: Drop another column
        // Setup the table
        final ColumnDefinition[] includedColumns2 = new ColumnDefinition[]{
                PARTITIONING_COLUMN_DEFINITION,
                INTEGER_COLUMN_DEFINITION,
                DOUBLE_COLUMN_DEFINITION
        };
        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(with(true), with(ColumnToCodecMappings.EMPTY), with(equal(includedColumns2)));
            will(returnValue(columnSourceManager));
            oneOf(columnSourceManager).disableGrouping();
        }});
        final Table dropColumnsResult2 = dropColumnsResult1.dropColumns(CHARACTER_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(dropColumnsResult2 instanceof PartitionAwareSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {{
            oneOf(locationProvider).subscribe(with(any(TableLocationProvider.Listener.class)));
            will(new CustomAction("Supply no locations") {
                @Override
                public Object invoke(Invocation invocation) {
                    return null;
                }
            });
            oneOf(columnSourceManager).refresh();
            will(returnValue(Index.FACTORY.getEmptyIndex()));
            oneOf(columnSourceManager).getColumnSources();
            will(returnValue(
                    Arrays.stream(includedColumns2).collect(Collectors.toMap(ColumnDefinition::getName, cd -> dataTypeToColumnSource.get(cd.getDataType()), Assert::neverInvoked, LinkedHashMap::new))));
        }});
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
        final ColumnDefinition[] includedColumns3 = new ColumnDefinition[]{
                INTEGER_COLUMN_DEFINITION,
                PARTITIONING_COLUMN_DEFINITION,
        };
        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(with(true), with(ColumnToCodecMappings.EMPTY), with(equal(includedColumns3)));
            will(returnValue(columnSourceManager));
            oneOf(columnSourceManager).disableGrouping();
        }});
        final Table viewResult1 = dropColumnsResult2.view(INTEGER_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(viewResult1 instanceof DeferredViewTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {{
            oneOf(locationProvider).subscribe(with(any(TableLocationProvider.Listener.class)));
            will(new CustomAction("Supply no locations") {
                @Override
                public Object invoke(Invocation invocation) {
                    return null;
                }
            });
            oneOf(columnSourceManager).refresh();
            will(returnValue(Index.FACTORY.getEmptyIndex()));
            oneOf(columnSourceManager).getColumnSources();
            will(returnValue(
                    Arrays.stream(includedColumns3).collect(Collectors.toMap(ColumnDefinition::getName, cd -> dataTypeToColumnSource.get(cd.getDataType()), Assert::neverInvoked, LinkedHashMap::new))));
        }});
        assertEquals(NUM_COLUMNS - 4, viewResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(viewResult1.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));

        // Test 5: Add a new derived column on
        // Setup the table
        final Table viewResult2 = viewResult1.updateView("SizeSquared=" + INTEGER_COLUMN_DEFINITION.getName() + '*' + INTEGER_COLUMN_DEFINITION.getName());
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
        final String[] expectedDistinctDates = IntStream.of(1, 3, 5).mapToObj(li -> COLUMN_PARTITIONS[li]).distinct().toArray(String[]::new);
        doInitializeCheck(tableLocations, passedLocations, false, true);
        passedLocations.forEach(tl ->
                checking(new Expectations() {{
                    oneOf(tl).refresh();
                    oneOf(tl).getSize();
                    will(returnValue(1L));
                }}));
        checking(new Expectations() {{
            oneOf(columnSourceManager).allLocations();
            will(returnValue(passedLocations));
        }});
        final Table result = SUT.selectDistinct(PARTITIONING_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        //noinspection unchecked
        final DataColumn<String> distinctDateColumn = result.getColumn(PARTITIONING_COLUMN_DEFINITION.getName());
        assertEquals(expectedDistinctDates.length, distinctDateColumn.size());
        final String[] distinctDates = (String[]) distinctDateColumn.getDirect();
        Arrays.sort(expectedDistinctDates);
        Arrays.sort(distinctDates);
        assertArrayEquals(expectedDistinctDates, distinctDates);
    }

    @Test
    public void testWhereDate() {
        doInitializeCheck(tableLocations, makePassingLocations(5), false, false);
        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(true, ColumnToCodecMappings.EMPTY, TABLE_DEFINITION.getColumns());
            will(returnValue(columnSourceManager));
            oneOf(columnSourceManager).disableGrouping();
        }});
        assertIndexEquals(expectedIndex, SUT.where(PARTITIONING_COLUMN_DEFINITION.getName() + "=`D0`").getIndex());
        assertIsSatisfied();
    }

    private static class DummyContext implements ColumnSource.GetContext, ColumnSource.FillContext {

        private final WritableChunk<Values> sourceChunk;

        private DummyContext(@NotNull final Class<?> dataType, final int chunkCapacity) {
            sourceChunk = ChunkType.fromElementType(dataType).makeWritableChunk(chunkCapacity);
        }
    }

    @Test
    public void testWhereSize() {
        doInitializeCheck(locationsSlice(0, 1, 2, 3, 4), makePassingLocations(1, 3), false, true);
        checking(new Expectations() {{
            allowing(columnSources[3]).getInt(with(any(long.class)));
            will(returnValue(1));
            allowing(columnSources[3]).makeGetContext(with(any(Integer.class)));
            will(new CustomAction("Make dummy context") {
                @Override
                public Object invoke(@NotNull final Invocation invocation) {
                    return new DummyContext(int.class, (int) invocation.getParameter(0));
                }
            });
            allowing(columnSources[3]).getChunk(with(any(DummyContext.class)), with(any(OrderedKeys.class)));
            will(new CustomAction("Fill dummy chunk") {
                @Override
                public Object invoke(@NotNull final Invocation invocation) {
                    final WritableIntChunk<Values> destination = ((DummyContext) invocation.getParameter(0)).sourceChunk.asWritableIntChunk();
                    final int length = ((OrderedKeys) invocation.getParameter(1)).intSize();
                    destination.fillWithValue(0, length, 1);
                    destination.setSize(length);
                    return destination;
                }
            });
        }});
        assertIndexEquals(expectedIndex, SUT.where(INTEGER_COLUMN_DEFINITION.getName() + ">0").where(CollectionUtil.ZERO_LENGTH_STRING_ARRAY).getIndex());
        assertIsSatisfied();
    }

    @Test
    public void testWhereDateSize() {
        doInitializeCheck(tableLocations, makePassingLocations(5), false, false);
        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(true, ColumnToCodecMappings.EMPTY, TABLE_DEFINITION.getColumns());
            will(returnValue(columnSourceManager));
            oneOf(columnSourceManager).disableGrouping();
            allowing(columnSources[3]).getInt(with(any(long.class)));
            will(returnValue(1));
            allowing(columnSources[3]).makeGetContext(with(any(Integer.class)));
            will(new CustomAction("Make dummy context") {
                @Override
                public Object invoke(@NotNull final Invocation invocation) {
                    return new DummyContext(int.class, (int) invocation.getParameter(0));
                }
            });
            allowing(columnSources[3]).getChunk(with(any(DummyContext.class)), with(any(OrderedKeys.class)));
            will(new CustomAction("Fill dummy chunk") {
                @Override
                public Object invoke(@NotNull final Invocation invocation) {
                    final WritableIntChunk<Values> destination = ((DummyContext) invocation.getParameter(0)).sourceChunk.asWritableIntChunk();
                    final int length = ((OrderedKeys) invocation.getParameter(1)).intSize();
                    destination.fillWithValue(0, length, 1);
                    destination.setSize(length);
                    return destination;
                }
            });
        }});
        assertIndexEquals(expectedIndex, SUT.where(PARTITIONING_COLUMN_DEFINITION.getName() + "=`D0`", INTEGER_COLUMN_DEFINITION.getName() + ">0").getIndex());
        assertIsSatisfied();
    }
}
