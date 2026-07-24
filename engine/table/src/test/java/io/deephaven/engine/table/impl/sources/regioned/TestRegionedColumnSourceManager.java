//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ColumnToCodecMappings;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.impl.SimpleTableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.qst.column.Column;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;
import org.junit.Before;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.table.impl.locations.TableLocationState.NULL_SIZE;
import static io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource.REGION_CAPACITY_IN_ELEMENTS;
import static io.deephaven.engine.testutil.TstUtils.assertRowSetEquals;

/**
 * Tests for {@link RegionedColumnSourceManager}.
 */
@SuppressWarnings({"JUnit4AnnotatedMethodInJUnit3TestCase", "AutoBoxing", "unchecked",
        "AnonymousInnerClassMayBeStatic"})
public class TestRegionedColumnSourceManager extends RefreshingTableTestCase {

    private static final int NUM_COLUMNS = 3;
    private static final int NUM_LOCATIONS = 4;

    private static final int PARTITIONING_INDEX = 0;
    private static final int GROUPING_INDEX = 1;
    private static final int NORMAL_INDEX = 2;

    @SuppressWarnings("FieldCanBeLocal")
    private final static boolean PRINT_STACK_TRACES = false;

    private static final String ROW_SET_COLUMN_NAME = "RowSet";

    private RegionedTableComponentFactory componentFactory;

    private List<ColumnDefinition<?>> columnDefinitions;
    private TableDefinition tableDefinition;
    private ColumnDefinition<?> partitioningColumnDefinition;
    private ColumnDefinition<?> groupingColumnDefinition;
    private ColumnDefinition<?> normalColumnDefinition;

    private RegionedColumnSource<String>[] columnSources;
    private RegionedColumnSource<String> partitioningColumnSource;
    private RegionedColumnSource<String> groupingColumnSource;
    private RegionedColumnSource<String> normalColumnSource;

    private ColumnLocation[][] columnLocations;

    private TableLocation[] tableLocations;
    private TableLocation tableLocation0A;
    private TableLocation tableLocation0B;
    private TableLocation tableLocation1A;
    private TableLocation tableLocation1B;

    private TableLocation duplicateTableLocation0A;

    private RowSet capturedRowSet;
    private DataIndex capturedPartitioningColumnIndex;
    private DataIndex capturedGroupingColumnIndex;

    private TableLocationUpdateSubscriptionBuffer[] subscriptionBuffers;
    private long[] lastSizes;
    private List<String[]>[] dataIndexColumnsByLocation;
    private int regionCount;
    private Int2IntMap locationIndexToRegionIndex;
    private WritableRowSet expectedRowSet;
    private RowSet expectedAddedRowSet;
    private Map<String, WritableRowSet> expectedPartitioningColumnIndex;
    private Map<String, WritableRowSet> expectedGroupingColumnIndex;

    private ControlledUpdateGraph updateGraph;

    private RegionedColumnSourceManager SUT;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        componentFactory = mock(RegionedTableComponentFactory.class);

        partitioningColumnDefinition = ColumnDefinition.ofString("RCS_0").withPartitioning();
        groupingColumnDefinition = ColumnDefinition.ofString("RCS_1");
        normalColumnDefinition = ColumnDefinition.ofString("RCS_2");

        columnDefinitions = List.of(partitioningColumnDefinition, groupingColumnDefinition, normalColumnDefinition);
        tableDefinition = TableDefinition.of(columnDefinitions);

        columnSources = columnDefinitions.stream()
                .map(cd -> mock(RegionedColumnSource.class, cd.getName()))
                .toArray(RegionedColumnSource[]::new);
        partitioningColumnSource = columnSources[PARTITIONING_INDEX];
        groupingColumnSource = columnSources[GROUPING_INDEX];
        normalColumnSource = columnSources[NORMAL_INDEX];

        checking(new Expectations() {
            {
                oneOf(componentFactory).createRegionedColumnSource(with(any(RegionedColumnSourceManager.class)),
                        with(same(partitioningColumnDefinition)), with(ColumnToCodecMappings.EMPTY));
                will(returnValue(partitioningColumnSource));
                allowing(partitioningColumnSource).getType();
                will(returnValue(partitioningColumnDefinition.getDataType()));
                allowing(partitioningColumnSource).getComponentType();
                will(returnValue(partitioningColumnDefinition.getComponentType()));
                oneOf(componentFactory).createRegionedColumnSource(with(any(RegionedColumnSourceManager.class)),
                        with(same(groupingColumnDefinition)), with(ColumnToCodecMappings.EMPTY));
                will(returnValue(groupingColumnSource));
                oneOf(componentFactory).createRegionedColumnSource(with(any(RegionedColumnSourceManager.class)),
                        with(same(normalColumnDefinition)), with(ColumnToCodecMappings.EMPTY));
                will(returnValue(normalColumnSource));
            }
        });

        columnLocations = new ColumnLocation[NUM_LOCATIONS][NUM_COLUMNS];
        IntStream.range(0, NUM_LOCATIONS).forEach(li -> IntStream.range(0, NUM_COLUMNS).forEach(ci -> {
            final ColumnLocation cl = columnLocations[li][ci] = mock(ColumnLocation.class, "CL_" + li + '_' + ci);
            checking(new Expectations() {
                {
                    allowing((cl)).getName();
                    will(returnValue(columnDefinitions.get(ci).getName()));
                }
            });
        }));

        // Initialize per-location data index columns to a single-entry list naming the grouping column,
        // matching the original mock behavior. Individual tests can replace entries to exercise alternate
        // returns (e.g., duplicates) before invoking initialize().
        // noinspection unchecked
        dataIndexColumnsByLocation = (List<String[]>[]) new List<?>[NUM_LOCATIONS];
        Arrays.fill(dataIndexColumnsByLocation,
                Collections.singletonList(new String[] {groupingColumnDefinition.getName()}));
        tableLocations = IntStream.range(0, NUM_LOCATIONS).mapToObj(li -> setUpTableLocation(li, ""))
                .toArray(TableLocation[]::new);
        tableLocation0A = tableLocations[0];
        tableLocation1A = tableLocations[1];
        tableLocation0B = tableLocations[2];
        tableLocation1B = tableLocations[3];
        checking(new Expectations() {
            {
                for (final TableLocation tl : tableLocations) {
                    allowing(tl).tryRetainReference();
                    will(returnValue(true));
                    allowing(tl).getWeakReference();
                    will(returnValue(new WeakReference<>(tl)));
                    allowing(tl).dropReference();
                }
            }
        });

        duplicateTableLocation0A = setUpTableLocation(0, "-duplicate");

        subscriptionBuffers = new TableLocationUpdateSubscriptionBuffer[NUM_LOCATIONS];
        lastSizes = new long[NUM_LOCATIONS];
        Arrays.fill(lastSizes, -1); // Not null size
        regionCount = 0;
        final Int2IntOpenHashMap tmpMap = new Int2IntOpenHashMap(4, 0.5f);
        tmpMap.defaultReturnValue(-1);
        locationIndexToRegionIndex = tmpMap;
        expectedRowSet = RowSetFactory.empty().toTracking();
        expectedAddedRowSet = RowSetFactory.empty();
        expectedPartitioningColumnIndex = new LinkedHashMap<>();
        expectedGroupingColumnIndex = new LinkedHashMap<>();

        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
    }

    private ImmutableTableLocationKey makeTableKey(@NotNull final String internalPartitionValue,
            @NotNull final String columnPartitionValue) {
        final Map<String, Comparable<?>> partitions = new LinkedHashMap<>();
        partitions.put(partitioningColumnDefinition.getName(), columnPartitionValue);
        partitions.put("__IP__", internalPartitionValue);
        return new SimpleTableLocationKey(partitions);
    }

    private TableLocation setUpTableLocation(final int li, @NotNull final String mockSuffix) {
        final String ip = Integer.toString(li % 2);
        final String cp = Character.toString((li / 2) == 0 ? 'A' : 'B');
        final TableLocation tl = mock(TableLocation.class, "TL_" + ip + '_' + cp + mockSuffix);
        final ImmutableTableLocationKey tlk = makeTableKey(ip, cp);
        checking(new Expectations() {
            {
                allowing(tl).getKey();
                will(returnValue(tlk));
                allowing(tl).toStringDetailed();
                will(returnValue("mocked TL_" + ip + '_' + cp + mockSuffix));
                allowing(tl).getSize();
                will(new CustomAction("Return last size") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        return lastSizes[li];
                    }
                });
                allowing(tl).getRowSet();
                will(new CustomAction("Return last size") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        return RowSetFactory.flat(lastSizes[li]);
                    }
                });
                allowing(tl).getDataIndexColumns();
                will(new CustomAction("Return current data index columns for this location") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        return dataIndexColumnsByLocation[li];
                    }
                });
                allowing(tl).hasDataIndex(groupingColumnDefinition.getName());
                will(returnValue(true));
                // Validating a registered multi-column MergedDataIndex (via DataIndexer.hasDataIndex during
                // de-duplication) queries each location for the full key column set.
                allowing(tl).hasDataIndex(groupingColumnDefinition.getName(), normalColumnDefinition.getName());
                will(returnValue(true));
            }
        });
        IntStream.range(0, NUM_COLUMNS).forEach(ci -> {
            final ColumnLocation cl = columnLocations[li][ci];
            checking(new Expectations() {
                {
                    allowing((tl)).getColumnLocation(with(columnDefinitions.get(ci).getName()));
                    will(returnValue(cl));
                    allowing(cl).getTableLocation();
                    will(returnValue(tl));
                }
            });
        });
        return tl;
    }

    private Map<String, ColumnSource<?>> makeColumnSourceMap() {
        final Map<String, ColumnSource<?>> result = new LinkedHashMap<>();
        IntStream.range(0, columnDefinitions.size())
                .forEachOrdered(ci -> result.put(columnDefinitions.get(ci).getName(), columnSources[ci]));
        return result;
    }

    private void captureIndexes(@NotNull final RowSet rowSet) {
        capturedRowSet = rowSet;
        if (rowSet.isTracking()) {
            final DataIndexer dataIndexer = DataIndexer.existingOf(rowSet.trackingCast());
            capturedPartitioningColumnIndex =
                    dataIndexer == null ? null : dataIndexer.getDataIndex(partitioningColumnSource);
            capturedGroupingColumnIndex =
                    dataIndexer == null ? null : dataIndexer.getDataIndex(groupingColumnSource);
        }
    }

    private void expectPoison() {
        expectPoison(3);
    }

    private void expectPoison(final int regionIndex) {
        checking(new Expectations() {
            {
                exactly(1).of(partitioningColumnSource).invalidateRegion(regionIndex);
                exactly(1).of(groupingColumnSource).invalidateRegion(regionIndex);
                exactly(1).of(normalColumnSource).invalidateRegion(regionIndex);
            }
        });
    }

    private void setSizeExpectations(final boolean refreshing, final boolean success, final long... sizes) {
        final WritableRowSet newExpectedRowSet = RowSetFactory.empty().toTracking();
        final Map<String, WritableRowSet> newExpectedPartitioningColumnIndex = new LinkedHashMap<>();
        IntStream.range(0, sizes.length).forEachOrdered(li -> {
            final long size = sizes[li];
            final long lastSize = lastSizes[li];
            final boolean newLocation = lastSize == -1;
            final String cp = Character.toString((li / 2) == 0 ? 'A' : 'B');
            final TableLocation tl = tableLocations[li];

            lastSizes[li] = size;

            if (refreshing) {
                if (li % 2 == 0) {
                    // Even locations don't support subscriptions
                    if (newLocation) {
                        checking(new Expectations() {
                            {
                                oneOf(tl).supportsSubscriptions();
                                will(returnValue(false));
                                oneOf(tl).refresh();
                            }
                        });
                    }
                } else {
                    // Odd locations do
                    if (subscriptionBuffers[li] == null) {
                        assertTrue(newLocation);
                        checking(new Expectations() {
                            {
                                oneOf(tl).supportsSubscriptions();
                                will(returnValue(true));
                                oneOf(tl).subscribe(with(any(TableLocationUpdateSubscriptionBuffer.class)));
                                will(new CustomAction("Capture subscription buffer") {
                                    @Override
                                    public Object invoke(Invocation invocation) {
                                        subscriptionBuffers[li] =
                                                (TableLocationUpdateSubscriptionBuffer) invocation.getParameter(0);
                                        subscriptionBuffers[li].handleUpdate();
                                        return null;
                                    }
                                });
                            }
                        });
                    } else if (lastSize != size) {
                        subscriptionBuffers[li].handleUpdate();
                    }
                }
            } else {
                if (newLocation) {
                    checking(new Expectations() {
                        {
                            oneOf(tl).refresh();
                        }
                    });
                }
            }

            if (size > 0) {
                final int initialRegionIndex = locationIndexToRegionIndex.get(li);
                final int regionIndex;
                if (initialRegionIndex >= 0) {
                    regionIndex = initialRegionIndex;
                } else {
                    regionIndex = regionCount++;
                    locationIndexToRegionIndex.put(li, regionIndex);
                    IntStream.range(0, NUM_COLUMNS).forEach(ci -> checking(new Expectations() {
                        {
                            oneOf(columnSources[ci]).addRegion(with(columnDefinitions.get(ci)),
                                    with(columnLocations[li][ci]));
                            will(returnValue(regionIndex));
                        }
                    }));
                }
                newExpectedRowSet.insertRange(
                        RegionedColumnSource.getFirstRowKey(regionIndex),
                        RegionedColumnSource.getFirstRowKey(regionIndex) + size - 1);
                if (success) {
                    // noinspection resource
                    newExpectedPartitioningColumnIndex.computeIfAbsent(cp, cpk -> RowSetFactory.empty())
                            .insertRange(
                                    RegionedColumnSource.getFirstRowKey(regionIndex),
                                    RegionedColumnSource.getFirstRowKey(regionIndex) + size - 1);
                }
            }
        });
        if (success) {
            expectedAddedRowSet = newExpectedRowSet.minus(expectedRowSet);
            expectedRowSet.clear();
            expectedRowSet.insert(newExpectedRowSet);
            expectedPartitioningColumnIndex = newExpectedPartitioningColumnIndex;
        } else {
            expectedAddedRowSet = null;
        }
    }

    private void checkIndexes() {
        assertIsSatisfied();
        if (capturedRowSet == null) {
            assertNull(expectedAddedRowSet);
        } else {
            assertRowSetEquals(expectedAddedRowSet, capturedRowSet);
        }
        checkIndex(expectedPartitioningColumnIndex, capturedPartitioningColumnIndex);
        checkIndex(expectedGroupingColumnIndex, capturedGroupingColumnIndex);
        capturedRowSet = null;
    }

    private static void checkIndex(
            @NotNull final Map<String, ? extends RowSet> expected,
            @Nullable final DataIndex index) {
        if (index == null) {
            assertTrue(expected.isEmpty());
            return;
        }
        final Table indexTable = index.table();
        final DataIndex.RowKeyLookup rowKeyLookup = index.rowKeyLookup();
        final ColumnSource<RowSet> rowSets = indexTable.getColumnSource(index.rowSetColumnName(), RowSet.class);
        assertEquals(expected.size(), indexTable.size());
        expected.forEach((final String expectedKey, final RowSet expectedRows) -> {
            final long indexRowKey = rowKeyLookup.apply(expectedKey, false);
            final RowSet indexRows = rowSets.get(indexRowKey);
            assertNotNull(indexRows);
            assertRowSetEquals(expectedRows, indexRows);
        });
    }

    @Test
    public void testStaticBasics() {
        testStaticBasics(DataIndexOptions.DEFAULT);
    }

    @Test
    public void testStaticBasicsPartial() {
        testStaticBasics(DataIndexOptions.USING_PARTIAL_TABLE);
    }

    private void testStaticBasics(final DataIndexOptions options) {
        SUT = new RegionedColumnSourceManager(false, false, componentFactory, ColumnToCodecMappings.EMPTY,
                tableDefinition);
        assertEquals(makeColumnSourceMap(), SUT.getColumnSources());

        assertTrue(SUT.isEmpty());
        assertTrue(SUT.allLocations().isEmpty());
        assertTrue(SUT.includedLocations().isEmpty());

        // Add a few locations
        Arrays.stream(tableLocations).limit(2).forEach(SUT::addLocation);
        assertEquals(Arrays.stream(tableLocations).limit(2).collect(Collectors.toList()), SUT.allLocations());
        assertTrue(SUT.includedLocations().isEmpty());

        // Try adding an identical duplicate
        try {
            SUT.addLocation(tableLocation0A);
            fail("Expected exception");
        } catch (TableDataException expected) {
            maybePrintStackTrace(expected);
        }
        assertEquals(Arrays.stream(tableLocations).limit(2).collect(Collectors.toList()), SUT.allLocations());
        assertTrue(SUT.includedLocations().isEmpty());

        // Try adding an matching-but-not-identical duplicate
        try {
            SUT.addLocation(duplicateTableLocation0A);
            fail("Expected exception");
        } catch (TableDataException expected) {
            maybePrintStackTrace(expected);
        }
        assertEquals(Arrays.stream(tableLocations).limit(2).collect(Collectors.toList()), SUT.allLocations());
        assertTrue(SUT.includedLocations().isEmpty());

        // Add the rest
        Arrays.stream(tableLocations).skip(2).forEach(SUT::addLocation);
        assertEquals(Arrays.stream(tableLocations).collect(Collectors.toList()), SUT.allLocations());
        assertTrue(SUT.includedLocations().isEmpty());

        // Test run
        setSizeExpectations(false, true, NULL_SIZE, 100, 0, REGION_CAPACITY_IN_ELEMENTS);

        try (final RowSet first = RowSetFactory.fromRange(0, 49);
                final RowSet second = RowSetFactory.fromRange(50, 99);
                final RowSet third = RowSetFactory.fromRange(50, REGION_CAPACITY_IN_ELEMENTS)) {
            checking(new Expectations() {
                {
                    oneOf(tableLocation1A).getDataIndex(groupingColumnDefinition.getName());
                    will(returnValue(new DataIndexImpl(TableFactory.newTable(
                            Column.of(groupingColumnDefinition.getName(), "ABC", "DEF"),
                            Column.of(ROW_SET_COLUMN_NAME, RowSet.class, first.copy(), second.copy())))));
                    oneOf(tableLocation1B).getDataIndex(groupingColumnDefinition.getName());
                    will(returnValue(new DataIndexImpl(TableFactory.newTable(
                            Column.of(groupingColumnDefinition.getName(), "DEF", "XYZ"),
                            Column.of(ROW_SET_COLUMN_NAME, RowSet.class, first.copy(), third.copy())))));
                }
            });

            expectedGroupingColumnIndex.put("ABC", first.copy());
            expectedGroupingColumnIndex.put("DEF", second.copy());
            expectedGroupingColumnIndex.get("DEF").insertWithShift(RegionedColumnSource.getFirstRowKey(1), first);
            expectedGroupingColumnIndex.put("XYZ", third.shift(RegionedColumnSource.getFirstRowKey(1)));
        }

        captureIndexes(SUT.initialize());

        // Force us to build the merged index *before* we check satisfaction
        // the checkIndexes method will call table() a second time with the DEFAULT options; which exercises lazy
        // conversion
        capturedGroupingColumnIndex.table(options);

        checkIndexes();
        assertEquals(Arrays.asList(tableLocation1A, tableLocation1B), SUT.includedLocations());
    }

    private final class DataIndexImpl extends ReferenceCountedLivenessNode implements BasicDataIndex {

        private final Table table;

        private DataIndexImpl(@NotNull final Table table) {
            super(false);
            this.table = table;
        }

        @Override
        public @NotNull List<String> keyColumnNames() {
            return List.of(groupingColumnDefinition.getName());
        }

        @Override
        public @NotNull Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
            return Map.of(groupingColumnSource, groupingColumnDefinition.getName());
        }

        @Override
        public @NotNull String rowSetColumnName() {
            return ROW_SET_COLUMN_NAME;
        }

        @Override
        public boolean tableIsCached() {
            return true;
        }

        @Override
        public @NotNull Table table(DataIndexOptions ignored) {
            return table;
        }

        @Override
        public boolean isRefreshing() {
            return false;
        }
    }

    /**
     * Verify that {@link RegionedColumnSourceManager#initialize()} de-duplicates the data index columns returned by the
     * first included {@link TableLocation}. A misbehaving location that lists the same key column set twice (for
     * example, a Core+ Deephaven format location whose schema declares a column as both a grouping column and a
     * single-column data index) must not cause {@code DataIndexer.addDataIndex} to throw on a redundant registration.
     */
    @Test
    public void testStaticDeduplicatesDuplicateDataIndexColumns() {
        SUT = new RegionedColumnSourceManager(false, false, componentFactory, ColumnToCodecMappings.EMPTY,
                tableDefinition);

        Arrays.stream(tableLocations).forEach(SUT::addLocation);

        // Only tableLocation1A is included (the others are NULL_SIZE), so it is unambiguously the only
        // location consulted for data index columns. Have it report the same single-column data index twice,
        // plus a multi-column index in two different orderings that share the same key column set.
        dataIndexColumnsByLocation[1] = Arrays.asList(
                new String[] {groupingColumnDefinition.getName()},
                new String[] {groupingColumnDefinition.getName()},
                new String[] {groupingColumnDefinition.getName(), normalColumnDefinition.getName()},
                new String[] {normalColumnDefinition.getName(), groupingColumnDefinition.getName()});

        setSizeExpectations(false, true, NULL_SIZE, 100, NULL_SIZE, NULL_SIZE);

        // Initialize must not throw despite duplicate / set-equivalent entries.
        captureIndexes(SUT.initialize());

        // Exactly one MergedDataIndex should have been registered for the grouping column, despite the
        // double entry in the returned list.
        assertNotNull(capturedGroupingColumnIndex);
        assertEquals(Collections.singletonList(tableLocation1A), SUT.includedLocations());
    }

    @Test
    public void testStaticOverflow() {
        SUT = new RegionedColumnSourceManager(false, false, componentFactory, ColumnToCodecMappings.EMPTY,
                tableDefinition);

        // Add a location
        SUT.addLocation(tableLocation0A);
        assertEquals(Collections.singletonList(tableLocation0A), SUT.allLocations());
        assertTrue(SUT.includedLocations().isEmpty());

        // Test run with an overflow
        lastSizes[0] = REGION_CAPACITY_IN_ELEMENTS + 1;
        checking(new Expectations() {
            {
                oneOf(tableLocation0A).refresh();
            }
        });
        try {
            // noinspection resource
            SUT.initialize();
            fail("Expected exception");
        } catch (TableDataException expected) {
            maybePrintStackTrace(expected);
        }
    }

    @Test
    public void testRefreshing() {
        SUT = new RegionedColumnSourceManager(true, false, componentFactory, ColumnToCodecMappings.EMPTY,
                tableDefinition);
        assertEquals(makeColumnSourceMap(), SUT.getColumnSources());

        assertTrue(SUT.isEmpty());
        assertTrue(SUT.allLocations().isEmpty());
        assertTrue(SUT.includedLocations().isEmpty());

        // Check run with no locations
        captureIndexes(SUT.initialize());
        checkIndexes();

        // Add a few locations
        Arrays.stream(tableLocations).limit(2).forEach(SUT::addLocation);
        assertEquals(Arrays.stream(tableLocations).limit(2).collect(Collectors.toList()), SUT.allLocations());
        assertTrue(SUT.includedLocations().isEmpty());

        // Refresh them
        setSizeExpectations(true, true, 5, 1000);
        updateGraph.runWithinUnitTestCycle(() -> captureIndexes(SUT.refresh().added()));
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.includedLocations());

        // Refresh them with no change
        setSizeExpectations(true, true, 5, 1000);
        updateGraph.runWithinUnitTestCycle(() -> captureIndexes(SUT.refresh().added()));
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.includedLocations());

        // Refresh them with a change for the subscription-supporting one
        setSizeExpectations(true, true, 5, 1001);
        updateGraph.runWithinUnitTestCycle(() -> captureIndexes(SUT.refresh().added()));
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.includedLocations());

        // Try adding a duplicate
        try {
            SUT.addLocation(tableLocation0A);
            fail("Expected exception");
        } catch (TableDataException expected) {
            maybePrintStackTrace(expected);
        }
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.allLocations());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.includedLocations());

        // Try adding an matching-but-not-identical duplicate
        try {
            SUT.addLocation(duplicateTableLocation0A);
            fail("Expected exception");
        } catch (TableDataException expected) {
            maybePrintStackTrace(expected);
        }
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.allLocations());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.includedLocations());

        // Add the rest
        Arrays.stream(tableLocations).skip(2).forEach(SUT::addLocation);
        assertEquals(Arrays.stream(tableLocations).collect(Collectors.toList()), SUT.allLocations());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.includedLocations());

        // Test run with new locations included
        setSizeExpectations(true, true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, NULL_SIZE);
        updateGraph.runWithinUnitTestCycle(() -> captureIndexes(SUT.refresh().added()));
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B), SUT.includedLocations());

        // Test no-op run
        setSizeExpectations(true, true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, NULL_SIZE);
        updateGraph.runWithinUnitTestCycle(() -> captureIndexes(SUT.refresh().added()));
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B), SUT.includedLocations());

        // Test run with a location updated from null to not
        setSizeExpectations(true, true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, 2);
        updateGraph.runWithinUnitTestCycle(() -> captureIndexes(SUT.refresh().added()));
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with a location updated
        setSizeExpectations(true, true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, 10000002);
        updateGraph.runWithinUnitTestCycle(() -> captureIndexes(SUT.refresh().added()));
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with a size decrease
        setSizeExpectations(true, false, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, 2);
        expectPoison();
        updateGraph.runWithinUnitTestCycle(() -> {
            try {
                SUT.refresh();
                fail("Expected exception");
            } catch (AssertionFailure expected) {
                maybePrintStackTrace(expected);
            }
        });
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with a location truncated
        setSizeExpectations(true, false, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, NULL_SIZE);
        expectPoison();
        updateGraph.runWithinUnitTestCycle(() -> {
            try {
                SUT.refresh();
                fail("Expected exception");
            } catch (TableDataException expected) {
                maybePrintStackTrace(expected);
            }
        });
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with an overflow
        setSizeExpectations(true, false, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, REGION_CAPACITY_IN_ELEMENTS + 1);
        updateGraph.runWithinUnitTestCycle(() -> {
            try {
                SUT.refresh();
                fail("Expected exception");
            } catch (TableDataException expected) {
                maybePrintStackTrace(expected);
            }
        });
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with an exception
        subscriptionBuffers[3].handleException(new TableDataException("TEST"));
        expectPoison();
        updateGraph.runWithinUnitTestCycle(() -> {
            try {
                SUT.refresh();
                fail("Expected exception");
            } catch (TableDataException expected) {
                assertEquals("TEST", expected.getCause().getMessage());
            }
        });
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // expect table locations to be cleaned up via LivenessScope release as the test exits
        IntStream.range(0, tableLocations.length).forEachOrdered(li -> {
            final TableLocation tl = tableLocations[li];
            checking(new Expectations() {
                {
                    oneOf(tl).supportsSubscriptions();
                    if (li % 2 == 0) {
                        // Even locations don't support subscriptions
                        will(returnValue(false));
                    } else {
                        will(returnValue(true));
                        oneOf(tl).unsubscribe(with(subscriptionBuffers[li]));
                    }
                }
            });
        });
    }

    /**
     * Regression test for a bug where the pushdown-filter helpers resolved a region's {@link TableLocation} by indexing
     * {@code orderedIncludedTableLocations} with the region's stable, monotonically-increasing region index rather than
     * a list position. Since that list is compacted whenever a location is removed, but region indices are never
     * reused, an included location's region index can outgrow the list's size (yielding an
     * {@link IndexOutOfBoundsException}) or land on the wrong entry (yielding a silently incorrect result).
     */
    @Test
    public void testPushdownAfterLocationRemoval() {
        SUT = new RegionedColumnSourceManager(true, true, componentFactory, ColumnToCodecMappings.EMPTY,
                tableDefinition);

        // Check run with no locations. This establishes capturedPartitioningColumnIndex/capturedGroupingColumnIndex
        // against the manager's persistent, tracking master RowSet, which checkIndexes() relies on for every
        // subsequent refresh() (whose TableUpdate#added() is a plain, non-tracking per-cycle delta).
        captureIndexes(SUT.initialize());
        checkIndexes();

        // Add and include all 4 locations: region indices 0, 1, 2, 3 in insertion order.
        Arrays.stream(tableLocations).forEach(SUT::addLocation);
        setSizeExpectations(true, true, 5, 1000, 5003, 2);
        updateGraph.runWithinUnitTestCycle(() -> captureIndexes(SUT.refresh().added()));
        checkIndexes();
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Remove tableLocation0A (region index 0). orderedIncludedTableLocations compacts to size 3 (holding, in
        // order, the entries for region indices 1, 2, and 3), but region index 0 is never reused. (Not using
        // setSizeExpectations/checkIndexes here, since that harness has no notion of removal: re-supplying the
        // unchanged sizes would incorrectly re-include region 0's rows in the expected row set/data index.)
        expectPoison(0);
        updateGraph.runWithinUnitTestCycle(() -> {
            SUT.removeLocationKey(tableLocation0A.getKey());
            SUT.refresh();
        });
        assertIsSatisfied();
        assertEquals(Arrays.asList(tableLocation1A, tableLocation0B, tableLocation1B), SUT.includedLocations());

        final WhereFilter filter = WhereFilterFactory.getExpression("RCS_2 = `x`");
        final PushdownFilterContext context = PushdownFilterContext.NO_PUSHDOWN_CONTEXT;
        final JobScheduler jobScheduler = new ImmediateJobScheduler();

        // Region 3 (tableLocation1B, size 2) has the highest region index. Before the fix, resolving it via
        // orderedIncludedTableLocations.get(3) throws IndexOutOfBoundsException, because that list was compacted to
        // size 3 by the removal above.
        try (final RowSet region3Selection = RowSetFactory.fromRange(
                RegionedColumnSource.getFirstRowKey(3), RegionedColumnSource.getFirstRowKey(3) + 1)) {
            checking(new Expectations() {
                {
                    oneOf(tableLocation1B).estimatePushdownFilterCost(
                            with(same(filter)), with(any(RowSet.class)), with(false), with(same(context)),
                            with(same(jobScheduler)), with(any(LongConsumer.class)), with(any(Consumer.class)));
                    will(new CustomAction("complete cost") {
                        @Override
                        public Object invoke(final Invocation invocation) {
                            ((LongConsumer) invocation.getParameter(5))
                                    .accept(PushdownResult.REGION_METADATA_STATS_COST);
                            return null;
                        }
                    });
                }
            });

            final AtomicLong cost = new AtomicLong(-1);
            final AtomicReference<Exception> error = new AtomicReference<>();
            SUT.estimatePushdownFilterCost(filter, region3Selection, false, context, jobScheduler, cost::set,
                    error::set);

            assertNull("estimatePushdownFilterCost for the highest-indexed region must not throw", error.get());
            assertEquals(PushdownResult.REGION_METADATA_STATS_COST, cost.get());
            assertIsSatisfied();
        }

        try (final RowSet region3Selection = RowSetFactory.fromRange(
                RegionedColumnSource.getFirstRowKey(3), RegionedColumnSource.getFirstRowKey(3) + 1)) {
            checking(new Expectations() {
                {
                    oneOf(tableLocation1B).pushdownFilter(
                            with(same(filter)), with(any(RowSet.class)), with(false), with(same(context)),
                            with(any(Long.class)), with(same(jobScheduler)), with(any(Consumer.class)),
                            with(any(Consumer.class)));
                    will(new CustomAction("complete pushdown") {
                        @Override
                        public Object invoke(final Invocation invocation) {
                            final RowSet shiftedSelection = (RowSet) invocation.getParameter(1);
                            // noinspection unchecked
                            ((Consumer<PushdownResult>) invocation.getParameter(6))
                                    .accept(PushdownResult.allMaybeMatch(shiftedSelection));
                            return null;
                        }
                    });
                }
            });

            final AtomicReference<PushdownResult> result = new AtomicReference<>();
            final AtomicReference<Exception> error = new AtomicReference<>();
            SUT.pushdownFilter(filter, region3Selection, false, context, Long.MAX_VALUE, jobScheduler, result::set,
                    error::set);

            assertNull("pushdownFilter for the highest-indexed region must not throw", error.get());
            assertNotNull(result.get());
            assertRowSetEquals(region3Selection, result.get().maybeMatch());
            result.get().close();
            assertIsSatisfied();
        }

        // Region 1 (tableLocation1A, size 1000) sits at list position 0 in the post-removal
        // orderedIncludedTableLocations, but list position 1 -- what its region index would incorrectly select --
        // now holds tableLocation0B's entry. Before the fix, this silently queries the wrong location.
        try (final RowSet region1Selection = RowSetFactory.fromRange(
                RegionedColumnSource.getFirstRowKey(1), RegionedColumnSource.getFirstRowKey(1) + 999)) {
            checking(new Expectations() {
                {
                    oneOf(tableLocation1A).pushdownFilter(
                            with(same(filter)), with(any(RowSet.class)), with(false), with(same(context)),
                            with(any(Long.class)), with(same(jobScheduler)), with(any(Consumer.class)),
                            with(any(Consumer.class)));
                    will(new CustomAction("complete pushdown") {
                        @Override
                        public Object invoke(final Invocation invocation) {
                            final RowSet shiftedSelection = (RowSet) invocation.getParameter(1);
                            // noinspection unchecked
                            ((Consumer<PushdownResult>) invocation.getParameter(6))
                                    .accept(PushdownResult.allMaybeMatch(shiftedSelection));
                            return null;
                        }
                    });
                }
            });

            final AtomicReference<PushdownResult> result = new AtomicReference<>();
            final AtomicReference<Exception> error = new AtomicReference<>();
            SUT.pushdownFilter(filter, region1Selection, false, context, Long.MAX_VALUE, jobScheduler, result::set,
                    error::set);

            assertNull("pushdownFilter must query the location that actually owns the requested region",
                    error.get());
            assertNotNull(result.get());
            assertRowSetEquals(region1Selection, result.get().maybeMatch());
            result.get().close();
            assertIsSatisfied();
        }

        // expect the still-included table locations to be cleaned up via LivenessScope release as the test exits.
        // tableLocation0A (li=0) was removed above, so it is no longer tracked by the manager and does not go
        // through this cleanup path.
        IntStream.range(1, tableLocations.length).forEachOrdered(li -> {
            final TableLocation tl = tableLocations[li];
            checking(new Expectations() {
                {
                    oneOf(tl).supportsSubscriptions();
                    if (li % 2 == 0) {
                        // Even locations don't support subscriptions
                        will(returnValue(false));
                    } else {
                        will(returnValue(true));
                        oneOf(tl).unsubscribe(with(subscriptionBuffers[li]));
                    }
                }
            });
        });
    }

    private static void maybePrintStackTrace(@NotNull final Exception e) {
        if (PRINT_STACK_TRACES) {
            e.printStackTrace();
        }
    }
}
