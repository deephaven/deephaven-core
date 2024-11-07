//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ColumnToCodecMappings;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.impl.SimpleTableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
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
    private int regionCount;
    private TIntIntMap locationIndexToRegionIndex;
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

        columnSources = columnDefinitions.stream()
                .map(cd -> mock(RegionedColumnSource.class, cd.getName()))
                .toArray(RegionedColumnSource[]::new);
        partitioningColumnSource = columnSources[PARTITIONING_INDEX];
        groupingColumnSource = columnSources[GROUPING_INDEX];
        normalColumnSource = columnSources[NORMAL_INDEX];

        checking(new Expectations() {
            {
                oneOf(componentFactory).createRegionedColumnSource(with(same(partitioningColumnDefinition)),
                        with(ColumnToCodecMappings.EMPTY));
                will(returnValue(partitioningColumnSource));
                allowing(partitioningColumnSource).getType();
                will(returnValue(partitioningColumnDefinition.getDataType()));
                allowing(partitioningColumnSource).getComponentType();
                will(returnValue(partitioningColumnDefinition.getComponentType()));
                oneOf(componentFactory).createRegionedColumnSource(with(same(groupingColumnDefinition)),
                        with(ColumnToCodecMappings.EMPTY));
                will(returnValue(groupingColumnSource));
                oneOf(componentFactory).createRegionedColumnSource(with(same(normalColumnDefinition)),
                        with(ColumnToCodecMappings.EMPTY));
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
        locationIndexToRegionIndex = new TIntIntHashMap(4, 0.5f, -1, -1);
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
                will(returnValue(Collections.singletonList((new String[] {groupingColumnDefinition.getName()}))));
                allowing(tl).hasDataIndex(groupingColumnDefinition.getName());
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
        checking(new Expectations() {
            {
                exactly(1).of(partitioningColumnSource).invalidateRegion(3);
                exactly(1).of(groupingColumnSource).invalidateRegion(3);
                exactly(1).of(normalColumnSource).invalidateRegion(3);
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
        SUT = new RegionedColumnSourceManager(false, componentFactory, ColumnToCodecMappings.EMPTY, columnDefinitions);
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
        capturedGroupingColumnIndex.table(); // Force us to build the merged index *before* we check satisfaction

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
        public @NotNull Table table() {
            return table;
        }

        @Override
        public boolean isRefreshing() {
            return false;
        }
    }

    @Test
    public void testStaticOverflow() {
        SUT = new RegionedColumnSourceManager(false, componentFactory, ColumnToCodecMappings.EMPTY, columnDefinitions);

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
        SUT = new RegionedColumnSourceManager(true, componentFactory, ColumnToCodecMappings.EMPTY, columnDefinitions);
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

    private static void maybePrintStackTrace(@NotNull final Exception e) {
        if (PRINT_STACK_TRACES) {
            e.printStackTrace();
        }
    }
}
