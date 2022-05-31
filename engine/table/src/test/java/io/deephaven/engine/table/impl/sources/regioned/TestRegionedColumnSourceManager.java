package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.ColumnToCodecMappings;
import io.deephaven.engine.table.impl.RefreshingTableTestCase;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.SimpleTableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntIntHashMap;
import org.jetbrains.annotations.NotNull;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.table.impl.TstUtils.assertIndexEquals;
import static io.deephaven.engine.table.impl.locations.TableLocationState.NULL_SIZE;
import static io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource.REGION_CAPACITY_IN_ELEMENTS;

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
    private static boolean PRINT_STACK_TRACES = false;

    private RegionedTableComponentFactory componentFactory;

    private List<ColumnDefinition<?>> columnDefinitions;
    private ColumnDefinition<?> partitioningColumnDefinition;
    private ColumnDefinition<?> groupingColumnDefinition;
    private ColumnDefinition<?> normalColumnDefinition;

    private RegionedColumnSource[] columnSources;
    private RegionedColumnSource partitioningColumnSource;
    private RegionedColumnSource groupingColumnSource;
    private RegionedColumnSource normalColumnSource;

    private ColumnLocation[][] columnLocations;

    private TableLocation[] tableLocations;
    private TableLocation tableLocation0A;
    private TableLocation tableLocation0B;
    private TableLocation tableLocation1A;
    private TableLocation tableLocation1B;

    private TableLocation duplicateTableLocation0A;

    private Map<String, RowSet> partitioningColumnGrouping;
    private KeyRangeGroupingProvider groupingColumnGroupingProvider;

    private TableLocationUpdateSubscriptionBuffer[] subscriptionBuffers;
    private long[] lastSizes;
    private int regionCount;
    private TIntIntMap locationIndexToRegionIndex;
    private RowSet expectedRowSet;
    private RowSet expectedAddedRowSet;
    private Map<String, WritableRowSet> expectedPartitioningColumnGrouping;

    private RegionedColumnSourceManager SUT;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        componentFactory = mock(RegionedTableComponentFactory.class);

        partitioningColumnDefinition = ColumnDefinition.ofString("RCS_0").withPartitioning();
        groupingColumnDefinition = ColumnDefinition.ofString("RCS_1").withGrouping();
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

        duplicateTableLocation0A = setUpTableLocation(0, "-duplicate");

        subscriptionBuffers = new TableLocationUpdateSubscriptionBuffer[NUM_LOCATIONS];
        lastSizes = new long[NUM_LOCATIONS];
        Arrays.fill(lastSizes, -1); // Not null size
        regionCount = 0;
        locationIndexToRegionIndex = new TIntIntHashMap(4, 0.5f, -1, -1);
        expectedRowSet = RowSetFactory.empty();
        expectedAddedRowSet = RowSetFactory.empty();
        expectedPartitioningColumnGrouping = new LinkedHashMap<>();
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

    private Map<String, ColumnSource> makeColumnSourceMap() {
        final Map<String, ColumnSource> result = new LinkedHashMap<>();
        IntStream.range(0, columnDefinitions.size())
                .forEachOrdered(ci -> result.put(columnDefinitions.get(ci).getName(), columnSources[ci]));
        return result;
    }

    private void expectPartitioningColumnInitialGrouping() {
        partitioningColumnGrouping = null;
        checking(new Expectations() {
            {
                allowing(partitioningColumnSource).getGroupToRange();
                will(new CustomAction("Return previously set partitioning column grouping") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        return partitioningColumnGrouping;
                    }
                });
                oneOf(partitioningColumnSource).setGroupToRange(with(any(Map.class)));
                will(new CustomAction("Capture partitioning column grouping") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        partitioningColumnGrouping = (Map) invocation.getParameter(0);
                        return null;
                    }
                });
            }
        });
    }

    private void expectGroupingColumnInitialGrouping() {
        groupingColumnGroupingProvider = null;
        checking(new Expectations() {
            {
                allowing(groupingColumnSource).getGroupingProvider();
                will(new CustomAction("Return previously set grouping column grouping provider") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        return groupingColumnGroupingProvider;
                    }
                });
                oneOf(groupingColumnSource).setGroupingProvider(with(any(GroupingProvider.class)));
                will(new CustomAction("Capture grouping column grouping provider") {
                    @Override
                    public Object invoke(Invocation invocation) {
                        groupingColumnGroupingProvider = (KeyRangeGroupingProvider) invocation.getParameter(0);
                        return null;
                    }
                });
            }
        });
    }

    private void setSizeExpectations(final boolean refreshing, final long... sizes) {
        final WritableRowSet newExpectedRowSet = RowSetFactory.empty();
        expectedPartitioningColumnGrouping = new LinkedHashMap<>();
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
                expectedPartitioningColumnGrouping.computeIfAbsent(cp, cpk -> RowSetFactory.empty())
                        .insertRange(
                                RegionedColumnSource.getFirstRowKey(regionIndex),
                                RegionedColumnSource.getFirstRowKey(regionIndex) + size - 1);
            }
        });
        expectedAddedRowSet = newExpectedRowSet.minus(expectedRowSet);
        expectedRowSet = newExpectedRowSet;
    }

    private void checkIndexes(@NotNull final RowSet addedRowSet) {
        assertIsSatisfied();
        assertIndexEquals(expectedAddedRowSet, addedRowSet);
        if (partitioningColumnGrouping == null) {
            assertTrue(expectedPartitioningColumnGrouping.isEmpty());
        } else {
            assertEquals(expectedPartitioningColumnGrouping.keySet(), partitioningColumnGrouping.keySet());
            expectedPartitioningColumnGrouping
                    .forEach((final String columnPartition, final RowSet expectedGrouping) -> {
                        final RowSet grouping = partitioningColumnGrouping.get(columnPartition);
                        assertIndexEquals(expectedGrouping, grouping);
                    });
        }
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
        expectPartitioningColumnInitialGrouping();
        expectGroupingColumnInitialGrouping();
        setSizeExpectations(false, NULL_SIZE, 100, 0, REGION_CAPACITY_IN_ELEMENTS);

        checkIndexes(SUT.refresh());
        assertEquals(Arrays.asList(tableLocation1A, tableLocation1B), SUT.includedLocations());
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
            SUT.refresh();
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

        // Disable grouping, as we don't maintain it for refreshing instances
        checking(new Expectations() {
            {
                oneOf(groupingColumnSource).setGroupingProvider(null);
                oneOf(groupingColumnSource).setGroupToRange(null);
            }
        });
        SUT.disableGrouping();
        assertIsSatisfied();

        // Do it a second time, to test that it's a no-op
        SUT.disableGrouping();
        assertIsSatisfied();

        // Check run with no locations
        checkIndexes(SUT.refresh());

        // Add a few locations
        Arrays.stream(tableLocations).limit(2).forEach(SUT::addLocation);
        assertEquals(Arrays.stream(tableLocations).limit(2).collect(Collectors.toList()), SUT.allLocations());
        assertTrue(SUT.includedLocations().isEmpty());

        // Refresh them
        expectPartitioningColumnInitialGrouping();
        setSizeExpectations(true, 5, 1000);
        checkIndexes(SUT.refresh());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.includedLocations());

        // Refresh them with no change
        setSizeExpectations(true, 5, 1000);
        checkIndexes(SUT.refresh());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A), SUT.includedLocations());

        // Refresh them with a change for the subscription-supporting one
        setSizeExpectations(true, 5, 1001);
        checkIndexes(SUT.refresh());
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
        setSizeExpectations(true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, NULL_SIZE);
        checkIndexes(SUT.refresh());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B), SUT.includedLocations());

        // Test no-op run
        setSizeExpectations(true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, NULL_SIZE);
        checkIndexes(SUT.refresh());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B), SUT.includedLocations());

        // Test run with a location updated from null to not
        setSizeExpectations(true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, 2);
        checkIndexes(SUT.refresh());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with a location updated
        setSizeExpectations(true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, 10000002);
        checkIndexes(SUT.refresh());
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with a size decrease
        setSizeExpectations(true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, 2);
        try {
            checkIndexes(SUT.refresh());
            fail("Expected exception");
        } catch (AssertionFailure expected) {
            maybePrintStackTrace(expected);
        }
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with a location truncated
        setSizeExpectations(true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, NULL_SIZE);
        try {
            checkIndexes(SUT.refresh());
            fail("Expected exception");
        } catch (TableDataException expected) {
            maybePrintStackTrace(expected);
        }
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with an overflow
        setSizeExpectations(true, 5, REGION_CAPACITY_IN_ELEMENTS, 5003, REGION_CAPACITY_IN_ELEMENTS + 1);
        try {
            checkIndexes(SUT.refresh());
            fail("Expected exception");
        } catch (TableDataException expected) {
            maybePrintStackTrace(expected);
        }
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());

        // Test run with an exception
        subscriptionBuffers[3].handleException(new TableDataException("TEST"));
        try {
            checkIndexes(SUT.refresh());
            fail("Expected exception");
        } catch (TableDataException expected) {
            assertEquals("TEST", expected.getCause().getMessage());
        }
        assertEquals(Arrays.asList(tableLocation0A, tableLocation1A, tableLocation0B, tableLocation1B),
                SUT.includedLocations());
    }

    private static void maybePrintStackTrace(@NotNull final Exception e) {
        if (PRINT_STACK_TRACES) {
            e.printStackTrace();
        }
    }
}
