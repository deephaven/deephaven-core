//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.SimpleTableLocationKey;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.table.impl.perf.PerformanceEntry;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TestNotification;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.LogicalClock;
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

/**
 * Test for a {@link PartitionAwareSourceTable} that does not permit removing TableLocations. The test in
 * {@link TestPartitionAwareSourceTable} permits adds and removes of the table locations; whereas this version does not.
 * It only focuses on testing the behavior of removed locations; filtered [which should work] and unfiltered [which
 * should deliver an error].
 */
@SuppressWarnings({"AutoBoxing", "JUnit4AnnotatedMethodInJUnit3TestCase", "AnonymousInnerClassMayBeStatic"})
public class TestPartitionAwareSourceTableNoRemoval extends RefreshingTableTestCase {

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

        @OverridingMethodsMustInvokeSuper
        @Override
        protected synchronized void destroy() {
            super.destroy();
            tableLocation = null;
        }
    }

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
                will(returnValue(TableUpdateMode.ADD_ONLY));
                allowing(locationProvider).getLocationUpdateMode();
                will(returnValue(TableUpdateMode.ADD_ONLY));
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

        final Map<String, ColumnDefinition<?>> partDef =
                TABLE_DEFINITION.getColumnStream().filter(ColumnDefinition::isPartitioning)
                        .collect(Collectors.toMap(ColumnDefinition::getName, cd -> cd));

        SUT = new PartitionAwareSourceTable(TABLE_DEFINITION, "", componentFactory, locationProvider,
                ExecutionContext.getContext().getUpdateGraph(), partDef,
                WhereFilter.of(FilterComparison.eq(ColumnName.of("Date"), Literal.of("D3"))));
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
    public void testRemoveFilteredLocation() {
        doInitializeCheck(locationKeysSlice(0, 3), makePassingLocations(3), true);
        doRefreshOkCheck();
        doRemoveLocations(locationKeysSlice(0));
        doRefreshOkCheck();
    }

    @Test
    public void testRemoveIncludedLocation() {
        doInitializeCheck(locationKeysSlice(0, 3), makePassingLocations(3), true);
        doRefreshOkCheck();
        doRemoveLocations(locationKeysSlice(3));
        doBadRefreshCheck();
    }

    private void doInitializeCheck(final ImmutableTableLocationKey[] tableLocationKeys,
            final Set<TableLocation> expectPassFilters, final boolean coalesceAndListen) {
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
                will(returnValue(toAdd));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(getIncludedColumnsMap(0, 1, 2, 3, 4)));
            }
        });
        expectPassFilters.forEach(tl -> checking(new Expectations() {
            {
                oneOf(columnSourceManager).addLocation(tl);
            }
        }));

        expectedRowSet.insert(toAdd);
        if (coalesceAndListen) {
            coalesced = SUT.coalesce();
            coalesced.retainReference();
            coalesced.addUpdateListener(listener);
            assertIsSatisfied();
            assertRowSetEquals(expectedRowSet, SUT.getRowSet());
        }
    }

    private void doRefreshOkCheck() {
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

    private void doBadRefreshCheck() {
        checking(new Expectations() {
            {
                // the notification that emanates from the deliverError is delayed, because sources are always
                // satisfied. The way the delayed machinery works is that it adds a new source to the update graph,
                // but the way the unit test framework works is that the update graph ignores added sources. Therefore,
                // the result table never gets an error notification within the unit test.
                oneOf(columnSourceManager).deliverError(with(any(TableLocationRemovedException.class)),
                        with(any(PerformanceEntry.class)));
            }
        });

        notification.reset();
        ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().startCycleForUnitTests();
        try {
            SUT.refresh();
        } finally {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().completeCycleForUnitTests();
        }
        assertIsSatisfied();
        notification.assertNotInvoked();
    }

    private void doRemoveLocations(final ImmutableTableLocationKey[] tableLocationKeys) {
        Arrays.stream(tableLocationKeys).map(TestKeySupplier::new)
                .forEach(subscriptionBuffer::handleTableLocationKeyRemoved);
    }
}
