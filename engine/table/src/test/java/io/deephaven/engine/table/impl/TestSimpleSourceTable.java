/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.StandaloneTableLocationKey;
import io.deephaven.engine.table.impl.sources.DeferredGroupingColumnSource;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.table.impl.TstUtils.assertIndexEquals;

/**
 * Tests for {@link SimpleSourceTable}.
 */
@SuppressWarnings({"AutoBoxing", "JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TestSimpleSourceTable extends RefreshingTableTestCase {

    private static final int NUM_COLUMNS = 4;
    private static final ColumnDefinition<Boolean> BOOLEAN_COLUMN_DEFINITION = ColumnDefinition.ofBoolean("Active");
    private static final ColumnDefinition<Character> CHARACTER_COLUMN_DEFINITION =
            ColumnDefinition.ofChar("Type").withGrouping();
    private static final ColumnDefinition<Integer> INTEGER_COLUMN_DEFINITION = ColumnDefinition.ofInt("Size");
    private static final ColumnDefinition<Double> DOUBLE_COLUMN_DEFINITION = ColumnDefinition.ofDouble("Price");

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            BOOLEAN_COLUMN_DEFINITION,
            CHARACTER_COLUMN_DEFINITION,
            INTEGER_COLUMN_DEFINITION,
            DOUBLE_COLUMN_DEFINITION);

    private static final long INDEX_INCREMENT = 1000;

    private SourceTableComponentFactory componentFactory;
    private ColumnSourceManager columnSourceManager;

    private DeferredGroupingColumnSource<?>[] columnSources;

    private TableLocationProvider locationProvider;
    private TableLocation tableLocation;

    private WritableRowSet expectedRowSet;

    private SimpleSourceTable SUT;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        componentFactory = mock(SourceTableComponentFactory.class);
        columnSourceManager = mock(ColumnSourceManager.class);
        columnSources = TABLE_DEFINITION.getColumnStream().map(cd -> {
            final DeferredGroupingColumnSource<?> mocked = mock(DeferredGroupingColumnSource.class, cd.getName());
            checking(new Expectations() {
                {
                    allowing(mocked).getType();
                    will(returnValue(cd.getDataType()));
                    allowing(mocked).getComponentType();
                    will(returnValue(cd.getComponentType()));
                    allowing(mocked).isStateless();
                    will(returnValue(true));
                }
            });
            return mocked;
        }).toArray(DeferredGroupingColumnSource[]::new);
        locationProvider = mock(TableLocationProvider.class);
        tableLocation = mock(TableLocation.class);
        checking(new Expectations() {
            {
                allowing(locationProvider).getTableLocationKeys();
                will(returnValue(Collections.singleton(StandaloneTableLocationKey.getInstance())));
                allowing(locationProvider).getTableLocation(with(StandaloneTableLocationKey.getInstance()));
                will(returnValue(tableLocation));
                allowing(tableLocation).supportsSubscriptions();
                will(returnValue(true));
                allowing(tableLocation).getKey();
                will(returnValue(StandaloneTableLocationKey.getInstance()));
                allowing(locationProvider).supportsSubscriptions();
                will(returnValue(true));
            }
        });

        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(
                        with(false),
                        with(ColumnToCodecMappings.EMPTY),
                        with(equal(TABLE_DEFINITION.getColumns())));
                will(returnValue(columnSourceManager));
            }
        });

        expectedRowSet = RowSetFactory.empty();

        // Since TestPAST covers refreshing SourceTables, let this cover the static case.
        SUT = new SimpleSourceTable(TABLE_DEFINITION, "", componentFactory, locationProvider, null);
        assertIsSatisfied();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private static List<ColumnDefinition<?>> getIncludedColumnDefs(final int... indices) {
        return IntStream.of(indices).mapToObj(ci -> TABLE_DEFINITION.getColumns().get(ci))
                .collect(Collectors.toList());
    }

    private static String[] getIncludedColumnNames(final int... indices) {
        return IntStream.of(indices).mapToObj(ci -> TABLE_DEFINITION.getColumns().get(ci).getName())
                .toArray(String[]::new);
    }

    private static String[] getExcludedColumnNames(final TableDefinition currentDef, final int... indices) {
        final Set<String> includedNames = IntStream.of(indices)
                .mapToObj(ci -> TABLE_DEFINITION.getColumns().get(ci).getName()).collect(Collectors.toSet());
        return currentDef.getColumnStream().map(ColumnDefinition::getName).filter(n -> !includedNames.contains(n))
                .toArray(String[]::new);
    }

    private Map<String, ? extends DeferredGroupingColumnSource<?>> getIncludedColumnsMap(final int... indices) {
        return IntStream.of(indices)
                .mapToObj(ci -> new Pair<>(TABLE_DEFINITION.getColumns().get(ci).getName(), columnSources[ci]))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, Assert::neverInvoked, LinkedHashMap::new));
    }

    @Test
    public void testInitialize() {
        doSingleLocationInitializeCheck(false, true);
    }

    @Test
    public void testInitializeException() {
        doSingleLocationInitializeCheck(true, true);
    }

    private void doSingleLocationInitializeCheck(final boolean throwException,
            @SuppressWarnings("SameParameterValue") final boolean coalesce) {
        Assert.assertion(!(throwException && !coalesce), "!(throwException && !listen)");
        final TableDataException exception = new TableDataException("test");
        final RowSet toAdd =
                RowSetFactory.fromRange(expectedRowSet.lastRowKey() + 1,
                        expectedRowSet.lastRowKey() + INDEX_INCREMENT);

        checking(new Expectations() {
            {
                oneOf(locationProvider).refresh();
                oneOf(columnSourceManager).addLocation(tableLocation);
                oneOf(columnSourceManager).refresh();
                if (throwException) {
                    will(throwException(exception));
                } else {
                    will(returnValue(toAdd));
                    oneOf(columnSourceManager).getColumnSources();
                    will(returnValue(getIncludedColumnsMap(0, 1, 2, 3)));
                }
            }
        });
        expectedRowSet.insert(toAdd);
        if (coalesce) {
            final RowSet rowSet;
            try {
                rowSet = SUT.getRowSet();
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
            assertIndexEquals(expectedRowSet, rowSet);
            assertIsSatisfied();
        }
    }

    @Test
    public void testRedefinition() {
        UpdateGraphProcessor.DEFAULT.exclusiveLock().doLocked(this::doTestRedefinition);
    }

    private void doTestRedefinition() {
        // Note: We expect redefinition to make a new CSM, but no work until we force a coalesce by asking for column
        // sources

        // Test 1: Drop a column
        // Setup the table
        final int[] includedColumnIndices1 = new int[] {1, 2, 3};
        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(
                        with(false),
                        with(ColumnToCodecMappings.EMPTY),
                        with(equal(getIncludedColumnDefs(includedColumnIndices1))));
                will(returnValue(columnSourceManager));
            }
        });
        final Table dropColumnsResult1 =
                SUT.dropColumns(getExcludedColumnNames(SUT.getDefinition(), includedColumnIndices1));
        assertIsSatisfied();
        assertTrue(dropColumnsResult1 instanceof SimpleSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {
            {
                oneOf(locationProvider).refresh();
                oneOf(columnSourceManager).addLocation(tableLocation);
                oneOf(columnSourceManager).refresh();
                will(returnValue(RowSetFactory.empty()));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(getIncludedColumnsMap(includedColumnIndices1)));
            }
        });
        assertEquals(NUM_COLUMNS - 1, dropColumnsResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(dropColumnsResult1.getColumnSource(CHARACTER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult1.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult1.getColumnSource(DOUBLE_COLUMN_DEFINITION.getName()));

        // Test 2: Drop another column
        // Setup the table
        final int[] includedColumnIndices2 = new int[] {2, 3};
        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(
                        with(false),
                        with(ColumnToCodecMappings.EMPTY),
                        with(equal(getIncludedColumnDefs(includedColumnIndices2))));
                will(returnValue(columnSourceManager));
            }
        });
        final Table dropColumnsResult2 = dropColumnsResult1
                .dropColumns(getExcludedColumnNames(dropColumnsResult1.getDefinition(), includedColumnIndices2));
        assertIsSatisfied();
        assertTrue(dropColumnsResult2 instanceof SimpleSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {
            {
                oneOf(locationProvider).refresh();
                oneOf(columnSourceManager).addLocation(tableLocation);
                oneOf(columnSourceManager).refresh();
                will(returnValue(RowSetFactory.empty()));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(getIncludedColumnsMap(includedColumnIndices2)));
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
        final int[] includedColumnIndices3 = new int[] {2};
        checking(new Expectations() {
            {
                oneOf(componentFactory).createColumnSourceManager(
                        with(false),
                        with(ColumnToCodecMappings.EMPTY),
                        with(equal(getIncludedColumnDefs(includedColumnIndices3))));
                will(returnValue(columnSourceManager));
            }
        });
        final Table viewResult1 = dropColumnsResult2.view(getIncludedColumnNames(includedColumnIndices3));
        assertIsSatisfied();
        assertTrue(viewResult1 instanceof SimpleSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {
            {
                oneOf(locationProvider).refresh();
                oneOf(columnSourceManager).addLocation(tableLocation);
                oneOf(columnSourceManager).refresh();
                will(returnValue(RowSetFactory.empty()));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(getIncludedColumnsMap(includedColumnIndices3)));
            }
        });
        assertEquals(NUM_COLUMNS - 3, viewResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(viewResult1.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));

        // Test 5: Add a new derived column on
        // Setup the table
        final Table viewResult2 = viewResult1.updateView(
                "SizeSquared=" + INTEGER_COLUMN_DEFINITION.getName() + '*' + INTEGER_COLUMN_DEFINITION.getName());
        assertTrue(viewResult2 instanceof DeferredViewTable);
        assertEquals(NUM_COLUMNS - 2, viewResult2.getColumnSources().size());
        assertNotNull(viewResult2.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));
        assertNotNull(viewResult2.getColumnSource("SizeSquared"));
        assertIsSatisfied();

        final Table viewResult3 = viewResult2.view("Result=SizeSquared");
        assertTrue(viewResult3 instanceof DeferredViewTable);
        assertEquals(NUM_COLUMNS - 3, viewResult3.getColumnSources().size());
        assertNotNull(viewResult3.getColumnSource("Result"));
        assertIsSatisfied();

        final Table viewResult4 = viewResult2.view("SizeSquared");
        assertTrue(viewResult4 instanceof DeferredViewTable);
        assertEquals(NUM_COLUMNS - 3, viewResult4.getColumnSources().size());
        assertNotNull(viewResult4.getColumnSource("SizeSquared"));
        assertIsSatisfied();
    }
}
