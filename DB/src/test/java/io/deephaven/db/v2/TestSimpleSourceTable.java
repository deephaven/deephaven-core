/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.DefaultColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.TableLocationProvider;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.stream.Collectors;

import static io.deephaven.db.v2.TstUtils.assertIndexEquals;

/**
 * Tests for {@link SimpleSourceTable}.
 */
@SuppressWarnings({"AutoBoxing", "JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TestSimpleSourceTable extends LiveTableTestCase {

    private static final int NUM_COLUMNS = 4;
    private static final ColumnDefinition<Boolean> BOOLEAN_COLUMN_DEFINITION = ColumnDefinition.ofBoolean("Active");
    private static final ColumnDefinition<Character> CHARACTER_COLUMN_DEFINITION = ColumnDefinition.ofChar("Type").withGrouping();
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

    private ColumnSource columnSource;

    private TableLocationProvider locationProvider;
    private TableLocation tableLocation;

    private Index expectedIndex;

    private SimpleSourceTable SUT;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        componentFactory = mock(SourceTableComponentFactory.class);
        columnSourceManager = mock(ColumnSourceManager.class);
        columnSource = mock(ColumnSource.class);
        locationProvider = mock(TableLocationProvider.class);
        tableLocation = mock(TableLocation.class);
        checking(new Expectations() {{
            allowing(locationProvider).getTableLocations();
            will(returnValue(Collections.singleton(tableLocation)));
            allowing(tableLocation).supportsSubscriptions();
            will(returnValue(true));
            allowing(tableLocation).getInternalPartition();
            will(returnValue(TableLocationKey.NULL_PARTITION));
            allowing(tableLocation).getColumnPartition();
            will(returnValue(TableLocationKey.NULL_PARTITION));
            allowing(locationProvider).supportsSubscriptions();
            will(returnValue(true));
        }});

        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(with(false), with(equal(TABLE_DEFINITION.getColumns())));
            will(returnValue(columnSourceManager));
        }});

        expectedIndex = Index.FACTORY.getEmptyIndex();

        // Since TestPAST covers refreshing SourceTables, let this cover the static case.
        SUT = new SimpleSourceTable(TABLE_DEFINITION, "", componentFactory, locationProvider, null);
        assertIsSatisfied();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void testInitialize() {
        doSingleLocationInitializeCheck(false, true);
    }

    @Test
    public void testInitializeException() {
        doSingleLocationInitializeCheck(true, true);
    }

    private void doSingleLocationInitializeCheck(final boolean throwException, @SuppressWarnings("SameParameterValue") final boolean coalesce) {
        Assert.assertion(!(throwException && !coalesce), "!(throwException && !listen)");
        final TableDataException exception = new TableDataException("test");
        final Index toAdd = Index.FACTORY.getIndexByRange(expectedIndex.lastKey() + 1, expectedIndex.lastKey() + INDEX_INCREMENT);

        checking(new Expectations() {{
            oneOf(locationProvider).refresh();
            oneOf(columnSourceManager).addLocation(tableLocation);
            oneOf(columnSourceManager).refresh();
            if (throwException) {
                will(throwException(exception));
            } else {
                will(returnValue(toAdd));
                oneOf(columnSourceManager).getColumnSources();
                will(returnValue(TABLE_DEFINITION.getColumnStream().collect(Collectors.toMap(DefaultColumnDefinition::getName, cd -> columnSource, Assert::neverInvoked, LinkedHashMap::new))));
            }
        }});
        expectedIndex.insert(toAdd);
        if (coalesce) {
            final Index index;
            try {
                index = SUT.getIndex();
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
            assertIndexEquals(expectedIndex, index);
            assertIsSatisfied();
        }
    }

    @Test
    public void testRedefinition() {
        LiveTableMonitor.DEFAULT.exclusiveLock().doLocked(this::doTestRedefinition);
    }

    private void doTestRedefinition() {
        // Note: We expect redefinition to make a new CSM, but no work until we force a coalesce by asking for column sources

        // Test 1: Drop a column
        // Setup the table
        final ColumnDefinition[] includedColumns1 = new ColumnDefinition[]{
                CHARACTER_COLUMN_DEFINITION,
                INTEGER_COLUMN_DEFINITION,
                DOUBLE_COLUMN_DEFINITION
        };
        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(with(false), with(equal(includedColumns1)));
            will(returnValue(columnSourceManager));
        }});
        final Table dropColumnsResult1 = SUT.dropColumns(BOOLEAN_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(dropColumnsResult1 instanceof SimpleSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {{
            oneOf(locationProvider).refresh();
            oneOf(columnSourceManager).addLocation(tableLocation);
            oneOf(columnSourceManager).refresh();
            will(returnValue(Index.FACTORY.getEmptyIndex()));
            oneOf(columnSourceManager).getColumnSources();
            will(returnValue(
                    Arrays.stream(includedColumns1).collect(Collectors.toMap(DefaultColumnDefinition::getName, cd -> columnSource, Assert::neverInvoked, LinkedHashMap::new))));
        }});
        assertEquals(NUM_COLUMNS - 1, dropColumnsResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(dropColumnsResult1.getColumnSource(CHARACTER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult1.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));
        assertNotNull(dropColumnsResult1.getColumnSource(DOUBLE_COLUMN_DEFINITION.getName()));

        // Test 2: Drop another column
        // Setup the table
        final ColumnDefinition[] includedColumns2 = new ColumnDefinition[]{
                INTEGER_COLUMN_DEFINITION,
                DOUBLE_COLUMN_DEFINITION
        };
        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(with(false), with(equal(includedColumns2)));
            will(returnValue(columnSourceManager));
        }});
        final Table dropColumnsResult2 = dropColumnsResult1.dropColumns(CHARACTER_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(dropColumnsResult2 instanceof SimpleSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {{
            oneOf(locationProvider).refresh();
            oneOf(columnSourceManager).addLocation(tableLocation);
            oneOf(columnSourceManager).refresh();
            will(returnValue(Index.FACTORY.getEmptyIndex()));
            oneOf(columnSourceManager).getColumnSources();
            will(returnValue(
                    Arrays.stream(includedColumns2).collect(Collectors.toMap(DefaultColumnDefinition::getName, cd -> columnSource, Assert::neverInvoked, LinkedHashMap::new))));
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
        checking(new Expectations() {{
            exactly(5).of(columnSource).getType();
            will(returnValue(INTEGER_COLUMN_DEFINITION.getDataType()));
            exactly(5).of(columnSource).getComponentType();
            will(returnValue(INTEGER_COLUMN_DEFINITION.getDataType().getComponentType()));
        }});
        assertEquals(NUM_COLUMNS - 2, renameColumnsResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(renameColumnsResult1.getColumnSource("A"));
        assertNotNull(renameColumnsResult1.getColumnSource(DOUBLE_COLUMN_DEFINITION.getName()));

        // Test 4: Use view to slice us down to one column
        // Setup the table
        final ColumnDefinition[] includedColumns3 = new ColumnDefinition[]{
                INTEGER_COLUMN_DEFINITION,
        };
        checking(new Expectations() {{
            oneOf(componentFactory).createColumnSourceManager(with(false), with(equal(includedColumns3)));
            will(returnValue(columnSourceManager));
        }});
        final Table viewResult1 = dropColumnsResult2.view(INTEGER_COLUMN_DEFINITION.getName());
        assertIsSatisfied();
        assertTrue(viewResult1 instanceof SimpleSourceTable);
        // Force a coalesce and make sure it has the right columns
        checking(new Expectations() {{
            oneOf(locationProvider).refresh();
            oneOf(columnSourceManager).addLocation(tableLocation);
            oneOf(columnSourceManager).refresh();
            will(returnValue(Index.FACTORY.getEmptyIndex()));
            oneOf(columnSourceManager).getColumnSources();
            will(returnValue(
                    Arrays.stream(includedColumns3).collect(Collectors.toMap(DefaultColumnDefinition::getName, cd -> columnSource, Assert::neverInvoked, LinkedHashMap::new))));
        }});
        assertEquals(NUM_COLUMNS - 3, viewResult1.getColumnSources().size());
        assertIsSatisfied();
        assertNotNull(viewResult1.getColumnSource(INTEGER_COLUMN_DEFINITION.getName()));

        // Test 5: Add a new derived column on
        // Setup the table
        final Table viewResult2 = viewResult1.updateView("SizeSquared=" + INTEGER_COLUMN_DEFINITION.getName() + '*' + INTEGER_COLUMN_DEFINITION.getName());
        assertTrue(viewResult2 instanceof DeferredViewTable);
        checking(new Expectations() {{
            allowing(columnSource).getType();
            will(returnValue(INTEGER_COLUMN_DEFINITION.getDataType()));
            allowing(columnSource).getComponentType();
            will(returnValue(null));
        }});
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
