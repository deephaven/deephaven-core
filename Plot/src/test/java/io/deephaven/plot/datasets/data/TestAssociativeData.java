/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.datasets.data;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.plot.errors.PlotIllegalArgumentException;
import io.deephaven.plot.util.tables.TableHandle;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

public class TestAssociativeData extends BaseArrayTestCase {
    private final String[] cats = {"A", "B"};
    private final int[] values = {1, 2};
    private final Integer x = values[0];
    private final Table t = TableTools.newTable(TableTools.col("Cat", cats), TableTools.intCol("Values", values));
    private final TableHandle tableHandle = new TableHandle(t, "Cat", "Values");
    private final AssociativeDataTable<String, Integer, Integer> associativeDataTable =
            new AssociativeDataTable<>(tableHandle, "Cat", "Values", String.class, Integer.class, null);
    private final AssociativeDataHashMap<String, Integer> dataHashMap = new AssociativeDataHashMap<>(null);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);

        // prime the listeners
        associativeDataTable.get(null);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    public void testAssociativeDataHashMap() {

        final Map<String, Integer> moreData = new HashMap<>();
        moreData.put("A", 5);

        dataHashMap.put("Test", 1);
        dataHashMap.put("Test2", 15);

        assertEquals((int) dataHashMap.get("Test"), 1);
        assertEquals((int) dataHashMap.get("Test2"), 15);
        assertTrue(dataHashMap.isModifiable());

        dataHashMap.putAll(moreData);
        assertEquals((int) dataHashMap.get("A"), 5);

        assertNull(dataHashMap.get("MISSING"));
    }

    public void testAssociativeDataTable() {
        try {
            new AssociativeDataTable<String, Integer, Integer>(null, "Cat", "Values", String.class, Integer.class,
                    null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            new AssociativeDataTable<String, Integer, Integer>(tableHandle, null, "Values", String.class, Integer.class,
                    null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        try {
            new AssociativeDataTable<String, Integer, Integer>(tableHandle, "Cat", null, String.class, Integer.class,
                    null);
            TestCase.fail("Expected an exception");
        } catch (PlotIllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Null"));
        }

        assertEquals((int) associativeDataTable.convert(x), 1);
        assertFalse(associativeDataTable.isModifiable());

        try {
            associativeDataTable.put("C", 234);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Modifying"));
        }

        final Map<String, Integer> moreValues = new HashMap<>();
        moreValues.put("C", 234);
        try {
            associativeDataTable.putAll(moreValues);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Modifying"));
        }
    }

    public void testAssociativeDataWithDefault() {
        final int def = 2;
        final Map<String, Integer> moreData = new HashMap<>();
        moreData.put("A", 5);
        final AssociativeDataWithDefault<String, Integer> dataWithDefault = new AssociativeDataWithDefault<>(null);

        assertNull(dataWithDefault.getDefault());
        dataWithDefault.setDefault(def);
        assertEquals((int) dataWithDefault.get("Test"), def);

        try {
            dataWithDefault.put("Test", 1);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("unmodifiable"));
        }

        dataWithDefault.setSpecific(dataHashMap);
        assertTrue(dataWithDefault.isModifiable());
        assertEquals((int) dataWithDefault.get("Test"), 2);
        assertEquals(dataWithDefault.getSpecific(), dataHashMap);
        dataWithDefault.putAll(moreData);
        assertEquals((int) dataWithDefault.get("A"), 5);

        dataWithDefault.setSpecific(associativeDataTable);
        assertFalse(dataWithDefault.isModifiable());
    }
}
