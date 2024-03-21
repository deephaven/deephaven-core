//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.tuple.ArrayTuple;

import static io.deephaven.engine.util.TableTools.*;

public class TestKeyedTableListener extends RefreshingTableTestCase {

    private QueryTable table;
    private KeyedTableListener keyedTableListener;
    private KeyedTableListener.KeyUpdateListener mockListener;

    private RowSet noAdded;
    private RowSet noRemoved;
    private RowSet noModified;

    private ArrayTuple aKey;
    private ArrayTuple bKey;
    private ArrayTuple cKey;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.noAdded = RowSetFactory.empty();
        this.noRemoved = RowSetFactory.empty();
        this.noModified = RowSetFactory.empty();

        this.mockListener = mock(KeyedTableListener.KeyUpdateListener.class);
        this.table = TstUtils.testRefreshingTable(TstUtils.i(0, 1, 2).toTracking(),
                col("Key1", "A", "B", "C"),
                intCol("Key2", 1, 2, 3),
                doubleCol("Data", 1.0, 2.0, 3.0));
        this.aKey = new ArrayTuple("A", 1);
        this.bKey = new ArrayTuple("B", 2);
        this.cKey = new ArrayTuple("C", 3);
        this.keyedTableListener = new KeyedTableListener(table, "Key1", "Key2");
        // enable immediately
        ExecutionContext.getContext().getUpdateGraph().sharedLock()
                .doLocked(() -> this.keyedTableListener.addUpdateListener());
    }

    public void testGetRow() {
        Object[] data;

        data = keyedTableListener.getRow(new ArrayTuple("A", 1));
        assertEquals(1.0, data[2]);

        data = keyedTableListener.getRow(new ArrayTuple("B", 2));
        assertEquals(2.0, data[2]);

        data = keyedTableListener.getRow(new ArrayTuple("C", 3));
        assertEquals(3.0, data[2]);

        // Wrong key
        data = keyedTableListener.getRow(new ArrayTuple("A", 2));
        assertNull(data);
    }

    public void testNoChanges() {
        checking(new Expectations() {
            {
                never(mockListener).update(with(any(KeyedTableListener.class)), with(any(ArrayTuple.class)),
                        with(any(long.class)), with(any(KeyedTableListener.KeyEvent.class)));
            }
        });
        keyedTableListener.subscribe(aKey, mockListener);
        keyedTableListener.subscribe(bKey, mockListener);
        keyedTableListener.subscribe(cKey, mockListener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(
                () -> table.notifyListeners(noAdded.copy(), noRemoved.copy(), noModified.copy()));

        keyedTableListener.unsubscribe(aKey, mockListener);
        keyedTableListener.unsubscribe(bKey, mockListener);
        keyedTableListener.unsubscribe(cKey, mockListener);
    }

    public void testAdd() {
        final ArrayTuple newKey = new ArrayTuple("D", 4);
        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey), with(3L),
                        with(KeyedTableListener.KeyEvent.ADDED));
            }
        });
        keyedTableListener.subscribe(newKey, mockListener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newAdd = TstUtils.i(3);
            TstUtils.addToTable(table, newAdd, col("Key1", "D"), col("Key2", 4), col("Data", 4.0));
            table.notifyListeners(newAdd, noRemoved.copy(), noModified.copy());
        });

        // Check that the new values are available
        final Object[] vals = keyedTableListener.getRow(newKey);
        assertEquals(4.0, vals[2]);

        keyedTableListener.unsubscribe(newKey, mockListener);
    }

    public void testRemoved() {
        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(2L),
                        with(KeyedTableListener.KeyEvent.REMOVED));
            }
        });
        keyedTableListener.subscribe(cKey, mockListener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newRemove = TstUtils.i(2);
            TstUtils.removeRows(table, newRemove);
            table.notifyListeners(noAdded.copy(), newRemove, noModified.copy());
        });

        // Check the values are missing
        final Object[] vals = keyedTableListener.getRow(cKey);
        assertNull(vals);

        keyedTableListener.unsubscribe(cKey, mockListener);
    }

    public void testModify() {
        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(2L),
                        with(KeyedTableListener.KeyEvent.MODIFIED));
            }
        });

        // Check the original value is still there
        Object[] vals = keyedTableListener.getRow(cKey);
        assertEquals(3.0, vals[2]);

        keyedTableListener.subscribe(cKey, mockListener);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newModified = TstUtils.i(2);
            TstUtils.addToTable(table, newModified, col("Key1", "C"), col("Key2", 3),
                    col("Data", 6.0));
            table.notifyListeners(noAdded.copy(), noRemoved.copy(), newModified);
        });

        // Check the value has changed
        vals = keyedTableListener.getRow(cKey);
        assertEquals(6.0, vals[2]);

        keyedTableListener.unsubscribe(cKey, mockListener);
    }

    public void testModifyChangedKey() {
        final ArrayTuple newKey = new ArrayTuple("C", 4);

        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(2L),
                        with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey), with(2L),
                        with(KeyedTableListener.KeyEvent.ADDED));
            }
        });

        keyedTableListener.subscribe(cKey, mockListener);
        keyedTableListener.subscribe(newKey, mockListener);

        // Add to table on an existing row key is a modify
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newModified = TstUtils.i(2);
            // Add to table on an existing row key is a modify
            TstUtils.addToTable(table, newModified, col("Key1", "C"), col("Key2", 4),
                    col("Data", 6.0));
            table.notifyListeners(noAdded.copy(), noRemoved.copy(), newModified);
        });

        // Check that the old key returns null now
        Object[] vals = keyedTableListener.getRow(cKey);
        assertNull(vals);

        // Check that the new key fetches data
        vals = keyedTableListener.getRow(newKey);
        assertEquals(6.0, vals[2]);

        keyedTableListener.unsubscribe(cKey, mockListener);
        keyedTableListener.unsubscribe(newKey, mockListener);
    }

    // Move an existing key up, while adding one to fill its place
    public void testModifyKeyMoved() {
        final ArrayTuple newKey = new ArrayTuple("D", 4);

        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(bKey), with(1L),
                        with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(2L),
                        with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(1L),
                        with(KeyedTableListener.KeyEvent.ADDED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey), with(2L),
                        with(KeyedTableListener.KeyEvent.ADDED));
            }
        });

        keyedTableListener.subscribe(bKey, mockListener);
        keyedTableListener.subscribe(cKey, mockListener);
        keyedTableListener.subscribe(newKey, mockListener);

        // Add to table on an existing row key is a modify
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newModified = TstUtils.i(1, 2);
            // Add to table on an existing row key is a modify
            TstUtils.addToTable(table, newModified, col("Key1", "C", "D"), col("Key2", 3, 4),
                    col("Data", 3.0, 4.0));
            table.notifyListeners(noAdded.copy(), noRemoved.copy(), newModified);
        });

        // Check that the old key returns null now
        Object[] vals = keyedTableListener.getRow(bKey);
        assertNull(vals);

        vals = keyedTableListener.getRow(cKey);
        assertEquals(3.0, vals[2]);

        // Check that the new key fetches data
        vals = keyedTableListener.getRow(newKey);
        assertEquals(4.0, vals[2]);

        keyedTableListener.unsubscribe(bKey, mockListener);
        keyedTableListener.unsubscribe(cKey, mockListener);
        keyedTableListener.unsubscribe(newKey, mockListener);
    }

    // Swap the places of B and C keys
    public void testModifySwap() {
        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(bKey), with(1L),
                        with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(1L),
                        with(KeyedTableListener.KeyEvent.ADDED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(2L),
                        with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(bKey), with(2L),
                        with(KeyedTableListener.KeyEvent.ADDED));
            }
        });
        keyedTableListener.subscribe(bKey, mockListener);
        keyedTableListener.subscribe(cKey, mockListener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newModified = TstUtils.i(1, 2);
            TstUtils.addToTable(table, newModified, col("Key1", "C", "B"), col("Key2", 3, 2),
                    col("Data", 3.0, 2.0));
            table.notifyListeners(noAdded.copy(), noRemoved.copy(), newModified);
        });

        // Check that the keys still return the correct values
        Object[] vals = keyedTableListener.getRow(bKey);
        assertEquals(2.0, vals[2]);

        vals = keyedTableListener.getRow(cKey);
        assertEquals(3.0, vals[2]);

        keyedTableListener.unsubscribe(bKey, mockListener);
        keyedTableListener.unsubscribe(cKey, mockListener);
    }

    // Test the combination of an add / remove and modify
    public void testAddRemoveModify() {
        final ArrayTuple newKey = new ArrayTuple("D", 4);

        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(aKey), with(0L),
                        with(KeyedTableListener.KeyEvent.MODIFIED));
                never(mockListener).update(with(any(KeyedTableListener.class)), with(bKey), with(1L),
                        with(any(KeyedTableListener.KeyEvent.class)));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(2L),
                        with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey), with(4L),
                        with(KeyedTableListener.KeyEvent.ADDED));
            }
        });

        keyedTableListener.subscribe(aKey, mockListener);
        keyedTableListener.subscribe(bKey, mockListener);
        keyedTableListener.subscribe(cKey, mockListener);
        keyedTableListener.subscribe(newKey, mockListener);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newRemoved = TstUtils.i(2);
            TstUtils.removeRows(table, newRemoved);

            final RowSet newModified = TstUtils.i(0);
            TstUtils.addToTable(table, newModified, col("Key1", "A"), col("Key2", 1),
                    col("Data", 1.5));

            final RowSet newAdd = TstUtils.i(4);
            TstUtils.addToTable(table, newAdd, col("Key1", "D"), col("Key2", 4), col("Data", 4.0));

            table.notifyListeners(newAdd, newRemoved, newModified);
        });

        // Check that the aKey has a new value
        Object[] vals = keyedTableListener.getRow(aKey);
        assertEquals(1.5, vals[2]);

        // Check bKey stayed the same
        vals = keyedTableListener.getRow(bKey);
        assertEquals(2.0, vals[2]);

        // Check cKey is removed
        vals = keyedTableListener.getRow(cKey);
        assertNull(vals);

        // Check newKey was added
        vals = keyedTableListener.getRow(newKey);
        assertEquals(4.0, vals[2]);

        keyedTableListener.unsubscribe(aKey, mockListener);
        keyedTableListener.unsubscribe(bKey, mockListener);
        keyedTableListener.unsubscribe(cKey, mockListener);
        keyedTableListener.unsubscribe(newKey, mockListener);
    }

    public void testRemoveAdd() {
        final ArrayTuple newKey = new ArrayTuple("D", 4);

        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey), with(2L),
                        with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey), with(2L),
                        with(KeyedTableListener.KeyEvent.ADDED));
            }
        });

        keyedTableListener.subscribe(cKey, mockListener);
        keyedTableListener.subscribe(newKey, mockListener);

        // Two cycles -- first remove
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newRemoved = TstUtils.i(2);
            TstUtils.removeRows(table, newRemoved);
            table.notifyListeners(noAdded.copy(), newRemoved, noModified.copy());
        });

        // Now add
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet newAdded = TstUtils.i(2);
            TstUtils.addToTable(table, newAdded, col("Key1", "D"), col("Key2", 4),
                    col("Data", 4.0));
            table.notifyListeners(newAdded, noRemoved.copy(), noModified.copy());
        });

        // Check cKey is removed
        Object[] vals = keyedTableListener.getRow(cKey);
        assertNull(vals);

        // Check newKey was added
        vals = keyedTableListener.getRow(newKey);
        assertEquals(4.0, vals[2]);

        keyedTableListener.unsubscribe(cKey, mockListener);
        keyedTableListener.unsubscribe(newKey, mockListener);
    }
}
