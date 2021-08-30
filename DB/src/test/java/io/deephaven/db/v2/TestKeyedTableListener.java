/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.base.testing.BaseCachedJMockTestCase;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.utils.Index;

public class TestKeyedTableListener extends BaseCachedJMockTestCase {

    private QueryTable table;
    private KeyedTableListener keyedTableListener;
    private KeyedTableListener.KeyUpdateListener mockListener;

    private final Index noAdded = Index.FACTORY.getEmptyIndex();
    private final Index noRemoved = Index.FACTORY.getEmptyIndex();
    private final Index noModified = Index.FACTORY.getEmptyIndex();

    private SmartKey aKey;
    private SmartKey bKey;
    private SmartKey cKey;

    @Override
    public void setUp() {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
        this.mockListener = mock(KeyedTableListener.KeyUpdateListener.class);
        this.table = TstUtils.testRefreshingTable(TstUtils.i(0, 1, 2),
            TstUtils.c("Key1", "A", "B", "C"),
            TstUtils.c("Key2", 1, 2, 3),
            TstUtils.c("Data", 1.0, 2.0, 3.0));
        this.aKey = new SmartKey("A", 1);
        this.bKey = new SmartKey("B", 2);
        this.cKey = new SmartKey("C", 3);
        this.keyedTableListener = new KeyedTableListener(table, "Key1", "Key2");
        this.keyedTableListener.listenForUpdates(); // enable immediately
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    public void testGetRow() {
        Object[] data;

        data = keyedTableListener.getRow(new SmartKey("A", 1));
        assertEquals(1.0, data[2]);

        data = keyedTableListener.getRow(new SmartKey("B", 2));
        assertEquals(2.0, data[2]);

        data = keyedTableListener.getRow(new SmartKey("C", 3));
        assertEquals(3.0, data[2]);

        // Wrong key
        data = keyedTableListener.getRow(new SmartKey("A", 2));
        assertEquals(null, data);
    }

    public void testNoChanges() {
        checking(new Expectations() {
            {
                never(mockListener).update(with(any(KeyedTableListener.class)),
                    with(any(SmartKey.class)), with(any(long.class)),
                    with(any(KeyedTableListener.KeyEvent.class)));
            }
        });
        keyedTableListener.subscribe(aKey, mockListener);
        keyedTableListener.subscribe(bKey, mockListener);
        keyedTableListener.subscribe(cKey, mockListener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(
            () -> table.notifyListeners(noAdded.clone(), noRemoved.clone(), noModified.clone()));

        keyedTableListener.unsubscribe(aKey, mockListener);
        keyedTableListener.unsubscribe(bKey, mockListener);
        keyedTableListener.unsubscribe(cKey, mockListener);
    }

    public void testAdd() {
        final SmartKey newKey = new SmartKey("D", 4);
        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey),
                    with(3L), with(KeyedTableListener.KeyEvent.ADDED));
            }
        });
        keyedTableListener.subscribe(newKey, mockListener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newAdd = TstUtils.i(3);
            TstUtils.addToTable(table, newAdd, TstUtils.c("Key1", "D"), TstUtils.c("Key2", 4),
                TstUtils.c("Data", 4.0));
            table.notifyListeners(newAdd, noRemoved.clone(), noModified.clone());
        });

        // Check that the new values are available
        final Object[] vals = keyedTableListener.getRow(newKey);
        assertEquals(4.0, vals[2]);

        keyedTableListener.unsubscribe(newKey, mockListener);
    }

    public void testRemoved() {
        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(2L), with(KeyedTableListener.KeyEvent.REMOVED));
            }
        });
        keyedTableListener.subscribe(cKey, mockListener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newRemove = TstUtils.i(2);
            TstUtils.removeRows(table, newRemove);
            table.notifyListeners(noAdded.clone(), newRemove, noModified.clone());
        });

        // Check the values are missing
        final Object[] vals = keyedTableListener.getRow(cKey);
        assertNull(vals);

        keyedTableListener.unsubscribe(cKey, mockListener);
    }

    public void testModify() {
        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(2L), with(KeyedTableListener.KeyEvent.MODIFIED));
            }
        });

        // Check the original value is still there
        Object[] vals = keyedTableListener.getRow(cKey);
        assertEquals(3.0, vals[2]);

        keyedTableListener.subscribe(cKey, mockListener);
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newModified = TstUtils.i(2);
            TstUtils.addToTable(table, newModified, TstUtils.c("Key1", "C"), TstUtils.c("Key2", 3),
                TstUtils.c("Data", 6.0));
            table.notifyListeners(noAdded.clone(), noRemoved.clone(), newModified);
        });

        // Check the value has changed
        vals = keyedTableListener.getRow(cKey);
        assertEquals(6.0, vals[2]);

        keyedTableListener.unsubscribe(cKey, mockListener);
    }

    public void testModifyChangedKey() {
        final SmartKey newKey = new SmartKey("C", 4);

        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(2L), with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey),
                    with(2L), with(KeyedTableListener.KeyEvent.ADDED));
            }
        });

        keyedTableListener.subscribe(cKey, mockListener);
        keyedTableListener.subscribe(newKey, mockListener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newModified = TstUtils.i(2);
            // Add to table on an existing index is a modify
            TstUtils.addToTable(table, newModified, TstUtils.c("Key1", "C"), TstUtils.c("Key2", 4),
                TstUtils.c("Data", 6.0));
            table.notifyListeners(noAdded.clone(), noRemoved.clone(), newModified);
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
        final SmartKey newKey = new SmartKey("D", 4);

        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(bKey),
                    with(1L), with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(2L), with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(1L), with(KeyedTableListener.KeyEvent.ADDED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey),
                    with(2L), with(KeyedTableListener.KeyEvent.ADDED));
            }
        });

        keyedTableListener.subscribe(bKey, mockListener);
        keyedTableListener.subscribe(cKey, mockListener);
        keyedTableListener.subscribe(newKey, mockListener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newModified = TstUtils.i(1, 2);
            // Add to table on an existing index is a modify
            TstUtils.addToTable(table, newModified, TstUtils.c("Key1", "C", "D"),
                TstUtils.c("Key2", 3, 4), TstUtils.c("Data", 3.0, 4.0));
            table.notifyListeners(noAdded.clone(), noRemoved.clone(), newModified);
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
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(bKey),
                    with(1L), with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(1L), with(KeyedTableListener.KeyEvent.ADDED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(2L), with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(bKey),
                    with(2L), with(KeyedTableListener.KeyEvent.ADDED));
            }
        });
        keyedTableListener.subscribe(bKey, mockListener);
        keyedTableListener.subscribe(cKey, mockListener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newModified = TstUtils.i(1, 2);
            TstUtils.addToTable(table, newModified, TstUtils.c("Key1", "C", "B"),
                TstUtils.c("Key2", 3, 2), TstUtils.c("Data", 3.0, 2.0));
            table.notifyListeners(noAdded.clone(), noRemoved.clone(), newModified);
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
        final SmartKey newKey = new SmartKey("D", 4);

        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(aKey),
                    with(0L), with(KeyedTableListener.KeyEvent.MODIFIED));
                never(mockListener).update(with(any(KeyedTableListener.class)), with(bKey),
                    with(1L), with(any(KeyedTableListener.KeyEvent.class)));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(2L), with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey),
                    with(4L), with(KeyedTableListener.KeyEvent.ADDED));
            }
        });

        keyedTableListener.subscribe(aKey, mockListener);
        keyedTableListener.subscribe(bKey, mockListener);
        keyedTableListener.subscribe(cKey, mockListener);
        keyedTableListener.subscribe(newKey, mockListener);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newRemoved = TstUtils.i(2);
            TstUtils.removeRows(table, newRemoved);

            final Index newModified = TstUtils.i(0);
            TstUtils.addToTable(table, newModified, TstUtils.c("Key1", "A"), TstUtils.c("Key2", 1),
                TstUtils.c("Data", 1.5));

            final Index newAdd = TstUtils.i(4);
            TstUtils.addToTable(table, newAdd, TstUtils.c("Key1", "D"), TstUtils.c("Key2", 4),
                TstUtils.c("Data", 4.0));

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
        final SmartKey newKey = new SmartKey("D", 4);

        checking(new Expectations() {
            {
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(cKey),
                    with(2L), with(KeyedTableListener.KeyEvent.REMOVED));
                oneOf(mockListener).update(with(any(KeyedTableListener.class)), with(newKey),
                    with(2L), with(KeyedTableListener.KeyEvent.ADDED));
            }
        });

        keyedTableListener.subscribe(cKey, mockListener);
        keyedTableListener.subscribe(newKey, mockListener);

        // Two cycles -- first remove
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newRemoved = TstUtils.i(2);
            TstUtils.removeRows(table, newRemoved);
            table.notifyListeners(noAdded, newRemoved, noModified);
        });

        // Now add
        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index newAdded = TstUtils.i(2);
            TstUtils.addToTable(table, newAdded, TstUtils.c("Key1", "D"), TstUtils.c("Key2", 4),
                TstUtils.c("Data", 4.0));
            table.notifyListeners(newAdded, noRemoved, noModified);
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
