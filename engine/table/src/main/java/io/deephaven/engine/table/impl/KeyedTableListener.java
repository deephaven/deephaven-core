//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.tuple.ArrayTuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class KeyedTableListener {

    public enum KeyEvent {
        ADDED, REMOVED, MODIFIED
    }

    public interface KeyUpdateListener {
        void update(KeyedTableListener keyedTableListener, ArrayTuple key, long index, KeyEvent event);
    }

    private final QueryTable table;
    private final TObjectLongHashMap<ArrayTuple> keyToIndexHashMap;
    private final TLongObjectHashMap<ArrayTuple> indexToKeyHashMap;
    private final HashMap<ArrayTuple, CopyOnWriteArrayList<KeyUpdateListener>> keyListenerHashMap;
    private final String[] keyColumnNames;
    private final String[] allColumnNames;
    private final Map<String, ColumnSource<?>> parentColumnSourceMap;
    private final ShiftObliviousInstrumentedListenerAdapter tableListener;

    private static final long NO_ENTRY = -1;

    // TODO: create an even more generic internals to handle multiple matches
    // TODO: Refactor with some sort of internal assistant object (unique versus generic)
    // TODO: private HashMap<ArrayTuple, TrackingWritableRowSet> keyToIndexObjectHashMap; // for storing multiple
    // matches

    public KeyedTableListener(QueryTable table, String... keyColumnNames) {
        this.table = table;
        final int tableSize = table.intSize("KeyedTableListener.initialize");
        this.keyToIndexHashMap = new TObjectLongHashMap<>(tableSize, 0.75f, NO_ENTRY);
        this.indexToKeyHashMap = new TLongObjectHashMap<>(tableSize, 0.75f, NO_ENTRY);
        this.keyListenerHashMap = new HashMap<>();
        this.keyColumnNames = keyColumnNames;
        this.tableListener = new ShiftObliviousInstrumentedListenerAdapter(null, table, false) {
            @Override
            public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
                handleUpdateFromTable(added, removed, modified);
            }
        };

        List<String> allColumnNames = table.getDefinition().getColumnNames();
        this.allColumnNames = allColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        this.parentColumnSourceMap = table.getColumnSourceMap();
    }

    public void addUpdateListener() {
        this.table.addUpdateListener(tableListener, true);
    }

    public void close() {
        this.table.removeUpdateListener(tableListener);
    }

    private void handleUpdateFromTable(final RowSet added, final RowSet removed, final RowSet modified) {
        // Add all the new rows to the hashmap
        for (RowSet.Iterator iterator = added.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            ArrayTuple key = constructArrayTuple(next);
            keyToIndexHashMap.put(key, next);
            indexToKeyHashMap.put(next, key);
            handleListeners(key, next, KeyEvent.ADDED);
        }

        // Remove all the removed rows from the hashmap
        for (RowSet.Iterator iterator = removed.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            ArrayTuple oldKey = indexToKeyHashMap.remove(next);
            Assert.assertion(oldKey != null, "oldKey != null");
            long oldIndex = keyToIndexHashMap.remove(oldKey);
            Assert.assertion(oldIndex != NO_ENTRY, "oldRow != NO_ENTRY");
            handleListeners(oldKey, next, KeyEvent.REMOVED);
        }

        // Modifies are a special case -- need to look for keys being removed / added
        for (RowSet.Iterator iterator = modified.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            ArrayTuple currentKey = constructArrayTuple(next);
            ArrayTuple prevKey = indexToKeyHashMap.get(next);

            // Check if the key values have changed
            if (!currentKey.equals(prevKey)) {
                // only want to remove the old key if it was pointing to this index
                if (keyToIndexHashMap.get(prevKey) == next) {
                    keyToIndexHashMap.remove(prevKey);
                    indexToKeyHashMap.remove(next);
                    handleListeners(prevKey, next, KeyEvent.REMOVED);
                }

                // Check if this current key was used elsewhere and remove the index->key mapping
                long otherIndex = keyToIndexHashMap.get(currentKey);
                if (otherIndex != NO_ENTRY) {
                    indexToKeyHashMap.remove(otherIndex);
                    handleListeners(currentKey, otherIndex, KeyEvent.REMOVED);
                }

                // Update the maps and notify the newly modified key
                keyToIndexHashMap.put(currentKey, next);
                indexToKeyHashMap.put(next, currentKey);
                handleListeners(currentKey, next, KeyEvent.ADDED);
            } else {
                handleListeners(currentKey, next, KeyEvent.MODIFIED);
            }
        }
    }

    private ArrayTuple constructArrayTuple(long index) {
        Object[] ArrayTupleVals = new Object[keyColumnNames.length];
        for (int i = 0; i < keyColumnNames.length; i++) {
            ArrayTupleVals[i] = parentColumnSourceMap.get(keyColumnNames[i]).get(index);
        }
        return new ArrayTuple(ArrayTupleVals);
    }

    private void handleListeners(ArrayTuple key, long index, KeyEvent event) {
        synchronized (keyListenerHashMap) {
            CopyOnWriteArrayList<KeyUpdateListener> listeners = keyListenerHashMap.get(key);
            if (listeners != null && listeners.size() > 0) {
                for (KeyUpdateListener listener : listeners) {
                    listener.update(this, key, index, event);
                }
            }
        }
    }

    public long getIndex(ArrayTuple key) {
        return keyToIndexHashMap.get(key);
    }

    // Make sure these are in the same order as the keys defined on construction
    public Object[] getRow(Object... keyValues) {
        return getRow(new ArrayTuple(keyValues));
    }

    public Object[] getRow(ArrayTuple key) {
        return getRow(key, allColumnNames);
    }

    public Object[] getRowAtIndex(long index) {
        return getRowAtIndex(index, allColumnNames);
    }

    public Object[] getRow(ArrayTuple key, String... columnNames) {
        long index = getIndex(key);
        return getRowAtIndex(index, columnNames);
    }

    public Object[] getRowAtIndex(long index, String... columnNames) {
        long tableRow = table.getRowSet().find(index);
        return (index != -1) ? DataAccessHelpers.getRecord(table, tableRow, columnNames) : null;
    }

    public long getTableRow(ArrayTuple key) {
        long index = getIndex(key);
        return (index == -1) ? -1 : table.getRowSet().find(index);
    }

    public void subscribe(ArrayTuple key, KeyUpdateListener listener) {
        subscribe(key, listener, false);
    }

    public void subscribe(ArrayTuple key, KeyUpdateListener listener, boolean replayInitialData) {
        synchronized (keyListenerHashMap) {
            if (!keyListenerHashMap.containsKey(key)) {
                keyListenerHashMap.put(key, new CopyOnWriteArrayList<>());
            }
            CopyOnWriteArrayList<KeyUpdateListener> callBackList = keyListenerHashMap.get(key);
            callBackList.add(listener);

            if (replayInitialData) {
                long index = keyToIndexHashMap.get(key);
                if (index != keyToIndexHashMap.getNoEntryValue()) {
                    listener.update(this, key, index, KeyEvent.ADDED);
                }
            }
        }
    }

    public void unsubscribe(ArrayTuple key, KeyUpdateListener listener) {
        synchronized (keyListenerHashMap) {
            CopyOnWriteArrayList<KeyUpdateListener> callBackList = keyListenerHashMap.get(key);
            if (callBackList != null) {
                callBackList.remove(listener);
            }
        }
    }
}
