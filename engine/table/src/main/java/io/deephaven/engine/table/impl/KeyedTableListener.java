//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.tuple.ArrayTuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

public class KeyedTableListener {

    public enum KeyEvent {
        ADDED, REMOVED, MODIFIED
    }

    public interface KeyUpdateListener {
        void update(KeyedTableListener keyedTableListener, ArrayTuple key, long index, KeyEvent event);
    }

    private final QueryTable table;
    private final TObjectLongHashMap<ArrayTuple> keyToRowKeyHashMap;
    private final TLongObjectHashMap<ArrayTuple> rowKeyToKeyHashMap;
    private final HashMap<ArrayTuple, CopyOnWriteArrayList<KeyUpdateListener>> keyListenerHashMap;
    private final String[] keyColumnNames;
    private final String[] allColumnNames;
    private final Map<String, ColumnSource<?>> parentColumnSourceMap;
    private final ShiftObliviousInstrumentedListenerAdapter tableListener;

    // TODO: create an even more generic internals to handle multiple matches
    // TODO: Refactor with some sort of internal assistant object (unique versus generic)
    // TODO: private HashMap<ArrayTuple, TrackingWritableRowSet> keyToRowKeyObjectHashMap; // for storing multiple
    // matches

    public KeyedTableListener(QueryTable table, String... keyColumnNames) {
        this.table = table;
        final int tableSize = table.intSize("KeyedTableListener.initialize");
        this.keyToRowKeyHashMap = new TObjectLongHashMap<>(tableSize, 0.75f, NULL_ROW_KEY);
        this.rowKeyToKeyHashMap = new TLongObjectHashMap<>(tableSize, 0.75f, NULL_ROW_KEY);
        this.keyListenerHashMap = new HashMap<>();
        this.keyColumnNames = keyColumnNames;
        this.tableListener = new ShiftObliviousInstrumentedListenerAdapter(null, table, false) {
            @Override
            public void onUpdate(final RowSet added, final RowSet removed, final RowSet modified) {
                handleUpdateFromTable(added, removed, modified);
            }
        };

        List<String> allColumnNames = table.getDefinition().getColumnNames();
        this.allColumnNames = allColumnNames.toArray(String[]::new);
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
            keyToRowKeyHashMap.put(key, next);
            rowKeyToKeyHashMap.put(next, key);
            handleListeners(key, next, KeyEvent.ADDED);
        }

        // Remove all the removed rows from the hashmap
        for (RowSet.Iterator iterator = removed.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            ArrayTuple oldKey = rowKeyToKeyHashMap.remove(next);
            Assert.assertion(oldKey != null, "oldKey != null");
            long oldRowKey = keyToRowKeyHashMap.remove(oldKey);
            Assert.assertion(oldRowKey != NULL_ROW_KEY, "oldRow != NULL_ROW_KEY");
            handleListeners(oldKey, next, KeyEvent.REMOVED);
        }

        // Modifies are a special case -- need to look for keys being removed / added
        for (RowSet.Iterator iterator = modified.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            ArrayTuple currentKey = constructArrayTuple(next);
            ArrayTuple prevKey = rowKeyToKeyHashMap.get(next);

            // Check if the key values have changed
            if (!currentKey.equals(prevKey)) {
                // only want to remove the old key if it was pointing to this rowKey
                if (keyToRowKeyHashMap.get(prevKey) == next) {
                    keyToRowKeyHashMap.remove(prevKey);
                    rowKeyToKeyHashMap.remove(next);
                    handleListeners(prevKey, next, KeyEvent.REMOVED);
                }

                // Check if this current key was used elsewhere and remove the rowKey->key mapping
                long otherRowKey = keyToRowKeyHashMap.get(currentKey);
                if (otherRowKey != NULL_ROW_KEY) {
                    rowKeyToKeyHashMap.remove(otherRowKey);
                    handleListeners(currentKey, otherRowKey, KeyEvent.REMOVED);
                }

                // Update the maps and notify the newly modified key
                keyToRowKeyHashMap.put(currentKey, next);
                rowKeyToKeyHashMap.put(next, currentKey);
                handleListeners(currentKey, next, KeyEvent.ADDED);
            } else {
                handleListeners(currentKey, next, KeyEvent.MODIFIED);
            }
        }
    }

    private ArrayTuple constructArrayTuple(long rowKey) {
        Object[] ArrayTupleVals = new Object[keyColumnNames.length];
        for (int i = 0; i < keyColumnNames.length; i++) {
            ArrayTupleVals[i] = parentColumnSourceMap.get(keyColumnNames[i]).get(rowKey);
        }
        return new ArrayTuple(ArrayTupleVals);
    }

    private void handleListeners(ArrayTuple key, long rowKey, KeyEvent event) {
        synchronized (keyListenerHashMap) {
            CopyOnWriteArrayList<KeyUpdateListener> listeners = keyListenerHashMap.get(key);
            if (listeners != null && !listeners.isEmpty()) {
                for (KeyUpdateListener listener : listeners) {
                    listener.update(this, key, rowKey, event);
                }
            }
        }
    }

    public long getRowKey(ArrayTuple key) {
        return keyToRowKeyHashMap.get(key);
    }

    // Make sure these are in the same order as the keys defined on construction
    public Object[] getRow(Object... keyValues) {
        return getRow(new ArrayTuple(keyValues));
    }

    public Object[] getRow(ArrayTuple key) {
        return getRow(key, allColumnNames);
    }

    public Object[] getRowAtRowKey(long rowKey) {
        return getRowAtRowKey(rowKey, allColumnNames);
    }

    public Object[] getRow(ArrayTuple key, String... columnNames) {
        long rowKey = getRowKey(key);
        return getRowAtRowKey(rowKey, columnNames);
    }

    public Object[] getRowAtRowKey(long rowKey, String... columnNames) {
        // @formatter:off
        return rowKey == NULL_ROW_KEY
                ? null
                : (columnNames.length > 0
                        ? Arrays.stream(columnNames).map(table::getColumnSource)
                        : table.getColumnSources().stream()
                  ).map(columnSource -> columnSource.get(rowKey)).toArray(Object[]::new);
        // @formatter:on
    }

    public long getRowPosition(ArrayTuple key) {
        final long rowKey = getRowKey(key);
        return rowKey == NULL_ROW_KEY ? NULL_ROW_KEY : table.getRowSet().find(rowKey);
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
                long rowKey = keyToRowKeyHashMap.get(key);
                if (rowKey != keyToRowKeyHashMap.getNoEntryValue()) {
                    listener.update(this, key, rowKey, KeyEvent.ADDED);
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
