/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

public class KeyedTableListener {

    public enum KeyEvent {
        ADDED, REMOVED, MODIFIED
    };

    public interface KeyUpdateListener {
        void update(KeyedTableListener keyedTableListener, SmartKey key, long index, KeyEvent event);
    }

    private final QueryTable table;
    private final TObjectLongHashMap<SmartKey> keyToIndexHashMap;
    private final TLongObjectHashMap<SmartKey> indexToKeyHashMap;
    private final HashMap<SmartKey, CopyOnWriteArrayList<KeyUpdateListener>> keyListenerHashMap;
    private final String[] keyColumnNames;
    private final String[] allColumnNames;
    private final Map<String, ColumnSource<?>> parentColumnSourceMap;
    private final InstrumentedListenerAdapter tableListener;

    private static final long NO_ENTRY = -1;

    // TODO: create an even more generic internals to handle multiple matches
    // TODO: Refactor with some sort of internal assistant object (unique versus generic)
    // TODO: private HashMap<SmartKey, Index> keyToIndexObjectHashMap; // for storing multiple matches

    public KeyedTableListener(QueryTable table, String... keyColumnNames) {
        this.table = table;
        final int tableSize = table.intSize("KeyedTableListener.initialize");
        this.keyToIndexHashMap = new TObjectLongHashMap<>(tableSize, 0.75f, NO_ENTRY);
        this.indexToKeyHashMap = new TLongObjectHashMap<>(tableSize, 0.75f, NO_ENTRY);
        this.keyListenerHashMap = new HashMap<>();
        this.keyColumnNames = keyColumnNames;
        this.tableListener = new InstrumentedListenerAdapter(null, table, false) {
            @Override
            public void onUpdate(final Index added, final Index removed, final Index modified) {
                handleUpdateFromTable(added, removed, modified);
            }
        };

        List<String> allColumnNames = table.getDefinition().getColumnNames();
        this.allColumnNames = allColumnNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        this.parentColumnSourceMap = table.getColumnSourceMap();
    }

    public void listenForUpdates() {
        this.table.listenForUpdates(tableListener, true);
    }

    public void close() {
        this.table.removeUpdateListener(tableListener);
    }

    private void handleUpdateFromTable(final Index added, final Index removed, final Index modified) {
        // Add all the new rows to the hashmap
        for (Index.Iterator iterator = added.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            SmartKey key = constructSmartKey(next);
            keyToIndexHashMap.put(key, next);
            indexToKeyHashMap.put(next, key);
            handleListeners(key, next, KeyEvent.ADDED);
        }

        // Remove all the removed rows from the hashmap
        for (Index.Iterator iterator = removed.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            SmartKey oldKey = indexToKeyHashMap.remove(next);
            Assert.assertion(oldKey != null, "oldKey != null");
            long oldIndex = keyToIndexHashMap.remove(oldKey);
            Assert.assertion(oldIndex != NO_ENTRY, "oldRow != NO_ENTRY");
            handleListeners(oldKey, next, KeyEvent.REMOVED);
        }

        // Modifies are a special case -- need to look for keys being removed / added
        for (Index.Iterator iterator = modified.iterator(); iterator.hasNext();) {
            long next = iterator.nextLong();
            SmartKey currentKey = constructSmartKey(next);
            SmartKey prevKey = indexToKeyHashMap.get(next);

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

    private SmartKey constructSmartKey(long index) {
        Object[] smartKeyVals = new Object[keyColumnNames.length];
        for (int i = 0; i < keyColumnNames.length; i++) {
            smartKeyVals[i] = parentColumnSourceMap.get(keyColumnNames[i]).get(index);
        }
        return new SmartKey(smartKeyVals);
    }

    private void handleListeners(SmartKey key, long index, KeyEvent event) {
        synchronized (keyListenerHashMap) {
            CopyOnWriteArrayList<KeyUpdateListener> listeners = keyListenerHashMap.get(key);
            if (listeners != null && listeners.size() > 0) {
                for (KeyUpdateListener listener : listeners) {
                    listener.update(this, key, index, event);
                }
            }
        }
    }

    public long getIndex(SmartKey key) {
        return keyToIndexHashMap.get(key);
    }

    // Make sure these are in the same order as the keys defined on construction
    public Object[] getRow(Object... keyValues) {
        return getRow(new SmartKey(keyValues));
    }

    public Object[] getRow(SmartKey key) {
        return getRow(key, allColumnNames);
    }

    public Object[] getRowAtIndex(long index) {
        return getRowAtIndex(index, allColumnNames);
    }

    public Object[] getRow(SmartKey key, String... columnNames) {
        long index = getIndex(key);
        return getRowAtIndex(index, columnNames);
    }

    public Object[] getRowAtIndex(long index, String... columnNames) {
        long tableRow = table.getIndex().find(index);
        return (index != -1) ? table.getRecord(tableRow, columnNames) : null;
    }

    public long getTableRow(SmartKey key) {
        long index = getIndex(key);
        return (index == -1) ? -1 : table.getIndex().find(index);
    }

    public void subscribe(SmartKey key, KeyUpdateListener listener) {
        subscribe(key, listener, false);
    }

    public void subscribe(SmartKey key, KeyUpdateListener listener, boolean replayInitialData) {
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

    public void unsubscribe(SmartKey key, KeyUpdateListener listener) {
        synchronized (keyListenerHashMap) {
            CopyOnWriteArrayList<KeyUpdateListener> callBackList = keyListenerHashMap.get(key);
            if (callBackList != null) {
                callBackList.remove(listener);
            }
        }
    }
}
