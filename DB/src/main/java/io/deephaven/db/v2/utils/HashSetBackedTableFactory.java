/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.Function;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.MutableColumnSourceGetDefaults;
import gnu.trove.iterator.TObjectLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An abstract table that represents a hashset of smart keys. Since we are representing a set, there
 * we are not defining an order to our output. Whatever order the table happens to end up in, is
 * fine.
 *
 * The table will refresh by regenerating the full hashset (using the setGenerator Function passed
 * in); and then comparing that to the existing hash set.
 */
public class HashSetBackedTableFactory {
    private final Function.Nullary<HashSet<SmartKey>> setGenerator;
    private final int refreshIntervalMs;
    private long nextRefresh;
    private final Map<String, ColumnSource> columns;
    private final TObjectLongMap<SmartKey> valueToIndexMap = new TObjectLongHashMap<>();
    private final TLongObjectMap<SmartKey> indexToValueMap = new TLongObjectHashMap<>();

    private final TLongObjectMap<SmartKey> indexToPreviousMap = new TLongObjectHashMap<>();
    private final TLongLongMap indexToPreviousClock = new TLongLongHashMap();
    private long lastIndex = 0;
    private final TLongArrayList freeSet = new TLongArrayList();
    Index index;

    private HashSetBackedTableFactory(Function.Nullary<HashSet<SmartKey>> setGenerator,
        int refreshIntervalMs, String... colNames) {
        this.setGenerator = setGenerator;
        this.refreshIntervalMs = refreshIntervalMs;
        nextRefresh = System.currentTimeMillis() + this.refreshIntervalMs;

        columns = new LinkedHashMap<>();

        for (int ii = 0; ii < colNames.length; ++ii) {
            columns.put(colNames[ii], new SmartKeyWrapperColumnSource(ii));
        }
    }

    /**
     * Create a ticking table based on a setGenerator.
     *
     * @param setGenerator a function that returns a HashSet of SmartKeys, each SmartKey is a row in
     *        the output.
     * @param refreshIntervalMs how often to refresh the table, if less than or equal to 0 the table
     *        does not tick.
     * @param colNames the column names for the output table, must match the number of elements in
     *        each SmartKey.
     * @return a table representing the Set returned by the setGenerator
     */
    public static Table create(Function.Nullary<HashSet<SmartKey>> setGenerator,
        int refreshIntervalMs, String... colNames) {
        HashSetBackedTableFactory factory =
            new HashSetBackedTableFactory(setGenerator, refreshIntervalMs, colNames);

        IndexBuilder addedBuilder = Index.FACTORY.getRandomBuilder();
        IndexBuilder removedBuilder = Index.FACTORY.getRandomBuilder();

        factory.updateValueSet(addedBuilder, removedBuilder);

        Index added = addedBuilder.getIndex();
        Index removed = removedBuilder.getIndex();

        factory.index = added;
        Assert.assertion(removed.size() == 0, "removed.size() == 0");

        return factory.getTable();
    }

    private Table getTable() {
        return new HashSetBackedTable(index, columns);
    }

    private void updateValueSet(IndexBuilder addedBuilder, IndexBuilder removedBuilder) {
        HashSet<SmartKey> valueSet = setGenerator.call();

        synchronized (this) {
            for (TObjectLongIterator<SmartKey> it = valueToIndexMap.iterator(); it.hasNext();) {
                it.advance();
                SmartKey key = it.key();
                if (!valueSet.contains(key)) {
                    removeValue(it, removedBuilder);
                }
            }

            for (SmartKey value : valueSet) {
                if (!valueToIndexMap.containsKey(value)) {
                    addValue(value, addedBuilder);
                }
            }
        }
    }

    private void removeValue(TObjectLongIterator<SmartKey> vtiIt, IndexBuilder removedBuilder) {
        long index = vtiIt.value();

        // record the old value for get prev
        indexToPreviousMap.put(index, vtiIt.key());
        vtiIt.remove();

        indexToPreviousClock.put(index, LogicalClock.DEFAULT.currentStep());

        indexToValueMap.remove(index);
        removedBuilder.addKey(index);
        freeSet.add(index);
    }

    private void addValue(SmartKey value, IndexBuilder addedBuilder) {
        long newIndex;
        if (freeSet.isEmpty()) {
            newIndex = lastIndex++;
        } else {
            newIndex = freeSet.get(freeSet.size() - 1);
            freeSet.remove(freeSet.size() - 1, 1);
        }
        addedBuilder.addKey(newIndex);
        valueToIndexMap.put(value, newIndex);
        indexToValueMap.put(newIndex, value);

        if (indexToPreviousClock.get(newIndex) != LogicalClock.DEFAULT.currentStep()) {
            indexToPreviousClock.put(newIndex, LogicalClock.DEFAULT.currentStep());
            indexToPreviousMap.put(newIndex, null);
        }
    }

    private class HashSetBackedTable extends QueryTable implements LiveTable {
        HashSetBackedTable(Index index, Map<String, ColumnSource> columns) {
            super(index, columns);
            if (refreshIntervalMs >= 0) {
                setRefreshing(true);
                LiveTableMonitor.DEFAULT.addTable(this);
            }
        }

        @Override
        public void refresh() {
            if (System.currentTimeMillis() < nextRefresh) {
                return;
            }
            nextRefresh = System.currentTimeMillis() + refreshIntervalMs;

            IndexBuilder addedBuilder = Index.FACTORY.getRandomBuilder();
            IndexBuilder removedBuilder = Index.FACTORY.getRandomBuilder();

            updateValueSet(addedBuilder, removedBuilder);

            final Index added = addedBuilder.getIndex();
            final Index removed = removedBuilder.getIndex();

            if (added.size() > 0 || removed.size() > 0) {
                final Index modified = added.intersect(removed);
                added.remove(modified);
                removed.remove(modified);

                index.update(added, removed);
                notifyListeners(added, removed, modified);
            }
        }

        @Override
        public void destroy() {
            super.destroy();
            if (refreshIntervalMs >= 0) {
                LiveTableMonitor.DEFAULT.removeTable(this);
            }
        }
    }

    private class SmartKeyWrapperColumnSource extends AbstractColumnSource<String>
        implements MutableColumnSourceGetDefaults.ForObject<String> {

        private final int columnIndex;

        public SmartKeyWrapperColumnSource(int columnIndex) {
            super(String.class, null);
            this.columnIndex = columnIndex;
        }

        @Override
        public String get(long index) {
            synchronized (HashSetBackedTableFactory.this) {
                SmartKey row = indexToValueMap.get(index);
                if (row == null)
                    return null;
                return (String) row.values_[columnIndex];
            }
        }

        @Override
        public String getPrev(long index) {
            synchronized (HashSetBackedTableFactory.this) {
                if (indexToPreviousClock.get(index) == LogicalClock.DEFAULT.currentStep()) {
                    SmartKey row = indexToPreviousMap.get(index);
                    if (row == null)
                        return null;
                    return (String) row.values_[columnIndex];
                } else {
                    return get(index);
                }
            }
        }

        @Override
        public void startTrackingPrevValues() {
            // Do nothing.
        }

        @Override
        public boolean isImmutable() {
            return false;
        }
    }
}
