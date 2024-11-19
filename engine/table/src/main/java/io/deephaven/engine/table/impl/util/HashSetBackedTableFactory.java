//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import gnu.trove.iterator.TObjectLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongLongMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.tuple.ArrayTuple;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * An abstract table that represents a hash set of array-backed tuples. Since we are representing a set, there we are
 * not defining an order to our output. Whatever order the table happens to end up in, is fine.
 *
 * The table will run by regenerating the full hash set (using the setGenerator Function passed in); and then comparing
 * that to the existing hash set.
 */
public class HashSetBackedTableFactory {

    private final Supplier<HashSet<ArrayTuple>> setGenerator;
    private final int refreshIntervalMs;
    private long nextRefresh;
    private final Map<String, ColumnSource<?>> columns;

    private final UpdateGraph updateGraph;

    private final TObjectLongMap<ArrayTuple> valueToIndexMap = new TObjectLongHashMap<>();
    private final TLongObjectMap<ArrayTuple> indexToValueMap = new TLongObjectHashMap<>();

    private final TLongObjectMap<ArrayTuple> indexToPreviousMap = new TLongObjectHashMap<>();
    private final TLongLongMap indexToPreviousClock = new TLongLongHashMap();
    private long lastIndex = 0;
    private final TLongArrayList freeSet = new TLongArrayList();
    private TrackingWritableRowSet rowSet;

    private HashSetBackedTableFactory(Supplier<HashSet<ArrayTuple>> setGenerator, int refreshIntervalMs,
            String... colNames) {
        this.setGenerator = setGenerator;
        this.refreshIntervalMs = refreshIntervalMs;
        nextRefresh = System.currentTimeMillis() + this.refreshIntervalMs;

        columns = new LinkedHashMap<>();

        for (int ii = 0; ii < colNames.length; ++ii) {
            columns.put(colNames[ii], new ArrayTupleWrapperColumnSource(ii));
        }

        updateGraph = ExecutionContext.getContext().getUpdateGraph();
    }

    /**
     * Create a ticking table based on a setGenerator.
     *
     * @param setGenerator a function that returns a HashSet of ArrayTuples, each ArrayTuple is a row in the output.
     * @param refreshIntervalMs how often to run the table, if less than or equal to 0 the table does not tick.
     * @param colNames the column names for the output table, must match the number of elements in each ArrayTuple.
     * @return a table representing the Set returned by the setGenerator
     */
    public static Table create(Supplier<HashSet<ArrayTuple>> setGenerator, int refreshIntervalMs,
            String... colNames) {
        HashSetBackedTableFactory factory = new HashSetBackedTableFactory(setGenerator, refreshIntervalMs, colNames);

        RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
        RowSetBuilderRandom removedBuilder = RowSetFactory.builderRandom();

        factory.updateValueSet(addedBuilder, removedBuilder);

        WritableRowSet added = addedBuilder.build();
        RowSet removed = removedBuilder.build();

        factory.rowSet = added.toTracking();
        Assert.assertion(removed.size() == 0, "removed.size() == 0");

        return factory.getTable();
    }

    private Table getTable() {
        return new HashSetBackedTable(rowSet, columns);
    }

    private void updateValueSet(RowSetBuilderRandom addedBuilder, RowSetBuilderRandom removedBuilder) {
        HashSet<ArrayTuple> valueSet = setGenerator.get();

        synchronized (this) {
            for (TObjectLongIterator<ArrayTuple> it = valueToIndexMap.iterator(); it.hasNext();) {
                it.advance();
                ArrayTuple key = it.key();
                if (!valueSet.contains(key)) {
                    removeValue(it, removedBuilder);
                }
            }

            for (ArrayTuple value : valueSet) {
                if (!valueToIndexMap.containsKey(value)) {
                    addValue(value, addedBuilder);
                }
            }
        }
    }

    private void removeValue(TObjectLongIterator<ArrayTuple> vtiIt, RowSetBuilderRandom removedBuilder) {
        long index = vtiIt.value();

        // record the old value for get prev
        indexToPreviousMap.put(index, vtiIt.key());
        vtiIt.remove();

        indexToPreviousClock.put(index, updateGraph.clock().currentStep());

        indexToValueMap.remove(index);
        removedBuilder.addKey(index);
        freeSet.add(index);
    }

    private void addValue(ArrayTuple value, RowSetBuilderRandom addedBuilder) {
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

        if (indexToPreviousClock.get(newIndex) != updateGraph.clock().currentStep()) {
            indexToPreviousClock.put(newIndex, updateGraph.clock().currentStep());
            indexToPreviousMap.put(newIndex, null);
        }
    }

    /**
     * @implNote The constructor publishes {@code this} to the {@link UpdateGraph} and cannot be subclassed.
     */
    private final class HashSetBackedTable extends QueryTable implements Runnable {
        HashSetBackedTable(TrackingRowSet rowSet, Map<String, ColumnSource<?>> columns) {
            super(rowSet, columns);
            if (refreshIntervalMs >= 0) {
                setRefreshing(true);
                updateGraph.addSource(this);
            }
        }

        @Override
        public void run() {
            if (System.currentTimeMillis() < nextRefresh) {
                return;
            }
            nextRefresh = System.currentTimeMillis() + refreshIntervalMs;

            RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
            RowSetBuilderRandom removedBuilder = RowSetFactory.builderRandom();

            updateValueSet(addedBuilder, removedBuilder);

            final WritableRowSet added = addedBuilder.build();
            final WritableRowSet removed = removedBuilder.build();

            if (added.size() > 0 || removed.size() > 0) {
                final RowSet modified = added.intersect(removed);
                added.remove(modified);
                removed.remove(modified);

                rowSet.update(added, removed);
                notifyListeners(added, removed, modified);
            }
        }

        @OverridingMethodsMustInvokeSuper
        @Override
        public void destroy() {
            super.destroy();
            if (refreshIntervalMs >= 0) {
                updateGraph.removeSource(this);
            }
        }
    }

    private class ArrayTupleWrapperColumnSource extends AbstractColumnSource<String>
            implements MutableColumnSourceGetDefaults.ForObject<String> {

        private final int columnIndex;

        public ArrayTupleWrapperColumnSource(int columnIndex) {
            super(String.class, null);
            this.columnIndex = columnIndex;
        }

        @Override
        public String get(long rowKey) {
            synchronized (HashSetBackedTableFactory.this) {
                ArrayTuple row = indexToValueMap.get(rowKey);
                if (row == null)
                    return null;
                return row.getElement(columnIndex);
            }
        }

        @Override
        public String getPrev(long rowKey) {
            synchronized (HashSetBackedTableFactory.this) {
                if (indexToPreviousClock.get(rowKey) == updateGraph.clock().currentStep()) {
                    ArrayTuple row = indexToPreviousMap.get(rowKey);
                    if (row == null)
                        return null;
                    return row.getElement(columnIndex);
                } else {
                    return get(rowKey);
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
