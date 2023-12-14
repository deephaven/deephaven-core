/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.dataindex.BaseDataIndex;
import io.deephaven.engine.table.impl.dataindex.DataIndexUtils;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * This class listens to a table and on each update verifies that the indexes returned by the table's RowSet for a set
 * of columns are still valid. It is meant to be used as part of a unit test for incremental updates, to ensure that
 * stale indexes are not left between table updates.
 */
public class IndexValidator extends InstrumentedTableUpdateListenerAdapter {
    private final Table source;
    private final Collection<String[]> indexColumns;
    private final String context;
    private int validationCount = 0;

    public IndexValidator(String context, Table source, ArrayList<ArrayList<String>> indexColumns) {
        this(context, source, convertListToArray(indexColumns));
    }

    static private Collection<String[]> convertListToArray(ArrayList<ArrayList<String>> indexColumns) {
        Collection<String[]> collectionOfArrays = new ArrayList<>();
        for (ArrayList<String> columnSet : indexColumns) {
            collectionOfArrays.add(columnSet.toArray(new String[columnSet.size()]));
        }
        return collectionOfArrays;
    }

    private IndexValidator(String context, Table source, Collection<String[]> indexColumns) {
        super("index validator " + context, source, false);
        this.context = context;
        this.source = source;
        this.indexColumns = indexColumns;

        source.addUpdateListener(this);
    }

    private void validateIndexes(Collection<String[]> indexColumns, RowSet rowSet, boolean usePrev) {
        for (String[] indexToCheck : indexColumns) {
            validateIndex(indexToCheck, rowSet, source, context, usePrev);
        }
    }

    // private void validatePrevIndexes(Collection<String[]> indexColumns, TrackingRowSet rowSet) {
    // for (String[] indexToCheck : indexColumns) {
    // validatePrevIndex(indexToCheck, rowSet);
    // }
    // }

    public static void validateIndex(String[] indexToCheck, RowSet rowSet, Table source, String context,
            boolean usePrev) {
        final ColumnSource[] groupColumns = getColumnSources(indexToCheck, source);
        if (!rowSet.isTracking() || !DataIndexer.of(rowSet.trackingCast()).hasDataIndex(groupColumns)) {
            return;
        }

        final DataIndexer dataIndexer = DataIndexer.of(rowSet.trackingCast());
        final BaseDataIndex dataIndex = (BaseDataIndex) dataIndexer.getDataIndex(groupColumns);


        final Table indexTable = dataIndex.table();

        // Create column iterators for the keys and the row set
        final CloseableIterator<?>[] keyIterators =
                Arrays.stream(indexToCheck).map(indexTable::columnIterator).toArray(CloseableIterator[]::new);
        final CloseableIterator<RowSet> rsIt = indexTable.columnIterator(dataIndex.rowSetColumnName());

        // Verify that the keys are correct in the table vs. the index.
        while (rsIt.hasNext()) {
            final RowSet rs = rsIt.next();
            final Object[] keyValues = Arrays.stream(keyIterators).map(CloseableIterator::next).toArray();
            final Object keys = getFromValues(groupColumns, keyValues);

            final RowSet.Iterator it = rs.iterator();
            while (it.hasNext()) {
                final long next = it.nextLong();
                if (indexToCheck.length == 1) {
                    if (usePrev) {
                        checkGroupPrevKey(groupColumns, next, keyValues[0], context);
                    } else {
                        checkGroupKey(groupColumns, next, keyValues[0], context);
                    }
                } else {
                    if (usePrev) {
                        checkGroupPrevKey(groupColumns, next, keys, context);
                    } else {
                        checkGroupKey(groupColumns, next, keys, context);
                    }
                }
            }
        }

        // Clean up the iterators
        SafeCloseableArray.close(keyIterators);
        SafeCloseable.closeAll(rsIt);

        // Verify that every key in the row set is in the index at the correct position.
        final DataIndex.RowKeyLookup rowKeyLookup = dataIndex.rowKeyLookup();
        final ColumnSource<RowSet> rowSetColumn = dataIndex.rowSetColumn();

        for (RowSet.Iterator it = rowSet.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object[] key = Arrays.stream(groupColumns).map(cs -> cs.get(next)).toArray();
            final long rowKey;
            if (key.length == 1) {
                rowKey = rowKeyLookup.apply(key[0], usePrev);
            } else {
                rowKey = rowKeyLookup.apply(key, usePrev);
            }
            final RowSet keyRowSet = rowSetColumn.get(rowKey);
            Assert.assertion(keyRowSet != null, "keyRowSet != null", next, "next", key, "key", context, "context");
            if (keyRowSet != null) {
                Assert.assertion(keyRowSet.find(next) >= 0, "keyRowSet.find(next) >= 0", next, "next", key, "key",
                        keyRowSet, "keyRowSet", context, "context");
            }
        }
    }

    private static ColumnSource[] getColumnSources(String[] indexToCheck, Table source) {
        return Arrays.stream(indexToCheck).map(source::getColumnSource).toArray(ColumnSource[]::new);
    }

    static private void checkGroupKey(final ColumnSource[] groupColumns, final long next, final Object key,
            final String context) {
        final Object value = getValue(groupColumns, next);
        if (key instanceof Object[]) {
            Assert.assertion(Arrays.equals((Object[]) value, (Object[]) key), "Arrays.equals(value, key)", value,
                    "value", key, "key", context, "context");
            return;
        }
        Assert.assertion(Objects.equals(value, key), "value.equals(key)", value, "value", key, "key", context,
                "context");
    }

    static private void checkGroupPrevKey(final ColumnSource[] groupColumns, final long next, final Object key,
            final String context) {
        Object value = getPrevValue(groupColumns, next);
        if (key instanceof Object[]) {
            Assert.assertion(Arrays.equals((Object[]) value, (Object[]) key), "Arrays.equals(value, key)", value,
                    "value", key, "key", context, "context");
            return;
        }
        Assert.assertion(value == key || value.equals(key), "value.equals(key)", value, "value", key, "key", context,
                "context");
    }

    static private Object getValue(ColumnSource[] groupColumns, long next) {
        // pretty inefficient, since this is a chunk source
        final ChunkSource.WithPrev<Values> source = DataIndexUtils.makeBoxedKeySource(groupColumns);
        try (final ChunkSource.GetContext ctx = source.makeGetContext(1)) {
            return source.getChunk(ctx, next, next).asObjectChunk().get(0);
        }
    }

    static private Object getPrevValue(ColumnSource[] groupColumns, long next) {
        // pretty inefficient, since this is a chunk source
        final ChunkSource.WithPrev<Values> source = DataIndexUtils.makeBoxedKeySource(groupColumns);
        try (final ChunkSource.GetContext ctx = source.makeGetContext(1)) {
            return source.getPrevChunk(ctx, next, next).asObjectChunk().get(0);
        }
    }

    static private Object getFromValues(ColumnSource[] groupColumns, Object... values) {
        if (values.length == 1) {
            return values[0];
        }
        return values;
    }

    public void validateIndexes() {
        validateIndexes(indexColumns, source.getRowSet(), false);
    }

    public void validatePrevIndexes() {
        validateIndexes(indexColumns, source.getRowSet(), true);
    }


    @Override
    public void onUpdate(final TableUpdate upstream) {
        // validateIndexes();
        // NB: This would normally be inappropriate: we don't expect index support on the non-tracking row sets we
        // use for updates. Forcing support by cloning and making the result tracking.
        validationCount++;
        System.out.println("Validation Count for " + context + ": " + validationCount);
    }

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        originalException.printStackTrace();
        TestCase.fail("Failure for context " + context + ": " + originalException.getMessage());
    }
}
