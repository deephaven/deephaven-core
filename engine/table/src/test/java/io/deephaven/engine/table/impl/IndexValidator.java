/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import junit.framework.TestCase;

import java.util.*;

/**
 * This class listens to a table and on each update verifies that the indexs returned by the table's RowSet for a set of
 * columns are still valid. It is meant to be used as part of a unit test for incremental updates, to ensure that stale
 * indexs are not left between table updates.
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

        validateIndexes(indexColumns, source.getRowSet());
        validatePrevIndexes(indexColumns, source.getRowSet());

        source.addUpdateListener(this);
    }

    private void validateIndexes(Collection<String[]> indexColumns, RowSet rowSet) {
        for (String[] indexToCheck : indexColumns) {
            validateIndex(indexToCheck, rowSet, source, context);
        }
    }

    private void validatePrevIndexes(Collection<String[]> indexColumns, TrackingRowSet rowSet) {
        for (String[] indexToCheck : indexColumns) {
            validatePrevIndex(indexToCheck, rowSet);
        }
    }

    public static void validateIndex(String[] indexToCheck, RowSet rowSet, Table source, String context) {
        final ColumnSource[] groupColumns = getColumnSources(indexToCheck, source);
        if (rowSet.isTracking()) {
            validateIndex(indexToCheck, rowSet, source, context,
                    DataIndexer.of(rowSet.trackingCast()).getDataIndex(groupColumns));
        }
    }

    public static void validateIndex(String[] indexToCheck, RowSet rowSet, Table source, String context,
            DataIndex index) {
        final ColumnSource[] groupColumns = getColumnSources(indexToCheck, source);
        final Table indexTable = index.table();

        // Create column iterators for the keys and the row set
        final CloseableIterator<?>[] keyIterators =
                Arrays.stream(indexToCheck).map(indexTable::columnIterator).toArray(CloseableIterator[]::new);
        final CloseableIterator<RowSet> rsIt = indexTable.columnIterator(index.rowSetColumnName());

        // Verify that the keys are correct in the table vs. the index.
        while (rsIt.hasNext()) {
            final RowSet rs = rsIt.next();
            final Object[] keys = Arrays.stream(keyIterators).map(CloseableIterator::next).toArray();
            final RowSet.Iterator it = rs.iterator();
            while (it.hasNext()) {
                final long next = it.nextLong();
                if (indexToCheck.length == 1) {
                    checkGroupKey(groupColumns, next, keys[0], context);
                } else {
                    checkGroupKey(groupColumns, next, keys, context);
                }
            }
        }

        // Verify that every key in the row set is in the index at the correct position.
        final DataIndex.RowSetLookup lookup = index.rowSetLookup();
        for (RowSet.Iterator it = rowSet.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object key = getValue(groupColumns, next);
            RowSet keyRowSet = lookup.apply(key);
            Assert.assertion(keyRowSet != null, "keyRowSet != null", next, "next", key, "key", context, "context");
            if (keyRowSet != null) {
                Assert.assertion(keyRowSet.find(next) >= 0, "keyRowSet.find(next) >= 0", next, "next", key, "key",
                        keyRowSet, "keyRowSet", context, "context");
            }
        }
    }

    public static void validateRestrictedIndex(String[] indexToCheck, RowSet rowSet, Table source,
            String context, Map<Object, RowSet> index, Set<Object> validKeys) {
        ColumnSource[] groupColumns = getColumnSources(indexToCheck, source);
        for (Map.Entry<Object, RowSet> objectIndexEntry : index.entrySet()) {
            final Object groupKey = objectIndexEntry.getKey();
            Assert.assertion(validKeys.contains(groupKey), "validKeys.contains(objectIndexEntry.getKey())", groupKey,
                    "groupKey", validKeys, "validKeys");
            for (RowSet.Iterator it = objectIndexEntry.getValue().iterator(); it.hasNext();) {
                long next = it.nextLong();
                checkGroupKey(groupColumns, next, groupKey, context);
            }
        }

        for (RowSet.Iterator it = rowSet.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object key = getValue(groupColumns, next);
            RowSet keyRowSet = index.get(key);

            if (validKeys.contains(key)) {
                Assert.assertion(keyRowSet != null, "keyRowSet != null", next, "next", key, "key", context, "context");
                if (keyRowSet != null) {
                    Assert.assertion(keyRowSet.find(next) >= 0, "keyRowSet.find(next) >= 0", next, "next", key, "key",
                            keyRowSet, "keyRowSet", context, "context");
                }
            } else {
                Assert.assertion(keyRowSet == null, "keyRowSet == null", next, "next", key, "key", context, "context");
            }
        }
    }

    private void validatePrevIndex(String[] indexToCheck, TrackingRowSet rowSet) {
        final ColumnSource[] groupColumns = getColumnSources(indexToCheck, source);

        final DataIndex index = DataIndexer.of(rowSet).getDataIndex(groupColumns);
        final Table indexTable = index.table(true);

        // Create column iterators for the keys and the row set
        final CloseableIterator<?>[] keyIterators =
                Arrays.stream(indexToCheck).map(indexTable::columnIterator).toArray(CloseableIterator[]::new);
        final CloseableIterator<RowSet> rsIt = indexTable.columnIterator(index.rowSetColumnName());

        // Verify that the keys are correct in the table vs. the index.
        while (rsIt.hasNext()) {
            final RowSet rs = rsIt.next();
            final Object[] keys = Arrays.stream(keyIterators).map(CloseableIterator::next).toArray();
            final RowSet.Iterator it = rs.iterator();
            while (it.hasNext()) {
                final long next = it.nextLong();
                if (indexToCheck.length == 1) {
                    checkGroupPrevKey(groupColumns, next, keys[0], context);
                } else {
                    checkGroupPrevKey(groupColumns, next, keys, context);
                }
            }
        }

        final DataIndex.RowSetLookup lookup = index.rowSetLookup();
        for (RowSet.Iterator it = rowSet.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object key = getPrevValue(groupColumns, next);
            RowSet keyRowSet = lookup.apply(key);
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
        Assert.assertion(Objects.equals(value, key), "value.equals(key)", value, "value", key, "key", context,
                "context");
    }

    static private void checkGroupPrevKey(final ColumnSource[] groupColumns, final long next, final Object key,
            final String context) {
        Object value = getPrevValue(groupColumns, next);
        Assert.assertion(value == key || value.equals(key), "value.equals(key)", value, "value", key, "key", context,
                "context");
    }

    static private Object getValue(ColumnSource[] groupColumns, long next) {
        return TupleSourceFactory.makeTupleSource(groupColumns).createTuple(next);
    }

    static private Object getPrevValue(ColumnSource[] groupColumns, long next) {
        return TupleSourceFactory.makeTupleSource(groupColumns).createPreviousTuple(next);
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        validateIndexes(indexColumns, source.getRowSet());
        // NB: This would normally be inappropriate: we don't expect index support on the non-tracking row sets we
        // use for updates. Forcing support by cloning and making the result tracking.
        validateIndexes(indexColumns, upstream.added().copy().toTracking());
        validateIndexes(indexColumns, upstream.modified().copy().toTracking());
        validationCount++;
        System.out.println("Validation Count for " + context + ": " + validationCount);
    }

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        originalException.printStackTrace();
        TestCase.fail("Failure for context " + context + ": " + originalException.getMessage());
    }
}
