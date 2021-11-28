/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import junit.framework.TestCase;

import java.util.*;

/**
 * This class listens to a table and on each update verifies that the groupings returned by the table's RowSet for a set
 * of columns are still valid. It is meant to be used as part of a unit test for incremental updates, to ensure that
 * stale groupings are not left between table updates.
 */
public class GroupingValidator extends InstrumentedTableUpdateListenerAdapter {
    private final Table source;
    private final Collection<String[]> groupingColumns;
    private final String context;
    private int validationCount = 0;

    public GroupingValidator(String context, Table source, ArrayList<ArrayList<String>> groupingColumns) {
        this(context, source, convertListToArray(groupingColumns));
    }

    static private Collection<String[]> convertListToArray(ArrayList<ArrayList<String>> groupingColumns) {
        Collection<String[]> collectionOfArrays = new ArrayList<>();
        for (ArrayList<String> columnSet : groupingColumns) {
            collectionOfArrays.add(columnSet.toArray(new String[columnSet.size()]));
        }
        return collectionOfArrays;
    }

    private GroupingValidator(String context, Table source, Collection<String[]> groupingColumns) {
        super("grouping validator " + context, source, false);
        this.context = context;
        this.source = source;
        this.groupingColumns = groupingColumns;

        validateGroupings(groupingColumns, source.getRowSet());
        validatePrevGroupings(groupingColumns, source.getRowSet());

        source.listenForUpdates(this);
    }

    private void validateGroupings(Collection<String[]> groupingColumns, RowSet rowSet) {
        for (String[] groupingToCheck : groupingColumns) {
            validateGrouping(groupingToCheck, rowSet, source, context);
        }
    }

    private void validatePrevGroupings(Collection<String[]> groupingColumns, TrackingRowSet rowSet) {
        for (String[] groupingToCheck : groupingColumns) {
            validatePrevGrouping(groupingToCheck, rowSet);
        }
    }

    public static void validateGrouping(String[] groupingToCheck, RowSet rowSet, Table source, String context) {
        final ColumnSource[] groupColumns = getColumnSources(groupingToCheck, source);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(groupColumns);
        validateGrouping(groupingToCheck, rowSet, source, context,
                rowSet.isTracking()
                        ? RowSetIndexer.of(rowSet.trackingCast()).getGrouping(tupleSource)
                        : Collections.emptyMap());
    }

    public static void validateGrouping(String[] groupingToCheck, RowSet rowSet, Table source, String context,
            Map<Object, RowSet> grouping) {
        final ColumnSource[] groupColumns = getColumnSources(groupingToCheck, source);
        for (Map.Entry<Object, RowSet> objectIndexEntry : grouping.entrySet()) {
            for (RowSet.Iterator it = objectIndexEntry.getValue().iterator(); it.hasNext();) {
                long next = it.nextLong();
                checkGroupKey(groupColumns, next, objectIndexEntry.getKey(), context);
            }
        }

        for (RowSet.Iterator it = rowSet.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object key = getValue(groupColumns, next);
            RowSet keyRowSet = grouping.get(key);
            Assert.assertion(keyRowSet != null, "keyRowSet != null", next, "next", key, "key", context, "context");
            if (keyRowSet != null) {
                Assert.assertion(keyRowSet.find(next) >= 0, "keyRowSet.find(next) >= 0", next, "next", key, "key",
                        keyRowSet, "keyRowSet", context, "context");
            }
        }
    }

    public static void validateRestrictedGrouping(String[] groupingToCheck, RowSet rowSet, Table source,
            String context, Map<Object, RowSet> grouping, Set<Object> validKeys) {
        ColumnSource[] groupColumns = getColumnSources(groupingToCheck, source);
        for (Map.Entry<Object, RowSet> objectIndexEntry : grouping.entrySet()) {
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
            RowSet keyRowSet = grouping.get(key);

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

    private void validatePrevGrouping(String[] groupingToCheck, TrackingRowSet rowSet) {
        final ColumnSource[] groupColumns = getColumnSources(groupingToCheck, source);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(groupColumns);
        final Map<Object, RowSet> grouping = RowSetIndexer.of(rowSet).getPrevGrouping(tupleSource);
        for (Map.Entry<Object, RowSet> objectIndexEntry : grouping.entrySet()) {
            for (RowSet.Iterator it = objectIndexEntry.getValue().iterator(); it.hasNext();) {
                long next = it.nextLong();
                checkGroupPrevKey(groupColumns, next, objectIndexEntry.getKey(), context);
            }
        }

        for (RowSet.Iterator it = rowSet.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object key = getPrevValue(groupColumns, next);
            RowSet keyRowSet = grouping.get(key);
            Assert.assertion(keyRowSet != null, "keyRowSet != null", next, "next", key, "key", context, "context");
            if (keyRowSet != null) {
                Assert.assertion(keyRowSet.find(next) >= 0, "keyRowSet.find(next) >= 0", next, "next", key, "key",
                        keyRowSet, "keyRowSet", context, "context");
            }
        }
    }

    private static ColumnSource[] getColumnSources(String[] groupingToCheck, Table source) {
        return Arrays.stream(groupingToCheck).map(source::getColumnSource).toArray(ColumnSource[]::new);
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
        validateGroupings(groupingColumns, source.getRowSet());
        // NB: This would normally be inappropriate: we don't expect grouping support on the non-tracking row sets we
        // use for updates. Forcing support by cloning and making the result tracking.
        validateGroupings(groupingColumns, upstream.added().copy().toTracking());
        validateGroupings(groupingColumns, upstream.modified().copy().toTracking());
        validationCount++;
        System.out.println("Validation Count for " + context + ": " + validationCount);
    }

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        originalException.printStackTrace();
        TestCase.fail("Failure for context " + context + ": " + originalException.getMessage());
    }
}
