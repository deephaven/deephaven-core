/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.db.v2.tuples.TupleSourceFactory;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import junit.framework.TestCase;

import java.util.*;

/**
 * This class listens to a table and on each update verifies that the groupings returned by the
 * table's index for a set of columns are still valid. It is meant to be used as part of a unit test
 * for incremental updates, to ensure that stale groupings are not left between table updates.
 */
public class GroupingValidator extends InstrumentedShiftAwareListenerAdapter {
    private DynamicTable source;
    private Collection<String[]> groupingColumns;
    private String context;
    private int validationCount = 0;

    public GroupingValidator(String context, DynamicTable source,
        ArrayList<ArrayList<String>> groupingColumns) {
        this(context, source, convertListToArray(groupingColumns));
    }

    static private Collection<String[]> convertListToArray(
        ArrayList<ArrayList<String>> groupingColumns) {
        Collection<String[]> collectionOfArrays = new ArrayList<>();
        for (ArrayList<String> columnSet : groupingColumns) {
            collectionOfArrays.add(columnSet.toArray(new String[columnSet.size()]));
        }
        return collectionOfArrays;
    }

    private GroupingValidator(String context, DynamicTable source,
        Collection<String[]> groupingColumns) {
        super("grouping validator " + context, source, false);
        this.context = context;
        this.source = source;
        this.groupingColumns = groupingColumns;

        validateGroupings(groupingColumns, source.getIndex());
        validatePrevGroupings(groupingColumns, source.getIndex());

        source.listenForUpdates(this);
    }

    private void validateGroupings(Collection<String[]> groupingColumns, Index index) {
        for (String[] groupingToCheck : groupingColumns) {
            validateGrouping(groupingToCheck, index, source, context);
        }
    }

    private void validatePrevGroupings(Collection<String[]> groupingColumns, Index index) {
        for (String[] groupingToCheck : groupingColumns) {
            validatePrevGrouping(groupingToCheck, index);
        }
    }

    public static void validateGrouping(String[] groupingToCheck, Index index, DynamicTable source,
        String context) {
        final ColumnSource[] groupColumns = getColumnSources(groupingToCheck, source);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(groupColumns);
        validateGrouping(groupingToCheck, index, source, context, index.getGrouping(tupleSource));
    }

    public static void validateGrouping(String[] groupingToCheck, Index index, DynamicTable source,
        String context, Map<Object, Index> grouping) {
        final ColumnSource[] groupColumns = getColumnSources(groupingToCheck, source);
        for (Map.Entry<Object, Index> objectIndexEntry : grouping.entrySet()) {
            for (Index.Iterator it = objectIndexEntry.getValue().iterator(); it.hasNext();) {
                long next = it.nextLong();
                checkGroupKey(groupColumns, next, objectIndexEntry.getKey(), context);
            }
        }

        for (Index.Iterator it = index.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object key = getValue(groupColumns, next);
            Index keyIndex = grouping.get(key);
            Assert.assertion(keyIndex != null, "keyIndex != null", next, "next", key, "key",
                context, "context");
            if (keyIndex != null) {
                Assert.assertion(keyIndex.find(next) >= 0, "keyIndex.find(next) >= 0", next, "next",
                    key, "key", keyIndex, "keyIndex", context, "context");
            }
        }
    }

    public static void validateRestrictedGrouping(String[] groupingToCheck, Index index,
        DynamicTable source, String context, Map<Object, Index> grouping, Set<Object> validKeys) {
        ColumnSource[] groupColumns = getColumnSources(groupingToCheck, source);
        for (Map.Entry<Object, Index> objectIndexEntry : grouping.entrySet()) {
            final Object groupKey = objectIndexEntry.getKey();
            Assert.assertion(validKeys.contains(groupKey),
                "validKeys.contains(objectIndexEntry.getKey())", groupKey, "groupKey", validKeys,
                "validKeys");
            for (Index.Iterator it = objectIndexEntry.getValue().iterator(); it.hasNext();) {
                long next = it.nextLong();
                checkGroupKey(groupColumns, next, groupKey, context);
            }
        }

        for (Index.Iterator it = index.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object key = getValue(groupColumns, next);
            Index keyIndex = grouping.get(key);

            if (validKeys.contains(key)) {
                Assert.assertion(keyIndex != null, "keyIndex != null", next, "next", key, "key",
                    context, "context");
                if (keyIndex != null) {
                    Assert.assertion(keyIndex.find(next) >= 0, "keyIndex.find(next) >= 0", next,
                        "next", key, "key", keyIndex, "keyIndex", context, "context");
                }
            } else {
                Assert.assertion(keyIndex == null, "keyIndex == null", next, "next", key, "key",
                    context, "context");
            }
        }
    }

    private void validatePrevGrouping(String[] groupingToCheck, Index index) {
        final ColumnSource[] groupColumns = getColumnSources(groupingToCheck, source);
        final TupleSource tupleSource = TupleSourceFactory.makeTupleSource(groupColumns);
        final Map<Object, Index> grouping = index.getPrevGrouping(tupleSource);
        for (Map.Entry<Object, Index> objectIndexEntry : grouping.entrySet()) {
            for (Index.Iterator it = objectIndexEntry.getValue().iterator(); it.hasNext();) {
                long next = it.nextLong();
                checkGroupPrevKey(groupColumns, next, objectIndexEntry.getKey(), context);
            }
        }

        for (Index.Iterator it = index.iterator(); it.hasNext();) {
            long next = it.nextLong();
            Object key = getPrevValue(groupColumns, next);
            Index keyIndex = grouping.get(key);
            Assert.assertion(keyIndex != null, "keyIndex != null", next, "next", key, "key",
                context, "context");
            if (keyIndex != null) {
                Assert.assertion(keyIndex.find(next) >= 0, "keyIndex.find(next) >= 0", next, "next",
                    key, "key", keyIndex, "keyIndex", context, "context");
            }
        }
    }

    private static ColumnSource[] getColumnSources(String[] groupingToCheck, DynamicTable source) {
        return Arrays.stream(groupingToCheck).map(source::getColumnSource)
            .toArray(ColumnSource[]::new);
    }

    static private void checkGroupKey(final ColumnSource[] groupColumns, final long next,
        final Object key, final String context) {
        final Object value = getValue(groupColumns, next);
        Assert.assertion(Objects.equals(value, key), "value.equals(key)", value, "value", key,
            "key", context, "context");
    }

    static private void checkGroupPrevKey(final ColumnSource[] groupColumns, final long next,
        final Object key, final String context) {
        Object value = getPrevValue(groupColumns, next);
        Assert.assertion(value == key || value.equals(key), "value.equals(key)", value, "value",
            key, "key", context, "context");
    }

    static private Object getValue(ColumnSource[] groupColumns, long next) {
        return TupleSourceFactory.makeTupleSource(groupColumns).createTuple(next);
    }

    static private Object getPrevValue(ColumnSource[] groupColumns, long next) {
        return TupleSourceFactory.makeTupleSource(groupColumns).createPreviousTuple(next);
    }

    @Override
    public void onUpdate(final Update upstream) {
        validateGroupings(groupingColumns, source.getIndex());
        validateGroupings(groupingColumns, upstream.added);
        validateGroupings(groupingColumns, upstream.modified);
        validationCount++;
        System.out.println("Validation Count for " + context + ": " + validationCount);
    }

    @Override
    public void onFailureInternal(Throwable originalException,
        UpdatePerformanceTracker.Entry sourceEntry) {
        originalException.printStackTrace();
        TestCase.fail("Failure for context " + context + ": " + originalException.getMessage());
    }
}
