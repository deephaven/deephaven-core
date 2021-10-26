package io.deephaven.engine.v2.utils;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.v2.utils.singlerange.SingleRange;

/**
 * Standard implementation for {@link RowSetFactory}.
 */
public class RowSetFactoryImpl implements RowSetFactory {

    private static final boolean USE_PRIORITY_QUEUE_RANDOM_BUILDER =
            Configuration.getInstance().getBooleanWithDefault("RowSetFactory.usePriorityQueueRandomBuilder", true);

    public static final RowSetFactory INSTANCE = new RowSetFactoryImpl();

    private RowSetFactoryImpl() {}

    @Override
    public MutableRowSet getEmptyRowSet() {
        return new MutableRowSetImpl();
    }

    @Override
    public MutableRowSet getRowSetByValues(final long... rowKeys) {
        if (rowKeys.length == 0) {
            return getEmptyRowSet();
        }
        if (rowKeys.length == 1) {
            return getRowSetByRange(rowKeys[0], rowKeys[0]);
        }
        final RowSetBuilderRandom indexBuilder = getRandomBuilder();
        for (long key : rowKeys) {
            indexBuilder.addKey(key);
        }
        return indexBuilder.build();
    }

    @Override
    public MutableRowSet getRowSetByValues(final long rowKey) {
        return getRowSetByRange(rowKey, rowKey);
    }

    @Override
    public RowSet getRowSetByValues(final TLongArrayList list) {
        list.sort();
        final RowSetBuilderSequential builder = getSequentialBuilder();
        list.forEach(builder);
        return builder.build();
    }

    @Override
    public MutableRowSet getRowSetByRange(final long firstRowKey, final long lastRowKey) {
        return new MutableRowSetImpl(SingleRange.make(firstRowKey, lastRowKey));
    }

    @Override
    public MutableRowSet getFlatRowSet(final long size) {
        return size <= 0 ? getEmptyRowSet() : getRowSetByRange(0, size - 1);
    }

    @Override
    public RowSetBuilderRandom getRandomBuilder() {
        if (USE_PRIORITY_QUEUE_RANDOM_BUILDER) {
            return new AdaptiveRowSetBuilderRandom();
        } else {
            return new BasicRowSetBuilderRandom();
        }
    }

    @Override
    public RowSetBuilderSequential getSequentialBuilder() {
        return new BasicRowSetBuilderSequential();
    }
}
