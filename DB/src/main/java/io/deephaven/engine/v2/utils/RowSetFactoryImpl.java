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
    public MutableRowSet empty() {
        return new MutableRowSetImpl();
    }

    @Override
    public MutableRowSet fromKeys(final long... rowKeys) {
        if (rowKeys.length == 0) {
            return empty();
        }
        if (rowKeys.length == 1) {
            return fromRange(rowKeys[0], rowKeys[0]);
        }
        final RowSetBuilderRandom indexBuilder = builderRandom();
        for (long key : rowKeys) {
            indexBuilder.addKey(key);
        }
        return indexBuilder.build();
    }

    @Override
    public MutableRowSet fromKeys(final long rowKey) {
        return fromRange(rowKey, rowKey);
    }

    @Override
    public RowSet fromKeys(final TLongArrayList list) {
        list.sort();
        final RowSetBuilderSequential builder = builderSequential();
        list.forEach(builder);
        return builder.build();
    }

    @Override
    public MutableRowSet fromRange(final long firstRowKey, final long lastRowKey) {
        return new MutableRowSetImpl(SingleRange.make(firstRowKey, lastRowKey));
    }

    @Override
    public MutableRowSet flat(final long size) {
        return size <= 0 ? empty() : fromRange(0, size - 1);
    }

    @Override
    public RowSetBuilderRandom builderRandom() {
        if (USE_PRIORITY_QUEUE_RANDOM_BUILDER) {
            return new AdaptiveRowSetBuilderRandom();
        } else {
            return new BasicRowSetBuilderRandom();
        }
    }

    @Override
    public RowSetBuilderSequential builderSequential() {
        return new BasicRowSetBuilderSequential();
    }
}
