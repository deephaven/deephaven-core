package io.deephaven.engine.v2.utils;

import gnu.trove.list.TLongList;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.v2.utils.singlerange.SingleRange;

import javax.validation.constraints.NotNull;

/**
 * Repository of factory methods for constructing {@link MutableRowSet row sets}.
 */
public abstract class RowSetFactory {

    private static final boolean USE_PRIORITY_QUEUE_RANDOM_BUILDER =
            Configuration.getInstance().getBooleanWithDefault("RowSetFactory.usePriorityQueueRandomBuilder", true); // TODO-RWC:
                                                                                                                    // Ask
                                                                                                                    // Cristian
                                                                                                                    // if
                                                                                                                    // I
                                                                                                                    // can
                                                                                                                    // delete
                                                                                                                    // this.

    private RowSetFactory() {}

    /**
     * Get an empty {@link MutableRowSet}.
     *
     * @return A new {@link MutableRowSet} containing no rows
     */
    public static MutableRowSet empty() {
        return new MutableRowSetImpl();
    }

    /**
     * Get a {@link MutableRowSet} containing the specified row keys. Row keys must be nonnegative numbers.
     *
     * @param rowKeys The row keys to include
     * @return A new {@link MutableRowSet} containing the specified row keys
     */
    public static MutableRowSet fromKeys(final long... rowKeys) {
        if (rowKeys.length == 0) {
            return empty();
        }
        if (rowKeys.length == 1) {
            return fromKeys(rowKeys[0]);
        }
        final RowSetBuilderRandom indexBuilder = builderRandom();
        for (final long rowKey : rowKeys) {
            indexBuilder.addKey(rowKey);
        }
        return indexBuilder.build();
    }

    /**
     * Produce a {@link MutableRowSet} containing a single row key. Row keys must be nonnegative numbers.
     *
     * @param rowKey The row key to include
     * @return A new {@link MutableRowSet} containing the specified row key
     */
    public static MutableRowSet fromKeys(final long rowKey) {
        return fromRange(rowKey, rowKey);
    }

    /**
     * Get a {@link MutableRowSet} containing the specified row keys.
     * <p>
     * The provided {@link TLongList} is sorted and then passed to a {@link RowSetBuilderSequential}.
     *
     * @param rowKeys A {@link TLongList}. Note that this list is mutated within the method!
     * @return A new {@link MutableRowSet} containing the values from {@code rowKeys}
     */
    public static RowSet fromKeys(@NotNull final TLongList rowKeys) {
        rowKeys.sort();
        final RowSetBuilderSequential builder = builderSequential();
        rowKeys.forEach(builder);
        return builder.build();
    }

    /**
     * Create a {@link MutableRowSet} containing the continuous range [firstRowKey, lastRowKey].
     *
     * @param firstRowKey The first row key in the continuous range
     * @param lastRowKey The last row key in the continuous range
     * @return A new {@link MutableRowSet} containing the specified row key range
     */
    public static MutableRowSet fromRange(final long firstRowKey, final long lastRowKey) {
        return new MutableRowSetImpl(SingleRange.make(firstRowKey, lastRowKey));
    }

    /**
     * Get a flat {@link MutableRowSet} containing the row key range {@code [0, size)}, or an {@link #empty() empty row
     * set} if {@code size <= 0}.
     *
     * @param size The size of the {@link MutableRowSet} to create
     * @return A flat {@link MutableRowSet} containing the row key range {@code [0, size)} or an {@link #empty() empty
     *         row set} if the {@code size <= 0}
     */
    public static MutableRowSet flat(final long size) {
        return size <= 0 ? empty() : fromRange(0, size - 1);
    }

    /**
     * @return A {@link RowSetBuilderRandom} suitable for inserting row keys and row key ranges in no particular order
     */
    public static RowSetBuilderRandom builderRandom() {
        if (USE_PRIORITY_QUEUE_RANDOM_BUILDER) {
            return new AdaptiveRowSetBuilderRandom();
        } else {
            return new BasicRowSetBuilderRandom();
        }
    }

    /**
     * @return A {@link RowSetBuilderRandom} optimized for inserting row keys and row key ranges sequentially in order
     */
    public static RowSetBuilderSequential builderSequential() {
        return new BasicRowSetBuilderSequential();
    }
}
