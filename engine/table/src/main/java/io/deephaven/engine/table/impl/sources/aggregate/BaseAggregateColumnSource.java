/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.vector.Vector;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.*;

/**
 * Base {@link ColumnSource} implementation for aggregation result columns.
 */
abstract class BaseAggregateColumnSource<DB_ARRAY_TYPE extends Vector, COMPONENT_TYPE>
        extends AbstractColumnSource<DB_ARRAY_TYPE> implements AggregateColumnSource<DB_ARRAY_TYPE, COMPONENT_TYPE> {

    final ColumnSource<COMPONENT_TYPE> aggregatedSource;
    final ColumnSource<? extends RowSet> groupRowSetSource;

    BaseAggregateColumnSource(@NotNull final Class<DB_ARRAY_TYPE> vectorType,
            @NotNull final ColumnSource<COMPONENT_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource) {
        super(vectorType, aggregatedSource.getType());
        this.aggregatedSource = aggregatedSource;
        this.groupRowSetSource = groupRowSetSource;
    }

    @Override
    public final UngroupedColumnSource<COMPONENT_TYPE> ungrouped() {
        return new UngroupedAggregateColumnSource<>(this);
    }

    @Override
    public final void startTrackingPrevValues() {}

    static final class AggregateFillContext implements FillContext {

        final GetContext groupRowSetGetContext;

        private AggregateFillContext(@NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
                final int chunkCapacity, final SharedContext sharedContext) {
            // TODO: Implement a proper shareable context to use with other instances that share a RowSet source.
            // Current usage is "safe" because RowSet sources are only exposed through this wrapper, and all
            // sources at a given level will pass through their ordered keys to the RowSet source unchanged.
            groupRowSetGetContext = groupRowSetSource.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            groupRowSetGetContext.close();
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new AggregateFillContext(groupRowSetSource, chunkCapacity, sharedContext);
    }

    @Override
    public final boolean isUngroupable() {
        return true;
    }

    @Override
    public final long getUngroupedSize(final long groupIndexKey) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return 0;
        }
        return groupRowSetSource.get(groupIndexKey).size();
    }

    @Override
    public final long getUngroupedPrevSize(final long groupIndexKey) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return 0;
        }
        final RowSet groupRowSetPrev = groupRowSetSource.getPrev(groupIndexKey);
        return groupRowSetPrev.isTracking()
                ? groupRowSetPrev.trackingCast().sizePrev()
                : groupRowSetPrev.size();
    }

    private long getPrevRowKey(final long groupIndexKey, final int offsetInGroup) {
        final RowSet groupRowSetPrev = groupRowSetSource.getPrev(groupIndexKey);
        return groupRowSetPrev.isTracking()
                ? groupRowSetPrev.trackingCast().getPrev(offsetInGroup)
                : groupRowSetPrev.get(offsetInGroup);
    }

    RowSet getPrevGroupRowSet(final long groupIndexKey) {
        final RowSet groupRowSetPrev = groupRowSetSource.getPrev(groupIndexKey);
        return groupRowSetPrev.isTracking()
                ? groupRowSetPrev.trackingCast().copyPrev()
                : groupRowSetPrev;
    }

    @Override
    public final Object getUngrouped(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return aggregatedSource.get(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final Object getUngroupedPrev(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return aggregatedSource.getPrev(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public final Boolean getUngroupedBoolean(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        return aggregatedSource.getBoolean(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final Boolean getUngroupedPrevBoolean(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        return aggregatedSource.getPrevBoolean(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public final double getUngroupedDouble(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        return aggregatedSource.getDouble(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final double getUngroupedPrevDouble(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        return aggregatedSource.getPrevDouble(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public final float getUngroupedFloat(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        return aggregatedSource.getFloat(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final float getUngroupedPrevFloat(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        return aggregatedSource.getPrevFloat(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public final byte getUngroupedByte(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        return aggregatedSource.getByte(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final byte getUngroupedPrevByte(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        return aggregatedSource.getPrevByte(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public final char getUngroupedChar(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        return aggregatedSource.getChar(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final char getUngroupedPrevChar(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        return aggregatedSource.getPrevChar(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public final short getUngroupedShort(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        return aggregatedSource.getShort(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final short getUngroupedPrevShort(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        return aggregatedSource.getPrevShort(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public final int getUngroupedInt(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        return aggregatedSource.getInt(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final int getUngroupedPrevInt(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_INT;
        }
        return aggregatedSource.getPrevInt(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public final long getUngroupedLong(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        return aggregatedSource.getLong(groupRowSetSource.get(groupIndexKey).get(offsetInGroup));
    }

    @Override
    public final long getUngroupedPrevLong(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == RowSequence.NULL_ROW_KEY) {
            return NULL_LONG;
        }
        return aggregatedSource.getPrevLong(getPrevRowKey(groupIndexKey, offsetInGroup));
    }

    @Override
    public boolean preventsParallelism() {
        return aggregatedSource.preventsParallelism() || groupRowSetSource.preventsParallelism();
    }

    @Override
    public boolean isStateless() {
        return aggregatedSource.isStateless() && groupRowSetSource.isStateless();
    }
}
