/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.sources.UngroupedColumnSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.util.QueryConstants.*;

/**
 * Base {@link ColumnSource} implementation for range join group aggregation columns.
 * <p>
 * Elements are {@link Vector vectors} defined by a {@link RowSet}, an inclusive start position, and an exclusive end
 * position. When either the start position or end position is null, the element is null. When the start position and
 * end position are equal, the element is empty.
 */
public abstract class RangeAggregateColumnSource<VECTOR_TYPE extends Vector, COMPONENT_TYPE>
        extends AbstractColumnSource<VECTOR_TYPE>
        implements AggregateColumnSource<VECTOR_TYPE, COMPONENT_TYPE> {

    protected final ColumnSource<COMPONENT_TYPE> aggregated;
    protected final ColumnSource<COMPONENT_TYPE> aggregatedPrev;
    protected final ColumnSource<? extends RowSet> rowSets;
    protected final ColumnSource<Integer> startPositionsInclusive;
    protected final ColumnSource<Integer> endPositionsExclusive;

    protected RangeAggregateColumnSource(
            @NotNull final Class<VECTOR_TYPE> vectorType,
            @NotNull final ColumnSource<COMPONENT_TYPE> aggregated,
            @NotNull final ColumnSource<? extends RowSet> rowSets,
            @NotNull final ColumnSource<Integer> startPositionsInclusive,
            @NotNull final ColumnSource<Integer> endPositionsExclusive) {
        super(vectorType, aggregated.getType());
        this.aggregated = aggregated;
        aggregatedPrev = aggregated.isImmutable() ? aggregated : aggregated.getPrevSource();
        this.rowSets = rowSets;
        this.startPositionsInclusive = startPositionsInclusive;
        this.endPositionsExclusive = endPositionsExclusive;
    }

    @Override
    public final UngroupedColumnSource<COMPONENT_TYPE> ungrouped() {
        return new UngroupedRangeAggregateColumnSource<>(this);
    }

    @Override
    public final void startTrackingPrevValues() {}

    protected static final class RangeAggregateFillContext implements FillContext {

        public final GetContext rowSetsGetContext;
        public final GetContext startPositionsInclusiveGetContext;
        public final GetContext endPositionsExclusiveGetContext;

        private RangeAggregateFillContext(
                @NotNull final ColumnSource<? extends RowSet> rowSets,
                @NotNull final ColumnSource<Integer> startPositionsInclusive,
                @NotNull final ColumnSource<Integer> endPositionsExclusive,
                final int chunkCapacity,
                final SharedContext sharedContext) {
            rowSetsGetContext = rowSets.makeGetContext(chunkCapacity, sharedContext);
            startPositionsInclusiveGetContext = startPositionsInclusive.makeGetContext(chunkCapacity, sharedContext);
            endPositionsExclusiveGetContext = endPositionsExclusive.makeGetContext(chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(
                    rowSetsGetContext,
                    startPositionsInclusiveGetContext,
                    endPositionsExclusiveGetContext);
        }
    }

    @Override
    public final FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new RangeAggregateFillContext(rowSets, startPositionsInclusive, endPositionsExclusive,
                chunkCapacity, sharedContext);
    }

    @Override
    public final boolean isUngroupable() {
        return true;
    }

    @NotNull
    RowSet groupRowSet(final long groupIndex) {
        // noinspection DataFlowIssue
        return rowSets.get(groupIndex);
    }

    @NotNull
    RowSet prevGroupRowSet(final long groupIndex) {
        final RowSet prevGroupRowSet = rowSets.getPrev(groupIndex);
        // noinspection DataFlowIssue
        return prevGroupRowSet.isTracking()
                ? prevGroupRowSet.trackingCast().prev()
                : prevGroupRowSet;
    }

    int startPositionInclusive(final long groupIndexKey) {
        return startPositionsInclusive.getInt(groupIndexKey);
    }

    int endPositionExclusive(final long groupIndexKey) {
        return endPositionsExclusive.getInt(groupIndexKey);
    }

    int prevStartPositionInclusive(final long groupIndexKey) {
        return startPositionsInclusive.getPrevInt(groupIndexKey);
    }

    int prevEndPositionExclusive(final long groupIndexKey) {
        return endPositionsExclusive.getPrevInt(groupIndexKey);
    }

    @Override
    public long getUngroupedSize(final long groupIndexKey) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return 0;
        }
        final long startPositionInclusive = startPositionInclusive(groupIndexKey);
        if (startPositionInclusive == NULL_INT) {
            return 0;
        }
        final long endPositionExclusive = endPositionExclusive(groupIndexKey);
        if (endPositionExclusive == NULL_INT) {
            return 0;
        }
        return endPositionExclusive - startPositionInclusive;
    }

    @Override
    public final long getUngroupedPrevSize(final long groupIndexKey) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return 0;
        }
        final long startPositionInclusive = prevStartPositionInclusive(groupIndexKey);
        if (startPositionInclusive == NULL_INT) {
            return 0;
        }
        final long endPositionExclusive = prevEndPositionExclusive(groupIndexKey);
        if (endPositionExclusive == NULL_INT) {
            return 0;
        }
        return endPositionExclusive - startPositionInclusive;
    }

    private long groupAndOffsetToOuterRowKey(final long groupIndex, final int offsetInGroup) {
        // noinspection resource
        final RowSet groupRowSet = groupRowSet(groupIndex);
        final int startPositionInclusive = startPositionInclusive(groupIndex);
        return groupRowSet.get(startPositionInclusive + offsetInGroup);
    }

    private long prevGroupAndOffsetToOuterRowKey(final long groupIndex, final int offsetInGroup) {
        // noinspection resource
        final RowSet groupRowSet = prevGroupRowSet(groupIndex);
        final int startPositionInclusive = prevStartPositionInclusive(groupIndex);
        return groupRowSet.get(startPositionInclusive + offsetInGroup);
    }

    @Override
    public final Object getUngrouped(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return null;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.get(rowKey);
    }

    @Override
    public final Object getUngroupedPrev(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return null;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrev(rowKey);
    }

    @Override
    public final Boolean getUngroupedBoolean(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return null;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getBoolean(rowKey);
    }

    @Override
    public final Boolean getUngroupedPrevBoolean(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrevBoolean(rowKey);
    }

    @Override
    public final double getUngroupedDouble(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getDouble(rowKey);
    }

    @Override
    public final double getUngroupedPrevDouble(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrevDouble(rowKey);
    }

    @Override
    public final float getUngroupedFloat(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getFloat(rowKey);
    }

    @Override
    public final float getUngroupedPrevFloat(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrevFloat(rowKey);
    }

    @Override
    public final byte getUngroupedByte(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getByte(rowKey);
    }

    @Override
    public final byte getUngroupedPrevByte(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrevByte(rowKey);
    }

    @Override
    public final char getUngroupedChar(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getChar(rowKey);
    }

    @Override
    public final char getUngroupedPrevChar(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrevChar(rowKey);
    }

    @Override
    public final short getUngroupedShort(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getShort(rowKey);
    }

    @Override
    public final short getUngroupedPrevShort(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrevShort(rowKey);
    }

    @Override
    public final int getUngroupedInt(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getInt(rowKey);
    }

    @Override
    public final int getUngroupedPrevInt(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrevInt(rowKey);
    }

    @Override
    public final long getUngroupedLong(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long rowKey = groupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getLong(rowKey);
    }

    @Override
    public final long getUngroupedPrevLong(final long groupIndexKey, final int offsetInGroup) {
        if (groupIndexKey == NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long rowKey = prevGroupAndOffsetToOuterRowKey(groupIndexKey, offsetInGroup);
        return aggregated.getPrevLong(rowKey);
    }

    @Override
    public boolean isStateless() {
        return aggregated.isStateless()
                && rowSets.isStateless()
                && startPositionsInclusive.isStateless()
                && endPositionsExclusive.isStateless();
    }


    @Override
    public boolean isImmutable() {
        return aggregated.isImmutable()
                && rowSets.isImmutable()
                && startPositionsInclusive.isImmutable()
                && endPositionsExclusive.isImmutable();
    }
}
