//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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
public abstract class RangeAggregateColumnSource<VECTOR_TYPE extends Vector<VECTOR_TYPE>, COMPONENT_TYPE>
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
    RowSet groupRowSet(final long groupRowKey) {
        // noinspection DataFlowIssue
        return rowSets.get(groupRowKey);
    }

    @NotNull
    RowSet prevGroupRowSet(final long groupRowKey) {
        final RowSet prevGroupRowSet = rowSets.getPrev(groupRowKey);
        // noinspection DataFlowIssue
        return prevGroupRowSet.isTracking()
                ? prevGroupRowSet.trackingCast().prev()
                : prevGroupRowSet;
    }

    int startPositionInclusive(final long groupRowKey) {
        return startPositionsInclusive.getInt(groupRowKey);
    }

    int endPositionExclusive(final long groupRowKey) {
        return endPositionsExclusive.getInt(groupRowKey);
    }

    int prevStartPositionInclusive(final long groupRowKey) {
        return startPositionsInclusive.getPrevInt(groupRowKey);
    }

    int prevEndPositionExclusive(final long groupRowKey) {
        return endPositionsExclusive.getPrevInt(groupRowKey);
    }

    @Override
    public long getUngroupedSize(final long groupRowKey) {
        if (groupRowKey == NULL_ROW_KEY) {
            return 0;
        }
        final long startPositionInclusive = startPositionInclusive(groupRowKey);
        if (startPositionInclusive == NULL_INT) {
            return 0;
        }
        final long endPositionExclusive = endPositionExclusive(groupRowKey);
        if (endPositionExclusive == NULL_INT) {
            return 0;
        }
        return endPositionExclusive - startPositionInclusive;
    }

    @Override
    public final long getUngroupedPrevSize(final long groupRowKey) {
        if (groupRowKey == NULL_ROW_KEY) {
            return 0;
        }
        final long startPositionInclusive = prevStartPositionInclusive(groupRowKey);
        if (startPositionInclusive == NULL_INT) {
            return 0;
        }
        final long endPositionExclusive = prevEndPositionExclusive(groupRowKey);
        if (endPositionExclusive == NULL_INT) {
            return 0;
        }
        return endPositionExclusive - startPositionInclusive;
    }

    private long groupAndOffsetToOuterRowKey(final long groupRowKey, final int offsetInGroup) {
        // noinspection resource
        final RowSet groupRowSet = groupRowSet(groupRowKey);
        final int startPositionInclusive = startPositionInclusive(groupRowKey);
        return groupRowSet.get(startPositionInclusive + offsetInGroup);
    }

    private long prevGroupAndOffsetToOuterRowKey(final long groupRowKey, final int offsetInGroup) {
        // noinspection resource
        final RowSet groupRowSet = prevGroupRowSet(groupRowKey);
        final int startPositionInclusive = prevStartPositionInclusive(groupRowKey);
        return groupRowSet.get(startPositionInclusive + offsetInGroup);
    }

    @Override
    public final Object getUngrouped(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return null;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.get(outerRowKey);
    }

    @Override
    public final Object getUngroupedPrev(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return null;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrev(outerRowKey);
    }

    @Override
    public final Boolean getUngroupedBoolean(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return null;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getBoolean(outerRowKey);
    }

    @Override
    public final Boolean getUngroupedPrevBoolean(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_BOOLEAN;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrevBoolean(outerRowKey);
    }

    @Override
    public final double getUngroupedDouble(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getDouble(outerRowKey);
    }

    @Override
    public final double getUngroupedPrevDouble(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrevDouble(outerRowKey);
    }

    @Override
    public final float getUngroupedFloat(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getFloat(outerRowKey);
    }

    @Override
    public final float getUngroupedPrevFloat(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_FLOAT;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrevFloat(outerRowKey);
    }

    @Override
    public final byte getUngroupedByte(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getByte(outerRowKey);
    }

    @Override
    public final byte getUngroupedPrevByte(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrevByte(outerRowKey);
    }

    @Override
    public final char getUngroupedChar(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getChar(outerRowKey);
    }

    @Override
    public final char getUngroupedPrevChar(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrevChar(outerRowKey);
    }

    @Override
    public final short getUngroupedShort(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getShort(outerRowKey);
    }

    @Override
    public final short getUngroupedPrevShort(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrevShort(outerRowKey);
    }

    @Override
    public final int getUngroupedInt(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getInt(outerRowKey);
    }

    @Override
    public final int getUngroupedPrevInt(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_INT;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrevInt(outerRowKey);
    }

    @Override
    public final long getUngroupedLong(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long outerRowKey = groupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getLong(outerRowKey);
    }

    @Override
    public final long getUngroupedPrevLong(final long groupRowKey, final int offsetInGroup) {
        if (groupRowKey == NULL_ROW_KEY) {
            return NULL_LONG;
        }
        final long outerRowKey = prevGroupAndOffsetToOuterRowKey(groupRowKey, offsetInGroup);
        return aggregated.getPrevLong(outerRowKey);
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

    @Override
    public ColumnSource<COMPONENT_TYPE> getAggregatedSource() {
        return aggregated;
    }
}
