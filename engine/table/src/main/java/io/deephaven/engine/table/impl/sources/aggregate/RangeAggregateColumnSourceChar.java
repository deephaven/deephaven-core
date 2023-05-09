/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.vector.CharVectorColumnWrapper;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorSlice;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.vector.CharVectorDirect.ZERO_LENGTH_VECTOR;

/**
 * {@link ColumnSource} implementation for aggregation result char columns.
 */
public final class RangeAggregateColumnSourceChar
        extends RangeAggregateColumnSource<CharVector, Character> {

    public RangeAggregateColumnSourceChar(
            @NotNull final ColumnSource<Character> aggregated,
            @NotNull final ColumnSource<? extends RowSet> rowSets,
            @NotNull final ColumnSource<Integer> startPositionsInclusive,
            @NotNull final ColumnSource<Integer> endPositionsExclusive) {
        super(CharVector.class, aggregated, rowSets, startPositionsInclusive, endPositionsExclusive);
    }

    @NotNull
    private CharVector makeVector(
            @NotNull final RowSet rowSet,
            final int startPositionInclusive,
            final int length) {
        return new CharVectorSlice(
                new CharVectorColumnWrapper(aggregated, rowSet),
                startPositionInclusive,
                length);
    }

    @NotNull
    private CharVector makePrevVector(
            @NotNull final RowSet rowSet,
            final int startPositionInclusive,
            final int length) {
        return new CharVectorSlice(
                new CharVectorColumnWrapper(aggregatedPrev, rowSet),
                startPositionInclusive,
                length);
    }

    @Override
    public CharVector get(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }

        final int startPositionInclusive = startPositionInclusive(rowKey);
        if (startPositionInclusive == NULL_INT) {
            return null;
        }
        final int endPositionExclusive = endPositionExclusive(rowKey);
        if (endPositionExclusive == NULL_INT) {
            return null;
        }

        final int length = endPositionExclusive - startPositionInclusive;
        if (length == 0) {
            return ZERO_LENGTH_VECTOR;
        }

        final RowSet rowSet = groupRowSet(rowKey);
        return makeVector(rowSet, startPositionInclusive, length);
    }

    @Override
    public CharVector getPrev(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }

        final int startPositionInclusive = prevStartPositionInclusive(rowKey);
        if (startPositionInclusive == NULL_INT) {
            return null;
        }
        final int endPositionExclusive = prevEndPositionExclusive(rowKey);
        if (endPositionExclusive == NULL_INT) {
            return null;
        }

        final int length = endPositionExclusive - startPositionInclusive;
        if (length == 0) {
            return ZERO_LENGTH_VECTOR;
        }

        final RowSet rowSet = prevGroupRowSet(rowKey);
        return makePrevVector(rowSet, startPositionInclusive, length);
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final RangeAggregateFillContext fc = (RangeAggregateFillContext) context;

        final ObjectChunk<RowSet, ? extends Values> rowSetChunk =
                rowSets.getChunk(fc.rowSetsGetContext, rowSequence).asObjectChunk();
        final IntChunk<? extends Values> startPositionsChunk =
                startPositionsInclusive.getChunk(fc.startPositionsInclusiveGetContext, rowSequence).asIntChunk();
        final IntChunk<? extends Values> endPositionsChunk =
                endPositionsExclusive.getChunk(fc.endPositionsExclusiveGetContext, rowSequence).asIntChunk();

        final WritableObjectChunk<CharVector, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            final int startPositionInclusive = startPositionsChunk.get(di);
            if (startPositionInclusive == NULL_INT) {
                typedDestination.set(di, null);
                continue;
            }
            final int endPositionExclusive = endPositionsChunk.get(di);
            if (endPositionExclusive == NULL_INT) {
                typedDestination.set(di, null);
                continue;
            }
            final int length = endPositionExclusive - startPositionInclusive;
            if (length == 0) {
                typedDestination.set(di, ZERO_LENGTH_VECTOR);
                continue;
            }
            final RowSet rowSet = rowSetChunk.get(di);
            typedDestination.set(di, makeVector(rowSet, startPositionInclusive, length));
        }
        typedDestination.setSize(size);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final RangeAggregateFillContext fc = (RangeAggregateFillContext) context;

        final ObjectChunk<RowSet, ? extends Values> rowSetChunk =
                rowSets.getPrevChunk(fc.rowSetsGetContext, rowSequence).asObjectChunk();
        final IntChunk<? extends Values> startPositionsChunk =
                startPositionsInclusive.getPrevChunk(fc.startPositionsInclusiveGetContext, rowSequence).asIntChunk();
        final IntChunk<? extends Values> endPositionsChunk =
                endPositionsExclusive.getPrevChunk(fc.endPositionsExclusiveGetContext, rowSequence).asIntChunk();

        final WritableObjectChunk<CharVector, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            final int startPositionInclusive = startPositionsChunk.get(di);
            if (startPositionInclusive == NULL_INT) {
                typedDestination.set(di, null);
                continue;
            }
            final int endPositionExclusive = endPositionsChunk.get(di);
            if (endPositionExclusive == NULL_INT) {
                typedDestination.set(di, null);
                continue;
            }
            final int length = endPositionExclusive - startPositionInclusive;
            if (length == 0) {
                typedDestination.set(di, ZERO_LENGTH_VECTOR);
                continue;
            }
            final RowSet rowSetRaw = rowSetChunk.get(di);
            final RowSet rowSet = rowSetRaw.isTracking() ? rowSetRaw.trackingCast().prev() : rowSetRaw;
            typedDestination.set(di, makePrevVector(rowSet, startPositionInclusive, length));
        }
        typedDestination.setSize(size);
    }
}
