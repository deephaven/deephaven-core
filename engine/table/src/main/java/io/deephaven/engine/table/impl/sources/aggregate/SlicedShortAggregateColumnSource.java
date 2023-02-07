/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAggregateColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.vector.SlicedRowSetPrevShortVectorColumnWrapper;
import io.deephaven.engine.table.impl.vector.SlicedRowSetShortVectorColumnWrapper;
import io.deephaven.vector.ShortVector;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result short columns.
 */
public final class SlicedShortAggregateColumnSource extends BaseAggregateSlicedColumnSource<ShortVector, Short> {
    public SlicedShortAggregateColumnSource(
            @NotNull final ColumnSource<Short> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource,
            @NotNull final ColumnSource<Long> startSource,
            @NotNull final ColumnSource<Long> endSource) {
        super(ShortVector.class, aggregatedSource, groupRowSetSource, startSource, endSource);
    }

    @Override
    public ShortVector get(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }

        final long startKey = startSource.get(rowKey);
        final long endKey = endSource.get(rowKey);
        if (startKey == RowSequence.NULL_ROW_KEY || endKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }

        // determine the slice of the groupRowSetSource from start to end
        final RowSet rowSetSlice = groupRowSetSource.get(rowKey).subSetByKeyRange(startKey, endKey);
        return new SlicedRowSetShortVectorColumnWrapper(aggregatedSource, rowSetSlice);
    }

    @Override
    public ShortVector getPrev(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }

        final long startKey = startSource.getPrev(rowKey);
        final long endKey = endSource.getPrev(rowKey);
        if (startKey == RowSequence.NULL_ROW_KEY || endKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }

        // determine the slice of the prev groupRowSetSource from start to end
        final RowSet rowSetSlice = getPrevGroupRowSet(rowKey).subSetByKeyRange(startKey, endKey);
        return new SlicedRowSetPrevShortVectorColumnWrapper(aggregatedSource, rowSetSlice);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> groupRowSetChunk = groupRowSetSource
                .getChunk(((AggregateSlicedFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final LongChunk startChunk = startSource
                .getChunk(((AggregateSlicedFillContext) context).startGetContext, rowSequence).asLongChunk();
        final LongChunk endChunk = endSource
                .getChunk(((AggregateSlicedFillContext) context).endGetContext, rowSequence).asLongChunk();

        final WritableObjectChunk<ShortVector, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            final long startKey = startChunk.get(di);
            final long endKey = endChunk.get(di);
            if (startKey == RowSequence.NULL_ROW_KEY || endKey == RowSequence.NULL_ROW_KEY) {
                typedDestination.set(di, null);
            } else {
                // determine the slice of the groupRowSetSource from start to end
                final RowSet rowSetSlice = groupRowSetChunk.get(di).subSetByKeyRange(startKey, endKey);
                typedDestination.set(di, new SlicedRowSetShortVectorColumnWrapper(aggregatedSource, rowSetSlice));
            }
        }
        typedDestination.setSize(size);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> groupRowSetPrevChunk = groupRowSetSource
                .getPrevChunk(((AggregateSlicedFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final LongChunk startPrevChunk = startSource
                .getPrevChunk(((AggregateSlicedFillContext) context).startGetContext, rowSequence).asLongChunk();
        final LongChunk endPrevChunk = endSource
                .getPrevChunk(((AggregateSlicedFillContext) context).endGetContext, rowSequence).asLongChunk();

        final WritableObjectChunk<ShortVector, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            final long startKey = startPrevChunk.get(di);
            final long endKey = endPrevChunk.get(di);
            if (startKey == RowSequence.NULL_ROW_KEY || endKey == RowSequence.NULL_ROW_KEY) {
                typedDestination.set(di, null);
            } else {
                final RowSet groupRowSetPrev = groupRowSetPrevChunk.get(di);
                final RowSet groupRowSetToUse = groupRowSetPrev.isTracking()
                        ? groupRowSetPrev.trackingCast().copyPrev()
                        : groupRowSetPrev;

                // determine the slice of the groupRowSetSource from start to end
                final RowSet rowSetSlice = groupRowSetToUse.subSetByKeyRange(startKey, endKey);
                typedDestination.set(di, new SlicedRowSetPrevShortVectorColumnWrapper(aggregatedSource, rowSetSlice));
            }
        }
        typedDestination.setSize(size);
    }
}
