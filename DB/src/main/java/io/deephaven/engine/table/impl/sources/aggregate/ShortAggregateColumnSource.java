/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAggregateColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.vector.ShortVector;
import io.deephaven.engine.table.impl.dbarrays.ShortVectorColumnWrapper;
import io.deephaven.engine.table.impl.dbarrays.PrevShortVectorColumnWrapper;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.chunk.Attributes.Values;
import io.deephaven.engine.chunk.ObjectChunk;
import io.deephaven.engine.chunk.WritableChunk;
import io.deephaven.engine.chunk.WritableObjectChunk;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result short columns.
 */
public final class ShortAggregateColumnSource extends BaseAggregateColumnSource<ShortVector, Short> {

    ShortAggregateColumnSource(@NotNull final ColumnSource<Short> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource) {
        super(ShortVector.class, aggregatedSource, groupRowSetSource);
    }

    @Override
    public ShortVector get(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return new ShortVectorColumnWrapper(aggregatedSource, groupRowSetSource.get(rowKey));
    }

    @Override
    public ShortVector getPrev(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return new PrevShortVectorColumnWrapper(aggregatedSource, getPrevGroupRowSet(rowKey));
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> groupRowSetChunk = groupRowSetSource
                .getChunk(((AggregateFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<ShortVector, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new ShortVectorColumnWrapper(aggregatedSource, groupRowSetChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> groupRowSetPrevChunk = groupRowSetSource
                .getPrevChunk(((AggregateFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<ShortVector, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            final RowSet groupRowSetPrev = groupRowSetPrevChunk.get(di);
            final RowSet groupRowSetToUse = groupRowSetPrev.isTracking()
                    ? groupRowSetPrev.trackingCast().getPrevRowSet()
                    : groupRowSetPrev;
            typedDestination.set(di, new PrevShortVectorColumnWrapper(aggregatedSource, groupRowSetToUse));
        }
        typedDestination.setSize(size);
    }
}
