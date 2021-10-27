/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAggregateColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.aggregate;

import io.deephaven.engine.tables.dbarrays.DbDoubleArray;
import io.deephaven.engine.v2.dbarrays.DbDoubleArrayColumnWrapper;
import io.deephaven.engine.v2.dbarrays.DbPrevDoubleArrayColumnWrapper;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.ObjectChunk;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;
import io.deephaven.engine.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.TrackingRowSet;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result double columns.
 */
public final class DoubleAggregateColumnSource extends BaseAggregateColumnSource<DbDoubleArray, Double> {

    DoubleAggregateColumnSource(@NotNull final ColumnSource<Double> aggregatedSource,
                              @NotNull final ColumnSource<? extends TrackingRowSet> indexSource) {
        super(DbDoubleArray.class, aggregatedSource, indexSource);
    }

    @Override
    public final DbDoubleArray get(final long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbDoubleArrayColumnWrapper(aggregatedSource, indexSource.get(index));
    }

    @Override
    public final DbDoubleArray getPrev(final long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbPrevDoubleArrayColumnWrapper(aggregatedSource, indexSource.getPrev(index));
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> indexChunk = indexSource.getChunk(((AggregateFillContext) context).indexGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbDoubleArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbDoubleArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<TrackingRowSet, ? extends Values> indexChunk = indexSource.getPrevChunk(((AggregateFillContext) context).indexGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbDoubleArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbPrevDoubleArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }
}
