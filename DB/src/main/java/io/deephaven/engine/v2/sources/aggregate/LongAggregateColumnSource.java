/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAggregateColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.aggregate;

import io.deephaven.engine.tables.dbarrays.DbLongArray;
import io.deephaven.engine.v2.dbarrays.DbLongArrayColumnWrapper;
import io.deephaven.engine.v2.dbarrays.DbPrevLongArrayColumnWrapper;
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
 * {@link ColumnSource} implementation for aggregation result long columns.
 */
public final class LongAggregateColumnSource extends BaseAggregateColumnSource<DbLongArray, Long> {

    LongAggregateColumnSource(@NotNull final ColumnSource<Long> aggregatedSource,
                              @NotNull final ColumnSource<? extends TrackingRowSet> indexSource) {
        super(DbLongArray.class, aggregatedSource, indexSource);
    }

    @Override
    public final DbLongArray get(final long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbLongArrayColumnWrapper(aggregatedSource, indexSource.get(index));
    }

    @Override
    public final DbLongArray getPrev(final long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbPrevLongArrayColumnWrapper(aggregatedSource, indexSource.getPrev(index));
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> indexChunk = indexSource.getChunk(((AggregateFillContext) context).indexGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbLongArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbLongArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<TrackingRowSet, ? extends Values> indexChunk = indexSource.getPrevChunk(((AggregateFillContext) context).indexGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbLongArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbPrevLongArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }
}
