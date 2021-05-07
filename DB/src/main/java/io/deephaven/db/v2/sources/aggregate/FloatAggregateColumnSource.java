/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAggregateColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.aggregate;

import io.deephaven.db.tables.dbarrays.DbFloatArray;
import io.deephaven.db.v2.dbarrays.DbFloatArrayColumnWrapper;
import io.deephaven.db.v2.dbarrays.DbPrevFloatArrayColumnWrapper;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result float columns.
 */
public final class FloatAggregateColumnSource extends BaseAggregateColumnSource<DbFloatArray, Float> {

    FloatAggregateColumnSource(@NotNull final ColumnSource<Float> aggregatedSource,
                              @NotNull final ColumnSource<Index> indexSource) {
        super(DbFloatArray.class, aggregatedSource, indexSource);
    }

    @Override
    public final DbFloatArray get(final long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        return new DbFloatArrayColumnWrapper(aggregatedSource, indexSource.get(index));
    }

    @Override
    public final DbFloatArray getPrev(final long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        return new DbPrevFloatArrayColumnWrapper(aggregatedSource, indexSource.getPrev(index));
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ObjectChunk<Index, ? extends Values> indexChunk = indexSource.getChunk(((AggregateFillContext) context).indexGetContext, orderedKeys).asObjectChunk();
        final WritableObjectChunk<DbFloatArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = orderedKeys.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbFloatArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ObjectChunk<Index, ? extends Values> indexChunk = indexSource.getPrevChunk(((AggregateFillContext) context).indexGetContext, orderedKeys).asObjectChunk();
        final WritableObjectChunk<DbFloatArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = orderedKeys.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbPrevFloatArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }
}
