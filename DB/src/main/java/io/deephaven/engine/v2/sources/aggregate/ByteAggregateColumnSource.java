/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAggregateColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.aggregate;

import io.deephaven.engine.tables.dbarrays.DbByteArray;
import io.deephaven.engine.v2.dbarrays.DbByteArrayColumnWrapper;
import io.deephaven.engine.v2.dbarrays.DbPrevByteArrayColumnWrapper;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.ObjectChunk;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;
import io.deephaven.engine.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.v2.utils.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result byte columns.
 */
public final class ByteAggregateColumnSource extends BaseAggregateColumnSource<DbByteArray, Byte> {

    ByteAggregateColumnSource(@NotNull final ColumnSource<Byte> aggregatedSource,
                              @NotNull final ColumnSource<RowSet> indexSource) {
        super(DbByteArray.class, aggregatedSource, indexSource);
    }

    @Override
    public final DbByteArray get(final long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbByteArrayColumnWrapper(aggregatedSource, indexSource.get(index));
    }

    @Override
    public final DbByteArray getPrev(final long index) {
        if (index == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbPrevByteArrayColumnWrapper(aggregatedSource, indexSource.getPrev(index));
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> indexChunk = indexSource.getChunk(((AggregateFillContext) context).indexGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbByteArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbByteArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> indexChunk = indexSource.getPrevChunk(((AggregateFillContext) context).indexGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbByteArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbPrevByteArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }
}
