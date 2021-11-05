/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAggregateColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.aggregate;

import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.tables.dbarrays.DbByteArray;
import io.deephaven.engine.v2.dbarrays.DbByteArrayColumnWrapper;
import io.deephaven.engine.v2.dbarrays.DbPrevByteArrayColumnWrapper;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.chunk.Attributes.Values;
import io.deephaven.engine.v2.sources.chunk.ObjectChunk;
import io.deephaven.engine.v2.sources.chunk.WritableChunk;
import io.deephaven.engine.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.engine.v2.utils.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result byte columns.
 */
public final class ByteAggregateColumnSource extends BaseAggregateColumnSource<DbByteArray, Byte> {

    ByteAggregateColumnSource(@NotNull final ColumnSource<Byte> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource) {
        super(DbByteArray.class, aggregatedSource, groupRowSetSource);
    }

    @Override
    public DbByteArray get(final long rowKey) {
        if (rowKey == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbByteArrayColumnWrapper(aggregatedSource, groupRowSetSource.get(rowKey));
    }

    @Override
    public DbByteArray getPrev(final long rowKey) {
        if (rowKey == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbPrevByteArrayColumnWrapper(aggregatedSource, getPrevGroupRowSet(rowKey));
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> groupRowSetChunk = groupRowSetSource
                .getChunk(((AggregateFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbByteArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbByteArrayColumnWrapper(aggregatedSource, groupRowSetChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> groupRowSetPrevChunk = groupRowSetSource
                .getPrevChunk(((AggregateFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbByteArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            final RowSet groupRowSetPrev = groupRowSetPrevChunk.get(di);
            final RowSet groupRowSetToUse = groupRowSetPrev.isTracking()
                    ? groupRowSetPrev.trackingCast().getPrevRowSet()
                    : groupRowSetPrev;
            typedDestination.set(di, new DbPrevByteArrayColumnWrapper(aggregatedSource, groupRowSetToUse));
        }
        typedDestination.setSize(size);
    }
}
