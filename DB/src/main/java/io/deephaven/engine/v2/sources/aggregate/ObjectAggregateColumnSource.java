package io.deephaven.engine.v2.sources.aggregate;

import io.deephaven.engine.tables.dbarrays.DbArray;
import io.deephaven.engine.v2.dbarrays.DbArrayColumnWrapper;
import io.deephaven.engine.v2.dbarrays.DbPrevArrayColumnWrapper;
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
 * {@link ColumnSource} implementation for aggregation result Object columns.
 */
public final class ObjectAggregateColumnSource<COMPONENT_TYPE> extends BaseAggregateColumnSource<DbArray, COMPONENT_TYPE> {

    ObjectAggregateColumnSource(@NotNull final ColumnSource<COMPONENT_TYPE> aggregatedSource,
                                @NotNull final ColumnSource<? extends RowSet> groupRowSetSource) {
        super(DbArray.class, aggregatedSource, groupRowSetSource);
    }

    @Override
    public final DbArray<COMPONENT_TYPE> get(final long rowKey) {
        if (rowKey == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbArrayColumnWrapper<>(aggregatedSource, groupRowSetSource.get(rowKey));
    }

    @Override
    public final DbArray<COMPONENT_TYPE> getPrev(final long rowKey) {
        if (rowKey == RowSet.NULL_ROW_KEY) {
            return null;
        }
        return new DbPrevArrayColumnWrapper<>(aggregatedSource, getPrevGroupRowSet(rowKey));
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> indexChunk = groupRowSetSource.getChunk(((AggregateFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbArray<COMPONENT_TYPE>, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbArrayColumnWrapper<>(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> groupRowSetPrevChunk = groupRowSetSource.getPrevChunk(((AggregateFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<DbArray<COMPONENT_TYPE>, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            final RowSet groupRowSetPrev = groupRowSetPrevChunk.get(di);
            final RowSet groupRowSetToUse = groupRowSetPrev.isTracking()
                    ? groupRowSetPrev.trackingCast().getPrevRowSet()
                    : groupRowSetPrev;
            typedDestination.set(di, new DbPrevArrayColumnWrapper<>(aggregatedSource, groupRowSetToUse));
        }
        typedDestination.setSize(size);
    }
}
