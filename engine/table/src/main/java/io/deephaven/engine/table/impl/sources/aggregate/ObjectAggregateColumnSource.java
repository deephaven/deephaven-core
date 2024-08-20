//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.aggregate;

import io.deephaven.vector.ObjectVector;
import io.deephaven.engine.table.vectors.ObjectVectorColumnWrapper;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result Object columns.
 */
public final class ObjectAggregateColumnSource<COMPONENT_TYPE>
        extends BaseAggregateColumnSource<ObjectVector<COMPONENT_TYPE>, COMPONENT_TYPE> {

    ObjectAggregateColumnSource(@NotNull final ColumnSource<COMPONENT_TYPE> aggregatedSource,
            @NotNull final ColumnSource<? extends RowSet> groupRowSetSource) {
        // noinspection unchecked,rawtypes
        super((Class<ObjectVector<COMPONENT_TYPE>>) (Class) ObjectVector.class, aggregatedSource, groupRowSetSource);
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> get(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return new ObjectVectorColumnWrapper<>(aggregatedSource, groupRowSetSource.get(rowKey));
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> getPrev(final long rowKey) {
        if (rowKey == RowSequence.NULL_ROW_KEY) {
            return null;
        }
        return new ObjectVectorColumnWrapper<>(aggregatedSourcePrev, getPrevGroupRowSet(rowKey));
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> indexChunk = groupRowSetSource
                .getChunk(((AggregateFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<ObjectVector<COMPONENT_TYPE>, ? super Values> typedDestination =
                destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new ObjectVectorColumnWrapper<>(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence) {
        final ObjectChunk<RowSet, ? extends Values> groupRowSetPrevChunk = groupRowSetSource
                .getPrevChunk(((AggregateFillContext) context).groupRowSetGetContext, rowSequence).asObjectChunk();
        final WritableObjectChunk<ObjectVector<COMPONENT_TYPE>, ? super Values> typedDestination =
                destination.asWritableObjectChunk();
        final int size = rowSequence.intSize();
        for (int di = 0; di < size; ++di) {
            final RowSet groupRowSetPrev = groupRowSetPrevChunk.get(di);
            final RowSet groupRowSetToUse = groupRowSetPrev.isTracking()
                    ? groupRowSetPrev.trackingCast().prev()
                    : groupRowSetPrev;
            typedDestination.set(di, new ObjectVectorColumnWrapper<>(aggregatedSourcePrev, groupRowSetToUse));
        }
        typedDestination.setSize(size);
    }
}
