package io.deephaven.db.v2.sources.aggregate;

import io.deephaven.db.tables.dbarrays.DbCharArray;
import io.deephaven.db.v2.dbarrays.DbCharArrayColumnWrapper;
import io.deephaven.db.v2.dbarrays.DbPrevCharArrayColumnWrapper;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * {@link ColumnSource} implementation for aggregation result char columns.
 */
public final class CharAggregateColumnSource extends BaseAggregateColumnSource<DbCharArray, Character> {

    CharAggregateColumnSource(@NotNull final ColumnSource<Character> aggregatedSource,
                              @NotNull final ColumnSource<Index> indexSource) {
        super(DbCharArray.class, aggregatedSource, indexSource);
    }

    @Override
    public final DbCharArray get(final long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        return new DbCharArrayColumnWrapper(aggregatedSource, indexSource.get(index));
    }

    @Override
    public final DbCharArray getPrev(final long index) {
        if (index == Index.NULL_KEY) {
            return null;
        }
        return new DbPrevCharArrayColumnWrapper(aggregatedSource, indexSource.getPrev(index));
    }

    @Override
    public final void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ObjectChunk<Index, ? extends Values> indexChunk = indexSource.getChunk(((AggregateFillContext) context).indexGetContext, orderedKeys).asObjectChunk();
        final WritableObjectChunk<DbCharArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = orderedKeys.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbCharArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }

    @Override
    public final void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final ObjectChunk<Index, ? extends Values> indexChunk = indexSource.getPrevChunk(((AggregateFillContext) context).indexGetContext, orderedKeys).asObjectChunk();
        final WritableObjectChunk<DbCharArray, ? super Values> typedDestination = destination.asWritableObjectChunk();
        final int size = orderedKeys.intSize();
        for (int di = 0; di < size; ++di) {
            typedDestination.set(di, new DbPrevCharArrayColumnWrapper(aggregatedSource, indexChunk.get(di)));
        }
        typedDestination.setSize(size);
    }
}
