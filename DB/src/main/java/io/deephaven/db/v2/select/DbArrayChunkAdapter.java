package io.deephaven.db.v2.select;

import io.deephaven.db.tables.dbarrays.DbArrayBase;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

/**
 * This class wraps an inner ChunkSource holding a DbArray. The purpose of doing so is to apply
 * DbArray#getDirect to the underlying DbArray values returned by the underlying ChunkSource. This
 * is the strategy for implementing this class: makeGetContext() - doesn't need to change. The
 * default implementation in our parent, namely DefaultChunkSource#makeGetContext, already does the
 * right thing. getChunk() - likewise. makeFillContext() - We don't need to add anything to the
 * "inner" context, so we just delegate to inner and return its context fillContext() - We first let
 * the inner fill the chunk, then we overwrite each value (where non-null) with the result of
 * DbArrayBase#getDirect() invoked on that value.
 */
public class DbArrayChunkAdapter<ATTR extends Attributes.Any> implements DefaultChunkSource<ATTR> {
    private final ChunkSource<ATTR> underlying;

    public DbArrayChunkAdapter(ChunkSource<ATTR> underlying) {
        this.underlying = underlying;
    }

    @Override
    public ChunkType getChunkType() {
        return underlying.getChunkType();
    }

    @Override
    public void fillChunk(@NotNull FillContext context,
        @NotNull WritableChunk<? super ATTR> destination,
        @NotNull OrderedKeys orderedKeys) {
        // First let the underlying ChunkSource fill the chunk, and then we overwrite the values
        // with the result
        // of applying DbArray#getDirect to each element.
        underlying.fillChunk(context, destination, orderedKeys);
        final WritableObjectChunk<DbArrayBase, ? super ATTR> typedDest =
            destination.asWritableObjectChunk();
        for (int ii = 0; ii < destination.size(); ++ii) {
            final DbArrayBase dbArray = typedDest.get(ii);
            if (dbArray != null) {
                final DbArrayBase direct = dbArray.getDirect();
                typedDest.set(ii, direct);
            }
        }
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return underlying.makeFillContext(chunkCapacity, sharedContext);
    }
}
