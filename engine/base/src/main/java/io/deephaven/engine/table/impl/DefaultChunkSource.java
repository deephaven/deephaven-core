/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

public interface DefaultChunkSource<ATTR extends Any> extends ChunkSource<ATTR> {

    @Override
    default GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new DefaultGetContext<>(this, chunkCapacity, sharedContext);
    }

    @Override
    default FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return DEFAULT_FILL_INSTANCE;
    }

    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        return getChunkByFilling(context, rowSequence);
    }

    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, long firstKey, long lastKey) {
        try (RowSequence rowSequence = RowSequenceFactory.forRange(firstKey, lastKey)) {
            return getChunk(context, rowSequence);
        }
    }

    @FinalDefault
    default Chunk<ATTR> getChunkByFilling(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        WritableChunk<ATTR> chunk = DefaultGetContext.getWritableChunk(context);
        fillChunk(DefaultGetContext.getFillContext(context), chunk, rowSequence);
        return chunk;
    }

    interface WithPrev<ATTR extends Any> extends DefaultChunkSource<ATTR>, ChunkSource.WithPrev<ATTR> {

        @Override
        default Chunk<? extends ATTR> getPrevChunk(@NotNull final GetContext context,
                @NotNull final RowSequence rowSequence) {
            return getPrevChunkByFilling(context, rowSequence);
        }

        @Override
        default Chunk<? extends ATTR> getPrevChunk(@NotNull final GetContext context, long firstKey, long lastKey) {
            try (RowSequence rowSequence = RowSequenceFactory.forRange(firstKey, lastKey)) {
                return getPrevChunk(context, rowSequence);
            }
        }

        @FinalDefault
        default Chunk<ATTR> getPrevChunkByFilling(@NotNull final GetContext context,
                @NotNull final RowSequence rowSequence) {
            WritableChunk<ATTR> chunk = DefaultGetContext.getWritableChunk(context);
            fillPrevChunk(DefaultGetContext.getFillContext(context), chunk, rowSequence);
            return chunk;
        }

        @Override
        default ChunkSource<ATTR> getPrevSource() {
            final ChunkSource.WithPrev<ATTR> chunkSource = this;

            return new ChunkSource<ATTR>() {
                @Override
                public ChunkType getChunkType() {
                    return chunkSource.getChunkType();
                }

                @Override
                public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
                    return chunkSource.getPrevChunk(context, rowSequence);
                }

                @Override
                public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
                    return chunkSource.getPrevChunk(context, firstKey, lastKey);
                }

                @Override
                public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
                        @NotNull RowSequence rowSequence) {
                    chunkSource.fillPrevChunk(context, destination, rowSequence);
                }

                @Override
                public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
                    return chunkSource.makeGetContext(chunkCapacity, sharedContext);
                }

                @Override
                public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
                    return chunkSource.makeFillContext(chunkCapacity, sharedContext);
                }
            };
        }
    }

    /**
     * An alternative set of defaults which may typically be used by {@link ChunkSource}s which support a get method
     * which only works for contiguous ranges. They should just implement {@link #getChunk(GetContext, long, long)}.
     */
    interface SupportsContiguousGet<ATTR extends Any> extends DefaultChunkSource<ATTR> {
        @Override
        default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context,
                @NotNull final RowSequence rowSequence) {
            return rowSequence.isContiguous() ? getChunk(context, rowSequence.firstRowKey(), rowSequence.lastRowKey())
                    : getChunkByFilling(context, rowSequence);
        }

        @Override
        Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, long firstKey, long lastKey);
    }
}
