/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.chunk;

import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

public interface DefaultChunkSource<ATTR extends Attributes.Any> extends ChunkSource<ATTR> {

    @Override
    default GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new DefaultGetContext<>(this, chunkCapacity, sharedContext);
    }

    @Override
    default FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return DEFAULT_FILL_INSTANCE;
    }

    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, @NotNull final OrderedKeys orderedKeys) {
        return getChunkByFilling(context, orderedKeys);
    }

    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, long firstKey, long lastKey) {
        try (OrderedKeys orderedKeys = OrderedKeys.forRange(firstKey, lastKey)) {
            return getChunk(context, orderedKeys);
        }
    }

    @FinalDefault
    default Chunk<ATTR> getChunkByFilling(@NotNull final GetContext context, @NotNull final OrderedKeys orderedKeys) {
        WritableChunk<ATTR> chunk = DefaultGetContext.getWritableChunk(context);
        fillChunk(DefaultGetContext.getFillContext(context), chunk, orderedKeys);
        return chunk;
    }

    interface WithPrev<ATTR extends Attributes.Any> extends DefaultChunkSource<ATTR>, ChunkSource.WithPrev<ATTR> {

        @Override
        default Chunk<? extends ATTR> getPrevChunk(@NotNull final GetContext context,
                @NotNull final OrderedKeys orderedKeys) {
            return getPrevChunkByFilling(context, orderedKeys);
        }

        @Override
        default Chunk<? extends ATTR> getPrevChunk(@NotNull final GetContext context, long firstKey, long lastKey) {
            try (OrderedKeys orderedKeys = OrderedKeys.forRange(firstKey, lastKey)) {
                return getPrevChunk(context, orderedKeys);
            }
        }

        @FinalDefault
        default Chunk<ATTR> getPrevChunkByFilling(@NotNull final GetContext context,
                @NotNull final OrderedKeys orderedKeys) {
            WritableChunk<ATTR> chunk = DefaultGetContext.getWritableChunk(context);
            fillPrevChunk(DefaultGetContext.getFillContext(context), chunk, orderedKeys);
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
                public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
                    return chunkSource.getPrevChunk(context, orderedKeys);
                }

                @Override
                public Chunk<? extends ATTR> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
                    return chunkSource.getPrevChunk(context, firstKey, lastKey);
                }

                @Override
                public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination,
                        @NotNull OrderedKeys orderedKeys) {
                    chunkSource.fillPrevChunk(context, destination, orderedKeys);
                }

                @Override
                public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
                    return chunkSource.makeGetContext(chunkCapacity, sharedContext);
                }

                @Override
                public GetContext makeGetContext(int chunkCapacity) {
                    return chunkSource.makeGetContext(chunkCapacity);
                }

                @Override
                public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
                    return chunkSource.makeFillContext(chunkCapacity, sharedContext);
                }

                @Override
                public FillContext makeFillContext(int chunkCapacity) {
                    return chunkSource.makeFillContext(chunkCapacity);
                }
            };
        }
    }

    /**
     * An alternative set of defaults which may typically be used by {@link ChunkSource}s which support a get method
     * which only works for contiguous ranges. They should just implement {@link #getChunk(GetContext, long, long)}.
     */
    interface SupportsContiguousGet<ATTR extends Attributes.Any> extends DefaultChunkSource<ATTR> {
        @Override
        default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context,
                @NotNull final OrderedKeys orderedKeys) {
            return orderedKeys.isContiguous() ? getChunk(context, orderedKeys.firstKey(), orderedKeys.lastKey())
                    : getChunkByFilling(context, orderedKeys);
        }

        @Override
        Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, long firstKey, long lastKey);
    }
}
