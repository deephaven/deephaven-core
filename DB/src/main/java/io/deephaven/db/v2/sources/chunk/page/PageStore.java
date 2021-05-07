package io.deephaven.db.v2.sources.chunk.page;

import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * PageStores are a collection of non-overlapping pages, which provides a single
 * {@link ChunkSource} interface across all the pages.
 */

public interface PageStore<ATTR extends Attributes.Any, INNER_ATTR extends ATTR, PAGE extends Page<INNER_ATTR>>
        extends PagingChunkSource<ATTR>, DefaultChunkSource.SupportsContiguousGet<ATTR> {

    /**
     * @return The page containing row, after applying {@link #mask()}.
     */
    @NotNull
    PAGE getPageContaining(FillContext fillContext, long row);

    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        if (orderedKeys.size() == 0) {
            return getChunkType().getEmptyChunk();
        }

        long firstKey = orderedKeys.firstKey();
        FillContext fillContext = DefaultGetContext.getFillContext(context);
        PAGE page = getPageContaining(fillContext, firstKey);
        long pageMaxRow = page.maxRow(firstKey);

        if (orderedKeys.lastKey() <= pageMaxRow) {
            return page.getChunk(context, orderedKeys);
        } else {
            WritableChunk<ATTR> destination = DefaultGetContext.getWritableChunk(context);
            doFillChunkAppend(fillContext, destination, orderedKeys, page);
            return destination;
        }
    }

    @Override
    @NotNull
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, long firstKey, long lastKey) {
        FillContext fillContext = DefaultGetContext.getFillContext(context);
        PAGE page = getPageContaining(fillContext, firstKey);
        long pageMaxRow = page.maxRow(firstKey);

        if (lastKey <= pageMaxRow) {
            return page.getChunk(context, firstKey, lastKey);
        } else {
            try (OrderedKeys orderedKeys = OrderedKeys.forRange(firstKey, lastKey)) {
                WritableChunk<ATTR> destination = DefaultGetContext.getWritableChunk(context);
                doFillChunkAppend(fillContext, destination, orderedKeys, page);
                return destination;
            }
        }
    }

    @Override
    default void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys orderedKeys) {
        if (orderedKeys.size() == 0) {
            return;
        }

        long firstKey = orderedKeys.firstKey();
        PAGE page = getPageContaining(context, firstKey);
        long pageMaxRow = page.maxRow(firstKey);

        if (orderedKeys.lastKey() <= pageMaxRow) {
            page.fillChunk(context, destination, orderedKeys);
        } else {
            doFillChunkAppend(context, destination, orderedKeys, page);
        }
    }


    @Override
    default void fillChunkAppend(@NotNull FillContext context, @NotNull WritableChunk<? super ATTR> destination, @NotNull OrderedKeys.Iterator orderedKeysIterator) {
        long firstKey = orderedKeysIterator.peekNextKey();
        long pageStoreMaxKey = maxRow(firstKey);

        do {
            PAGE page = getPageContaining(context, firstKey);
            page.fillChunkAppend(context, destination, orderedKeysIterator);
        } while (orderedKeysIterator.hasMore() &&
                (firstKey = orderedKeysIterator.peekNextKey()) <= pageStoreMaxKey);
    }

    /**
     * This is a helper which is the same as a call to {@link #fillChunkAppend}, except that some of the initial
     * work has already been done for the first call to
     * {@link Page#fillChunkAppend(FillContext, WritableChunk, OrderedKeys.Iterator)} which we don't want to repeat.
     */
    // Should be private
    @FinalDefault
    default void doFillChunkAppend(final @NotNull FillContext context, final @NotNull WritableChunk<? super ATTR> destination, OrderedKeys orderedKeys, @NotNull Page<INNER_ATTR> page) {
        destination.setSize(0);
        try (OrderedKeys.Iterator orderedKeysIterator = orderedKeys.getOrderedKeysIterator()) {
            page.fillChunkAppend(context, destination, orderedKeysIterator);
            fillChunkAppend(context, destination, orderedKeysIterator);
        }
    }
}
