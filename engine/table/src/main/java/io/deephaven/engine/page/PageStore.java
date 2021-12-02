package io.deephaven.engine.page;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 * PageStores are a collection of non-overlapping pages, which provides a single {@link ChunkSource} interface across
 * all the pages.
 */
public interface PageStore<ATTR extends Any, INNER_ATTR extends ATTR, PAGE extends Page<INNER_ATTR>>
        extends PagingChunkSource<ATTR>, DefaultChunkSource.SupportsContiguousGet<ATTR> {

    /**
     * @return The page containing row, after applying {@link #mask()}.
     */
    @NotNull
    PAGE getPageContaining(FillContext fillContext, long row);

    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.size() == 0) {
            return getChunkType().getEmptyChunk();
        }

        final long firstKey = rowSequence.firstRowKey();
        final FillContext fillContext = DefaultGetContext.getFillContext(context);
        final PAGE page = getPageContaining(fillContext, firstKey);
        final long pageMaxRow = page.maxRow(firstKey);

        if (rowSequence.lastRowKey() <= pageMaxRow) {
            return page.getChunk(context, rowSequence);
        } else {
            final WritableChunk<ATTR> destination = DefaultGetContext.getWritableChunk(context);
            doFillChunkAppend(fillContext, destination, rowSequence, page);
            return destination;
        }
    }

    @Override
    @NotNull
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, final long firstKey, final long lastKey) {
        final FillContext fillContext = DefaultGetContext.getFillContext(context);
        final PAGE page = getPageContaining(fillContext, firstKey);
        final long pageMaxRow = page.maxRow(firstKey);

        if (lastKey <= pageMaxRow) {
            return page.getChunk(context, firstKey, lastKey);
        } else {
            try (final RowSequence rowSequence = RowSequenceFactory.forRange(firstKey, lastKey)) {
                final WritableChunk<ATTR> destination = DefaultGetContext.getWritableChunk(context);
                doFillChunkAppend(fillContext, destination, rowSequence, page);
                return destination;
            }
        }
    }

    @Override
    default void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence) {
        if (rowSequence.size() == 0) {
            return;
        }

        final long firstKey = rowSequence.firstRowKey();
        final PAGE page = getPageContaining(context, firstKey);
        final long pageMaxRow = page.maxRow(firstKey);

        if (rowSequence.lastRowKey() <= pageMaxRow) {
            page.fillChunk(context, destination, rowSequence);
        } else {
            doFillChunkAppend(context, destination, rowSequence, page);
        }
    }

    @Override
    default void fillChunkAppend(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence.Iterator RowSequenceIterator) {
        long firstKey = RowSequenceIterator.peekNextKey();
        final long pageStoreMaxKey = maxRow(firstKey);

        do {
            final PAGE page = getPageContaining(context, firstKey);
            page.fillChunkAppend(context, destination, RowSequenceIterator);
        } while (RowSequenceIterator.hasMore() &&
                (firstKey = RowSequenceIterator.peekNextKey()) <= pageStoreMaxKey);
    }

    /**
     * This is a helper which is the same as a call to {@link #fillChunkAppend}, except that some of the initial work
     * has already been done for the first call to
     * {@link Page#fillChunkAppend(FillContext, WritableChunk, RowSequence.Iterator)} which we don't want to repeat.
     */
    // Should be private
    @FinalDefault
    default void doFillChunkAppend(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence, @NotNull final Page<INNER_ATTR> page) {
        destination.setSize(0);
        try (final RowSequence.Iterator RowSequenceIterator = rowSequence.getRowSequenceIterator()) {
            page.fillChunkAppend(context, destination, RowSequenceIterator);
            fillChunkAppend(context, destination, RowSequenceIterator);
        }
    }
}
