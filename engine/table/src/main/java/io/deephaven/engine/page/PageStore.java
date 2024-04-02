//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import org.jetbrains.annotations.Nullable;

/**
 * PageStores are a collection of non-overlapping {@link Page Pages}, providing a single {@link PagingChunkSource}
 * across all the pages. PageStores are responsible for mapping row keys to pages. PageStores may themselves be Pages
 * nested within other PageStores.
 */
public interface PageStore<ATTR extends Any, INNER_ATTR extends ATTR, PAGE extends Page<INNER_ATTR>>
        extends PagingChunkSource<ATTR>, DefaultChunkSource.SupportsContiguousGet<ATTR> {

    /**
     * @param fillContext The fill context to use; may be {@code null} if the calling code does not have a fill context
     * @param rowKey The row key to get the page for
     * @return The page containing {@code rowKey}, after applying {@link #mask()}.
     */
    @NotNull
    PAGE getPageContaining(@Nullable FillContext fillContext, long rowKey);

    @Override
    default Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
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
    default void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
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
    default void fillChunkAppend(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence.Iterator rowSequenceIterator) {
        long firstKey = rowSequenceIterator.peekNextKey();
        final long pageStoreMaxKey = maxRow(firstKey);

        do {
            final PAGE page = getPageContaining(context, firstKey);
            page.fillChunkAppend(context, destination, rowSequenceIterator);
        } while (rowSequenceIterator.hasMore() &&
                (firstKey = rowSequenceIterator.peekNextKey()) <= pageStoreMaxKey);
    }

    /**
     * This is a helper which is the same as a call to {@link #fillChunkAppend}, except that some of the initial work
     * has already been done for the first call to
     * {@link Page#fillChunkAppend(FillContext, WritableChunk, RowSequence.Iterator)} which we don't want to repeat.
     */
    // Should be private
    @FinalDefault
    default void doFillChunkAppend(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence,
            @NotNull final Page<INNER_ATTR> page) {
        destination.setSize(0);
        try (final RowSequence.Iterator rowSequenceIterator = rowSequence.getRowSequenceIterator()) {
            page.fillChunkAppend(context, destination, rowSequenceIterator);
            fillChunkAppend(context, destination, rowSequenceIterator);
        }
    }
}
