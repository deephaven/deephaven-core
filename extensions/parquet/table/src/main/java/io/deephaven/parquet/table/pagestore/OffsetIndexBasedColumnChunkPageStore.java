/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.pagestore;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.parquet.table.pagestore.topage.ToPage;
import io.deephaven.parquet.base.ColumnChunkReader;
import io.deephaven.parquet.base.ColumnPageReader;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.util.Arrays;

final class OffsetIndexBasedColumnChunkPageStore<ATTR extends Any> extends ColumnChunkPageStore<ATTR> {
    private final OffsetIndex offsetIndex;
    private final int numPages;
    /**
     * Set if first ({@link #numPages}-1) pages have equal number of rows
     */
    private boolean isPageSizeFixed;
    /**
     * Fixed number of rows per page, only valid if {@link #isPageSizeFixed} is true. Used to map from row index to page
     * number.
     */
    private final long fixedPageSize;
    private final Object[] objectsForSynchronizingPageAccess;
    private final ColumnPageReader[] columnPageReaders;
    private final WeakReference<PageCache.IntrusivePage<ATTR>>[] pages;

    OffsetIndexBasedColumnChunkPageStore(@NotNull final PageCache<ATTR> pageCache,
            @NotNull final ColumnChunkReader columnChunkReader,
            final long mask,
            @NotNull final ToPage<ATTR, ?> toPage) throws IOException {
        super(pageCache, columnChunkReader, mask, toPage);
        offsetIndex = columnChunkReader.getOffsetIndex();
        Assert.assertion(offsetIndex != null, "offsetIndex != null");
        numPages = offsetIndex.getPageCount();

        // noinspection unchecked
        pages = (WeakReference<PageCache.IntrusivePage<ATTR>>[]) new WeakReference[numPages];
        columnPageReaders = new ColumnPageReader[numPages];

        isPageSizeFixed = true;
        final long firstPageSize;
        if (numPages > 1) {
            firstPageSize = offsetIndex.getFirstRowIndex(1) - offsetIndex.getFirstRowIndex(0);
        } else {
            firstPageSize = numRows();
        }
        objectsForSynchronizingPageAccess = new Object[numPages];
        for (int i = 0; i < numPages; ++i) {
            objectsForSynchronizingPageAccess[i] = new Object();
            if (isPageSizeFixed && i > 0
                    && offsetIndex.getFirstRowIndex(i) - offsetIndex.getFirstRowIndex(i - 1) != firstPageSize) {
                isPageSizeFixed = false;
            }
        }
        if (isPageSizeFixed) {
            fixedPageSize = firstPageSize;
        } else {
            fixedPageSize = -1;
        }
    }

    /**
     * Binary search in offset index to find the page number that contains the row. Logic duplicated from
     * {@link Arrays#binarySearch(long[], long)} to use the offset index.
     */
    private static int findPageNumUsingOffsetIndex(final OffsetIndex offsetIndex, final long row) {
        int low = 0;
        int high = offsetIndex.getPageCount() - 1;

        while (low <= high) {
            final int mid = (low + high) >>> 1;
            final long midVal = offsetIndex.getFirstRowIndex(mid);

            if (midVal < row)
                low = mid + 1;
            else if (midVal > row)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1); // key not found.
    }

    private ChunkPage<ATTR> getPage(final int pageNum) {
        if (pageNum < 0 || pageNum >= numPages) {
            throw new IllegalArgumentException("pageNum " + pageNum + " is out of range [0, " + numPages + ")");
        }
        final PageCache.IntrusivePage<ATTR> page;
        WeakReference<PageCache.IntrusivePage<ATTR>> pageRef = pages[pageNum];
        if (pageRef == null || pageRef.get() == null) {
            synchronized (objectsForSynchronizingPageAccess[pageNum]) {
                // Make sure no one materialized this page as we waited for the lock
                pageRef = pages[pageNum];
                if (pageRef == null || pageRef.get() == null) {
                    if (columnPageReaders[pageNum] == null) {
                        columnPageReaders[pageNum] = columnPageReaderIterator.getPageReader(pageNum);
                    }
                    try {
                        page = new PageCache.IntrusivePage<>(toPage(offsetIndex.getFirstRowIndex(pageNum),
                                columnPageReaders[pageNum]));
                    } catch (final IOException except) {
                        throw new UncheckedIOException(except);
                    }
                    pages[pageNum] = new WeakReference<>(page);
                } else {
                    page = pageRef.get();
                }
            }
        } else {
            page = pageRef.get();
        }
        if (page == null) {
            throw new IllegalStateException("Page should not be null");
        }
        pageCache.touch(page);
        return page.getPage();
    }

    @NotNull
    @Override
    public ChunkPage<ATTR> getPageContaining(@NotNull final FillContext fillContext, long row) {
        row &= mask();
        Require.inRange(row, "row", numRows(), "numRows");

        int pageNum;
        if (isPageSizeFixed) {
            pageNum = (int) (row / fixedPageSize);
            if (pageNum >= numPages) {
                // This can happen if the last page is of different size from rest of the pages, assert this condition.
                // We have already checked that row is less than numRows.
                Assert.assertion(row >= offsetIndex.getFirstRowIndex(numPages - 1),
                        "row >= offsetIndex.getFirstRowIndex(numPages - 1)");
                pageNum = (numPages - 1);
            }
        } else {
            pageNum = findPageNumUsingOffsetIndex(offsetIndex, row);
            if (pageNum < 0) {
                pageNum = -2 - pageNum;
            }

        }
        return getPage(pageNum);
    }
}
