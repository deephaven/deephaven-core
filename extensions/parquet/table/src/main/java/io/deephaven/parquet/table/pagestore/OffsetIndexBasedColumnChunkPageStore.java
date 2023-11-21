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
import java.util.concurrent.atomic.AtomicReferenceArray;

final class OffsetIndexBasedColumnChunkPageStore<ATTR extends Any> extends ColumnChunkPageStore<ATTR> {
    private static final long PAGE_SIZE_NOT_FIXED = -1;

    private final OffsetIndex offsetIndex;
    private final int numPages;
    /**
     * Fixed number of rows per page. Set as positive value if first ({@link #numPages}-1) pages have equal number of
     * rows, else equal to {@value #PAGE_SIZE_NOT_FIXED}. We don't care about the size of the last page, because we can
     * assume all pages to be of the same size and calculate the page number as
     * {@code row_index / fixed_page_size -> page_number}. If it is greater than {@link #numPages}, we can assume the
     * row to be coming from last page.
     */
    private final long fixedPageSize;

    private final class PageState {
        WeakReference<PageCache.IntrusivePage<ATTR>> pageRef;

        PageState() {
            // Initialized when used for the first time
            pageRef = null;
        }
    }

    private final AtomicReferenceArray<PageState> pageStates;
    private final ColumnChunkReader.ColumnPageDirectAccessor columnPageDirectAccessor;

    OffsetIndexBasedColumnChunkPageStore(@NotNull final PageCache<ATTR> pageCache,
            @NotNull final ColumnChunkReader columnChunkReader,
            final long mask,
            @NotNull final ToPage<ATTR, ?> toPage) throws IOException {
        super(pageCache, columnChunkReader, mask, toPage);
        offsetIndex = columnChunkReader.getOffsetIndex();
        Assert.assertion(offsetIndex != null, "offsetIndex != null");
        numPages = offsetIndex.getPageCount();
        Assert.assertion(numPages > 0, "numPages > 0");
        pageStates = new AtomicReferenceArray<>(numPages);
        columnPageDirectAccessor = columnChunkReader.getPageAccessor();

        if (numPages == 1) {
            fixedPageSize = numRows();
            return;
        }
        boolean isPageSizeFixed = true;
        final long firstPageSize = offsetIndex.getFirstRowIndex(1) - offsetIndex.getFirstRowIndex(0);
        for (int i = 2; i < numPages; ++i) {
            if (offsetIndex.getFirstRowIndex(i) - offsetIndex.getFirstRowIndex(i - 1) != firstPageSize) {
                isPageSizeFixed = false;
                break;
            }
        }
        fixedPageSize = isPageSizeFixed ? firstPageSize : PAGE_SIZE_NOT_FIXED;
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
                return mid; // 'row' is the first row of page
        }
        return (low - 1); // 'row' is somewhere in the middle of page
    }

    private ChunkPage<ATTR> getPage(final int pageNum) {
        if (pageNum < 0 || pageNum >= numPages) {
            throw new IllegalArgumentException("pageNum " + pageNum + " is out of range [0, " + numPages + ")");
        }
        PageCache.IntrusivePage<ATTR> page;
        PageState pageState = pageStates.get(pageNum);
        if (pageState == null) {
            pageState = pageStates.updateAndGet(pageNum, p -> p == null ? new PageState() : p);
        }
        if (pageState.pageRef == null || (page = pageState.pageRef.get()) == null) {
            synchronized (pageState) {
                // Make sure no one materialized this page as we waited for the lock
                if (pageState.pageRef == null || (page = pageState.pageRef.get()) == null) {
                    final ColumnPageReader reader = columnPageDirectAccessor.getPageReader(pageNum);
                    try {
                        page = new PageCache.IntrusivePage<>(toPage(offsetIndex.getFirstRowIndex(pageNum), reader));
                    } catch (final IOException except) {
                        throw new UncheckedIOException(except);
                    }
                    pageState.pageRef = new WeakReference<>(page);
                }
            }
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
        if (fixedPageSize == PAGE_SIZE_NOT_FIXED) {
            pageNum = findPageNumUsingOffsetIndex(offsetIndex, row);
        } else {
            pageNum = (int) (row / fixedPageSize);
            if (pageNum >= numPages) {
                // This can happen if the last page is larger than rest of the pages, which are all the same size.
                // We have already checked that row is less than numRows.
                Assert.assertion(row >= offsetIndex.getFirstRowIndex(numPages - 1),
                        "row >= offsetIndex.getFirstRowIndex(numPages - 1)");
                pageNum = (numPages - 1);
            }
        }
        return getPage(pageNum);
    }
}
