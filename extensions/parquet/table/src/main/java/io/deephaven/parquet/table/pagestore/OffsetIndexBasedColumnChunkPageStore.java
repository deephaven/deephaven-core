//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.parquet.table.pagestore.PageCache.IntrusivePage;
import io.deephaven.parquet.table.pagestore.topage.ToPage;
import io.deephaven.parquet.base.ColumnChunkReader;
import io.deephaven.parquet.base.ColumnPageReader;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelContext.ContextHolder;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A {@link ColumnChunkPageStore} that uses {@link OffsetIndex} to find the page containing a row.
 */
final class OffsetIndexBasedColumnChunkPageStore<ATTR extends Any> extends ColumnChunkPageStore<ATTR> {
    private static final long PAGE_SIZE_NOT_FIXED = -1;
    private static final int NUM_PAGES_NOT_INITIALIZED = -1;

    private static final class PageState<ATTR extends Any> {
        private volatile WeakReference<PageCache.IntrusivePage<ATTR>> pageRef;
        private final AtomicInteger sparseReads = new AtomicInteger();

        PageState() {
            pageRef = null; // Initialized when used for the first time
        }
    }

    private volatile boolean isInitialized; // This class is initialized when reading the first page
    private OffsetIndex offsetIndex;
    private int numPages;
    /**
     * Fixed number of rows per page. Set as positive value if first ({@link #numPages}-1) pages have equal number of
     * rows, else equal to {@value #PAGE_SIZE_NOT_FIXED}. We cannot find the number of rows in the last page size from
     * offset index, because it only has the first row index of each page. And we don't want to materialize any pages.
     * So as a workaround, in case first ({@link #numPages}-1) pages have equal size, we can assume all pages to be of
     * the same size and calculate the page number as {@code row_index / fixed_page_size -> page_number}. If it is
     * greater than {@link #numPages}, we will infer that the row is coming from last page.
     */
    private long fixedPageSize;
    private AtomicReferenceArray<PageState<ATTR>> pageStates;
    private ColumnChunkReader.ColumnPageDirectAccessor columnPageDirectAccessor;

    OffsetIndexBasedColumnChunkPageStore(
            @NotNull final PageCache<ATTR> pageCache,
            @NotNull final ColumnChunkReader columnChunkReader,
            final long mask,
            @NotNull final ToPage<ATTR, ?> toPage) throws IOException {
        super(pageCache, columnChunkReader, mask, toPage);
        numPages = NUM_PAGES_NOT_INITIALIZED;
        fixedPageSize = PAGE_SIZE_NOT_FIXED;
    }

    private void ensureInitialized(@NotNull final SeekableChannelContext channelContext) {
        if (isInitialized) {
            return;
        }
        synchronized (this) {
            if (isInitialized) {
                return;
            }
            try (final ContextHolder holder = SeekableChannelContext.ensureContext(
                    columnChunkReader.getChannelsProvider(), channelContext)) {
                offsetIndex = columnChunkReader.getOffsetIndex(holder.get());
            }
            Assert.neqNull(offsetIndex, "offsetIndex");
            numPages = offsetIndex.getPageCount();
            Assert.gtZero(numPages, "numPages");
            pageStates = new AtomicReferenceArray<>(numPages);
            columnPageDirectAccessor =
                    columnChunkReader.getPageAccessor(offsetIndex, toPage.getPageMaterializerFactory());

            if (numPages == 1) {
                fixedPageSize = numRows();
            } else {
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
            isInitialized = true;
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
            if (midVal < row) {
                low = mid + 1;
            } else if (midVal > row) {
                high = mid - 1;
            } else {
                return mid; // 'row' is the first row of page
            }
        }
        return (low - 1); // 'row' is somewhere in the middle of page
    }

    @Override
    @NotNull
    ChunkPage<ATTR> getPage(@NotNull final SeekableChannelContext channelContext, final int pageNum) {
        final PageState<ATTR> pageState = pageState(pageNum);
        PageCache.IntrusivePage<ATTR> page;
        WeakReference<PageCache.IntrusivePage<ATTR>> localRef;
        if ((localRef = pageState.pageRef) == null || (page = localRef.get()) == null) {
            synchronized (pageState) {
                // Make sure no one materialized this page as we waited for the lock
                if ((localRef = pageState.pageRef) == null || (page = localRef.get()) == null) {
                    page = new IntrusivePage<>(getPageImpl(channelContext, pageNum));
                    pageState.pageRef = new WeakReference<>(page);
                }
            }
        }
        pageCache.touch(page);
        return page.getPage();
    }

    @NotNull
    private PageState<ATTR> pageState(final int pageNum) {
        if (pageNum < 0 || pageNum >= numPages) {
            throw new IllegalArgumentException("pageNum " + pageNum + " is out of range [0, " + numPages + ")");
        }
        PageState<ATTR> pageState;
        while ((pageState = pageStates.get(pageNum)) == null) {
            pageState = new PageState<>();
            if (pageStates.weakCompareAndSetVolatile(pageNum, null, pageState)) {
                break;
            }
        }
        return pageState;
    }

    @Override
    int recordSparseRead(final int pageNum) {
        return pageState(pageNum).sparseReads.incrementAndGet();
    }

    private ChunkPage<ATTR> getPageImpl(@NotNull final SeekableChannelContext channelContext, final int pageNum) {
        // Use the latest context while reading the page, or create (and close) new one
        try (final ContextHolder holder =
                SeekableChannelContext.ensureContext(columnChunkReader.getChannelsProvider(), channelContext)) {
            final ColumnPageReader reader = columnPageDirectAccessor.getPageReader(pageNum, holder.get());
            return toPage(offsetIndex.getFirstRowIndex(pageNum), reader, holder.get());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    @Nullable
    ChunkPage<ATTR> getCachedPage(final int pageNum) {
        final PageState<ATTR> pageState = pageStates.get(pageNum);
        final WeakReference<PageCache.IntrusivePage<ATTR>> localRef;
        final PageCache.IntrusivePage<ATTR> page;
        if (pageState == null || (localRef = pageState.pageRef) == null || (page = localRef.get()) == null) {
            return null;
        }
        pageCache.touch(page);
        return page.getPage();
    }

    @Override
    @NotNull
    ColumnPageReader getPageReader(@NotNull final SeekableChannelContext channelContext, final int pageNum) {
        return columnPageDirectAccessor.getPageReader(pageNum, channelContext);
    }

    @Override
    long pageFirstRow(final int pageNum) {
        return offsetIndex.getFirstRowIndex(pageNum);
    }

    @Override
    long pageRowCount(final int pageNum) {
        final long nextFirstRow = pageNum + 1 < numPages ? offsetIndex.getFirstRowIndex(pageNum + 1) : numRows();
        return nextFirstRow - offsetIndex.getFirstRowIndex(pageNum);
    }

    @Override
    @NotNull
    public ChunkPage<ATTR> getPageContaining(@Nullable final FillContext fillContext, final long rowKey) {
        // We don't really use chunk capacity in our FillContext. In practice, however, this method is only invoked with
        // a null FillContext for single-element "get" methods.
        try (final FillContext allocatedFillContext = fillContext != null ? null : makeFillContext(1, null)) {
            final FillContext fillContextToUse = fillContext != null ? fillContext : allocatedFillContext;
            final SeekableChannelContext channelContext = innerFillContext(fillContextToUse);
            return getPage(channelContext, pageNumContaining(channelContext, rowKey & mask()));
        } catch (final RuntimeException e) {
            throw new UncheckedDeephavenException("Failed to read parquet page data for row: " + rowKey + ", column: " +
                    columnChunkReader.columnName() + ", uri: " + columnChunkReader.getURI(), e);
        }
    }

    @Override
    int pageNumContaining(@NotNull final SeekableChannelContext channelContext, final long row) {
        ensureInitialized(channelContext);
        Require.inRange(row, "row", numRows(), "numRows");

        if (fixedPageSize == PAGE_SIZE_NOT_FIXED) {
            return findPageNumUsingOffsetIndex(offsetIndex, row);
        }
        final int pageNum = (int) (row / fixedPageSize);
        if (pageNum >= numPages) {
            // This can happen if the last page is larger than rest of the pages, which are all the same size.
            // We have already checked that row is less than numRows.
            Assert.geq(row, "row", offsetIndex.getFirstRowIndex(numPages - 1),
                    "offsetIndex.getFirstRowIndex(numPages - 1)");
            return numPages - 1;
        }
        return pageNum;
    }
}
