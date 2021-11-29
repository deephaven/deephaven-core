package io.deephaven.parquet.table.pagestore;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.parquet.table.pagestore.topage.ToPage;
import io.deephaven.parquet.base.ColumnChunkReader;
import io.deephaven.parquet.base.ColumnPageReader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.util.Arrays;

class VariablePageSizeColumnChunkPageStore<ATTR extends Any> extends ColumnChunkPageStore<ATTR> {

    // We will set numPages after changing all of these arrays in place and/or setting additional
    // elements to the
    // end of the array. Thus, for i < numPages, array[i] will always have the same value, and be
    // valid to use, as
    // long as we fetch numPages before accessing the arrays. This is the thread-safe pattern used
    // throughout.

    private volatile int numPages = 0;
    private volatile long[] pageRowOffsets;
    private volatile ColumnPageReader[] columnPageReaders;
    private volatile WeakReference<PageCache.IntrusivePage<ATTR>>[] pages;

    VariablePageSizeColumnChunkPageStore(@NotNull final PageCache<ATTR> pageCache,
            @NotNull final ColumnChunkReader columnChunkReader,
            final long mask,
            @NotNull final ToPage<ATTR, ?> toPage) throws IOException {
        super(pageCache, columnChunkReader, mask, toPage);

        final int INIT_ARRAY_SIZE = 15;
        pageRowOffsets = new long[INIT_ARRAY_SIZE + 1];
        pageRowOffsets[0] = 0;
        columnPageReaders = new ColumnPageReader[INIT_ARRAY_SIZE];

        // noinspection unchecked
        pages = (WeakReference<PageCache.IntrusivePage<ATTR>>[]) new WeakReference[INIT_ARRAY_SIZE];
    }

    private void extendOnePage(final int prevNumPages) {
        PageCache.IntrusivePage<ATTR> page = null;

        synchronized (this) {
            int localNumPages = numPages;

            // Make sure that no one has has already extended to this page yet.
            if (localNumPages == prevNumPages) {
                Assert.assertion(columnPageReaderIterator.hasNext(),
                        "columnPageReaderIterator.hasNext()",
                        "Parquet num rows and page iterator don't match, not enough pages.");

                if (columnPageReaders.length == localNumPages) {
                    int newSize = 2 * localNumPages;

                    pageRowOffsets = Arrays.copyOf(pageRowOffsets, newSize + 1);
                    columnPageReaders = Arrays.copyOf(columnPageReaders, newSize);
                    pages = Arrays.copyOf(pages, newSize);
                }

                final ColumnPageReader columnPageReader = columnPageReaderIterator.next();

                long numRows;
                WeakReference<PageCache.IntrusivePage<ATTR>> pageRef = PageCache.getNullPage();
                long prevRowOffset = pageRowOffsets[localNumPages];

                try {
                    numRows = columnPageReader.numRows();

                    if (numRows < 0) {
                        page = new PageCache.IntrusivePage<>(toPage(prevRowOffset, columnPageReader));
                        pageRef = new WeakReference<>(page);
                        numRows = page.getPage().size();
                    }
                } catch (IOException except) {
                    throw new UncheckedIOException(except);
                }

                columnPageReaders[localNumPages] = columnPageReader;
                pages[localNumPages] = pageRef;
                pageRowOffsets[localNumPages + 1] = prevRowOffset + numRows;
                numPages = localNumPages + 1;
            }
        }

        if (page != null) {
            pageCache.touch(page);
        }
    }

    private int fillToRow(int minPageNum, long row) {
        int localNumPages = numPages;

        while (row >= pageRowOffsets[localNumPages]) {
            minPageNum = localNumPages;
            extendOnePage(localNumPages);
            localNumPages = numPages;
        }

        return minPageNum;
    }

    private ChunkPage<ATTR> getPage(final int pageNum) {
        PageCache.IntrusivePage<ATTR> page = pages[pageNum].get();

        if (page == null) {
            synchronized (columnPageReaders[pageNum]) {
                // Make sure no one filled it for us as we waited for the lock
                page = pages[pageNum].get();

                if (page == null) {
                    try {
                        page = new PageCache.IntrusivePage<>(
                                toPage(pageRowOffsets[pageNum], columnPageReaders[pageNum]));
                    } catch (IOException except) {
                        throw new UncheckedIOException(except);
                    }

                    synchronized (this) {
                        pages[pageNum] = new WeakReference<>(page);
                    }
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
        Require.inRange(row - pageRowOffsets[0], "row", size(), "numRows");

        int localNumPages = numPages;
        int pageNum = Arrays.binarySearch(pageRowOffsets, 1, localNumPages + 1, row);

        if (pageNum < 0) {
            pageNum = -2 - pageNum;
        }

        if (pageNum >= localNumPages) {
            int minPageNum = fillToRow(localNumPages, row);
            localNumPages = numPages;
            pageNum = Arrays.binarySearch(pageRowOffsets, minPageNum + 1, localNumPages + 1, row);

            if (pageNum < 0) {
                pageNum = -2 - pageNum;
            }
        }

        return getPage(pageNum);
    }
}
