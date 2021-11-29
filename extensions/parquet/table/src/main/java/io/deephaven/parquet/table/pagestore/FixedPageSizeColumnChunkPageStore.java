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

class FixedPageSizeColumnChunkPageStore<ATTR extends Any> extends ColumnChunkPageStore<ATTR> {

    private final int pageFixedSize;
    private volatile int numPages = 0;
    private final ColumnPageReader[] columnPageReaders;
    private final WeakReference<PageCache.IntrusivePage<ATTR>>[] pages;

    FixedPageSizeColumnChunkPageStore(@NotNull final PageCache<ATTR> pageCache,
            @NotNull final ColumnChunkReader columnChunkReader,
            final long mask,
            @NotNull final ToPage<ATTR, ?> toPage) throws IOException {
        super(pageCache, columnChunkReader, mask, toPage);

        this.pageFixedSize = columnChunkReader.getPageFixedSize();

        Require.gtZero(pageFixedSize, "pageFixedSize");

        final int numPages = Math.toIntExact((size() - 1) / pageFixedSize + 1);
        this.columnPageReaders = new ColumnPageReader[numPages];

        // noinspection unchecked
        this.pages = (WeakReference<PageCache.IntrusivePage<ATTR>>[]) new WeakReference[numPages];
        Arrays.fill(pages, PageCache.getNullPage());
    }

    private void fillToPage(final int pageNum) {

        while (numPages <= pageNum) {
            synchronized (this) {
                if (numPages <= pageNum) {
                    Assert.assertion(columnPageReaderIterator.hasNext(),
                            "columnPageReaderIterator.hasNext()",
                            "Parquet fixed page size and page iterator don't match, not enough pages.");
                    columnPageReaders[numPages++] = columnPageReaderIterator.next();
                }
            }
        }
    }

    private ChunkPage<ATTR> getPage(final int pageNum) {
        PageCache.IntrusivePage<ATTR> page = pages[pageNum].get();

        if (page == null) {
            synchronized (columnPageReaders[pageNum]) {
                page = pages[pageNum].get();

                if (page == null) {
                    try {
                        page = new PageCache.IntrusivePage<>(
                                toPage((long) pageNum * pageFixedSize, columnPageReaders[pageNum]));
                    } catch (IOException except) {
                        throw new UncheckedIOException(except);
                    }

                    pages[pageNum] = new WeakReference<>(page);
                }
            }
        }

        pageCache.touch(page);
        return page.getPage();
    }

    @Override
    public @NotNull ChunkPage<ATTR> getPageContaining(FillContext fillContext,
            final long elementIndex) {
        final long row = elementIndex & mask();
        Require.inRange(row, "row", size(), "numRows");

        // This is safe because of our check in the constructor, and we know the row is in range.
        final int pageNum = (int) (row / pageFixedSize);

        fillToPage(pageNum);
        return getPage(pageNum);
    }
}
