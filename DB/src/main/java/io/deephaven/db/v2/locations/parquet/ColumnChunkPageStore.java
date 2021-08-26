package io.deephaven.db.v2.locations.parquet;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.locations.parquet.topage.ToPage;
import io.deephaven.db.v2.sources.Releasable;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.DictionaryKeys;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.sources.chunk.page.PageStore;
import io.deephaven.parquet.ColumnChunkReader;
import io.deephaven.parquet.ColumnPageReader;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

public abstract class ColumnChunkPageStore<ATTR extends Any>
        implements PageStore<ATTR, ATTR, ChunkPage<ATTR>>, Page<ATTR>, SafeCloseable, Releasable {

    protected final PageCache<ATTR> pageCache;
    private final ColumnChunkReader columnChunkReader;
    private final long mask;
    private final ToPage<ATTR, ?> toPage;

    private final long size;
    final ColumnChunkReader.ColumnPageReaderIterator columnPageReaderIterator;

    public static class CreatorResult<ATTR extends Any> {

        public final ColumnChunkPageStore<ATTR> pageStore;
        public final Supplier<Chunk<ATTR>> dictionaryChunkSupplier;
        public final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysPageStore;

        private CreatorResult(@NotNull final ColumnChunkPageStore<ATTR> pageStore,
                final Supplier<Chunk<ATTR>> dictionaryChunkSupplier,
                final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysPageStore) {
            this.pageStore = pageStore;
            this.dictionaryChunkSupplier = dictionaryChunkSupplier;
            this.dictionaryKeysPageStore = dictionaryKeysPageStore;
        }
    }

    public static <ATTR extends Any> CreatorResult<ATTR> create(
            @NotNull final PageCache<ATTR> pageCache,
            @NotNull final ColumnChunkReader columnChunkReader,
            final long mask,
            @NotNull final ToPage<ATTR, ?> toPage) throws IOException {
        final boolean fixedSizePages = columnChunkReader.getPageFixedSize() >= 1;
        final ColumnChunkPageStore<ATTR> columnChunkPageStore = fixedSizePages
                ? new FixedPageSizeColumnChunkPageStore<>(pageCache, columnChunkReader, mask, toPage)
                : new VariablePageSizeColumnChunkPageStore<>(pageCache, columnChunkReader, mask, toPage);
        final ToPage<DictionaryKeys, long[]> dictionaryKeysToPage =
                toPage.getDictionaryKeysToPage();
        final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysColumnChunkPageStore =
                dictionaryKeysToPage == null ? null
                        : fixedSizePages
                                ? new FixedPageSizeColumnChunkPageStore<>(pageCache.castAttr(), columnChunkReader, mask,
                                        dictionaryKeysToPage)
                                : new VariablePageSizeColumnChunkPageStore<>(pageCache.castAttr(), columnChunkReader,
                                        mask,
                                        dictionaryKeysToPage);
        return new CreatorResult<>(columnChunkPageStore, toPage::getDictionaryChunk,
                dictionaryKeysColumnChunkPageStore);
    }

    ColumnChunkPageStore(@NotNull final PageCache<ATTR> pageCache,
            @NotNull final ColumnChunkReader columnChunkReader,
            final long mask,
            final ToPage<ATTR, ?> toPage) throws IOException {
        Require.requirement(((mask + 1) & mask) == 0, "mask is one less than a power of two");

        this.pageCache = pageCache;
        this.columnChunkReader = columnChunkReader;
        this.mask = mask;
        this.toPage = toPage;

        this.size = Require.inRange(columnChunkReader.numRows(), "numRows", mask, "mask");
        this.columnPageReaderIterator = columnChunkReader.getPageIterator();
    }

    ChunkPage<ATTR> toPage(final long offset, @NotNull final ColumnPageReader columnPageReader)
            throws IOException {
        return toPage.toPage(offset, columnPageReader, mask);
    }

    @Override
    public long mask() {
        return mask;
    }

    @Override
    public long firstRowOffset() {
        return 0;
    }

    public long size() {
        return size;
    }

    @Override
    @NotNull
    public ChunkType getChunkType() {
        return toPage.getChunkType();
    }

    /**
     * These implementations don't use the FillContext parameter, so we're create a helper method to ignore it.
     */
    @NotNull
    public ChunkPage<ATTR> getPageContaining(final long row) {
        return getPageContaining(DEFAULT_FILL_INSTANCE, row);
    }

    /**
     * @see ColumnChunkReader#usesDictionaryOnEveryPage()
     */
    public boolean usesDictionaryOnEveryPage() {
        return columnChunkReader.usesDictionaryOnEveryPage();
    }

    @Override
    public void close() {
        try {
            columnPageReaderIterator.close();
        } catch (IOException except) {
            throw new UncheckedIOException(except);
        }
    }
}
