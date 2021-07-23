package io.deephaven.db.v2.locations.parquet;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.locations.parquet.topage.ToPage;
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
import io.deephaven.util.datastructures.intrusive.IntrusiveSoftLRU;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.util.function.Supplier;

public abstract class ColumnChunkPageStore<ATTR extends Any>
        implements PageStore<ATTR, ATTR, ChunkPage<ATTR>>, Page<ATTR>, SafeCloseable {

    private static final int CACHE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("ColumnChunkPageStore.cacheSize", 10000);

    final IntrusiveSoftLRU<IntrusivePage<ATTR>> intrusiveSoftLRU =
            new IntrusiveSoftLRU<>(IntrusiveSoftLRU.Node.Adapter.<IntrusivePage<ATTR>>getInstance(), CACHE_SIZE);
    private final long size;
    private final long mask;

    private final ToPage<ATTR, ?> toPage;

    final ColumnChunkReader.ColumnPageReaderIterator columnPageReaderIterator;

    private static final WeakReference<?> NULL_PAGE = new WeakReference<>(null);

    static <ATTR extends Any> WeakReference<IntrusivePage<ATTR>> getNullPage() {
        //noinspection unchecked
        return (WeakReference<IntrusivePage<ATTR>>) NULL_PAGE;
    }

    static class IntrusivePage<ATTR extends Any> extends IntrusiveSoftLRU.Node.Impl<IntrusivePage<ATTR>> {

        private final ChunkPage<ATTR> page;

        IntrusivePage(ChunkPage<ATTR> page) {
            this.page = page;
        }

        ChunkPage<ATTR> getPage() {
            return page;
        }
    }

    public static class CreatorResult<ATTR extends Any> {

        public final ColumnChunkPageStore<ATTR> pageStore;
        public final Chunk<ATTR> dictionary;
        public final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysPageStore;

        public CreatorResult(@NotNull final ColumnChunkPageStore<ATTR> pageStore,
                             final Chunk<ATTR> dictionary,
                             final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysPageStore) {
            this.pageStore = pageStore;
            this.dictionary = dictionary;
            this.dictionaryKeysPageStore = dictionaryKeysPageStore;
        }
    }

    @FunctionalInterface
    public interface Creator<ATTR extends Any> {

        @NotNull
        CreatorResult<ATTR> get(@NotNull ColumnDefinition columnDefinition, long mask) throws IOException;

        @NotNull
        default CreatorResult<ATTR> get(@NotNull final ColumnDefinition columnDefinition) throws IOException {
            return get(columnDefinition, (1L << 63) - 1);
        }
    }

    @FunctionalInterface
    public interface ToPageCreator<ATTR extends Any> {
        ToPage<ATTR, ?> get(@NotNull ColumnDefinition columnDefinition);
    }

    public static <ATTR extends Any> Creator<ATTR> makeCreator(@NotNull final ColumnChunkReader columnChunkReader,
                                                               @NotNull final ToPageCreator<ATTR> toPageCreator) {
        if (columnChunkReader.getPageFixedSize() >= 1) {
            return (columnDefinition, mask) -> {
                final ToPage<ATTR, ?> toPage = toPageCreator.get(columnDefinition);
                final ColumnChunkPageStore<ATTR> columnChunkPageStore =
                        new FixedPageSizeColumnChunkPageStore<>(columnChunkReader, toPage, mask);
                final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysColumnChunkPageStore =
                        new FixedPageSizeColumnChunkPageStore<>(columnChunkReader, toPage.getDictionaryKeysToPage(), mask);

                return new CreatorResult<>(columnChunkPageStore, toPage.getDictionary(), dictionaryKeysColumnChunkPageStore);
            };
        }
        return (columnDefinition, mask) -> {
            final ToPage<ATTR, ?> toPage = toPageCreator.get(columnDefinition);
            final ColumnChunkPageStore<ATTR> columnChunkPageStore =
                    new VariablePageSizeColumnChunkPageStore<>(columnChunkReader, toPage, mask);
            final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysColumnChunkPageStore =
                    new VariablePageSizeColumnChunkPageStore<>(columnChunkReader, toPage.getDictionaryKeysToPage(), mask);

            return new CreatorResult<>(columnChunkPageStore, toPage.getDictionary(), dictionaryKeysColumnChunkPageStore);
        };
    }

    public ColumnChunkPageStore(@NotNull final ColumnChunkReader columnChunkReader, final ToPage<ATTR, ?> toPage,final long mask) throws IOException {
        Require.eqTrue(((mask + 1) & mask) == 0, "Mask is one less than a  power of two.");

        this.toPage = toPage;
        this.mask = mask;
        this.size = Require.inRange(columnChunkReader.numRows(), "numRows", mask, "mask");
        this.columnPageReaderIterator = columnChunkReader.getPageIterator();
    }

    ChunkPage<ATTR> toPage(final long offset, @NotNull final ColumnPageReader columnPageReader) throws IOException {
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

    @Override
    public long length() {
        return size;
    }

    @Override
    public void close() {
        intrusiveSoftLRU.clear();
        try {
            columnPageReaderIterator.close();
        } catch (IOException except) {
            throw new UncheckedIOException(except);
        }
    }

    @Override
    @NotNull
    public Class<?> getNativeType() {
        return toPage.getNativeType();
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
}

