package io.deephaven.db.v2.locations.parquet;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.sources.chunk.page.ChunkPage;
import io.deephaven.db.v2.sources.chunk.page.Page;
import io.deephaven.db.v2.sources.chunk.page.PageStore;
import io.deephaven.db.v2.locations.parquet.topage.ToPage;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.intrusive.IntrusiveSoftLRU;
import io.deephaven.parquet.ColumnChunkReader;
import io.deephaven.parquet.ColumnPageReader;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.util.function.Supplier;

public abstract class ColumnChunkPageStore<ATTR extends Attributes.Any>
        implements PageStore<ATTR, ATTR, ChunkPage<ATTR>>, Page<ATTR>, SafeCloseable {

    private static final int CACHE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("ColumnChunkPageStore.cacheSize", 10000);

    IntrusiveSoftLRU<IntrusivePage<ATTR>> intrusiveSoftLRU =
            new IntrusiveSoftLRU<>(IntrusiveSoftLRU.Node.Adapter.<IntrusivePage<ATTR>>getInstance(), CACHE_SIZE);
    private final long size;
    private final long mask;

    private final ToPage<ATTR, ?> toPage;

    final ColumnChunkReader.ColumnPageReaderIterator columnPageReaderIterator;

    private static final WeakReference<?> NULL_PAGE = new WeakReference<>(null);

    static <ATTR extends Attributes.Any> WeakReference<IntrusivePage<ATTR>> getNullPage() {
        //noinspection unchecked
        return (WeakReference<IntrusivePage<ATTR>>) NULL_PAGE;
    }

    static class IntrusivePage<ATTR extends Attributes.Any> extends IntrusiveSoftLRU.Node.Impl<IntrusivePage<ATTR>> {
        ChunkPage<ATTR> page;

        IntrusivePage(ChunkPage<ATTR> page) {
            this.page = page;
        }

        ChunkPage<ATTR> getPage() {
            return page;
        }
    }

    public static class Values<ATTR extends Attributes.Any> {
        public final ColumnChunkPageStore<ATTR> pageStore;
        public final Chunk<ATTR> dictionary;
        public final ColumnChunkPageStore<Attributes.DictionaryKeys> dictionaryKeysPageStore;
        public final Supplier<?> getMetadata;

        public Values(ColumnChunkPageStore<ATTR> pageStore,
                      Chunk<ATTR> dictionary,
                      ColumnChunkPageStore<Attributes.DictionaryKeys> dictionaryKeysPageStore,
                      Supplier<?> getMetadata) {
            this.pageStore = pageStore;
            this.dictionary = dictionary;
            this.dictionaryKeysPageStore = dictionaryKeysPageStore;
            this.getMetadata = getMetadata;
        }
    }

    @FunctionalInterface
    public interface Creator<ATTR extends Attributes.Any> {

        default boolean exists() {
            return true;
        }

        @NotNull
        Values<ATTR> get(@NotNull ColumnDefinition columnDefinition, long mask) throws IOException;

        @NotNull
        default Values<ATTR> get(@NotNull ColumnDefinition columnDefinition) throws IOException {
            return get(columnDefinition, (1L << 63) - 1);
        }
    }

    @FunctionalInterface
    public interface MetaDataCreator {
       Supplier<?> get(ColumnDefinition columnDefinition) throws IOException;
    }

    @FunctionalInterface
    public interface ToPageCreator<ATTR extends Attributes.Any> {
        ToPage<ATTR, ?> get(@NotNull ColumnDefinition columnDefinition);
    }

    private static final Values NULL_VALUES = new Values<>(null, null, null, null);
    private static final Creator NULL_CREATOR = new Creator() {
        @Override
        public boolean exists() {
            return false;
        }

        @Override
        @NotNull
        public Values get(@NotNull ColumnDefinition columnDefinition, long mask) {
            return NULL_VALUES;
        }
    };

    public static <ATTR extends Attributes.Any>
    ColumnChunkPageStore.Creator<ATTR>
    creator(ColumnChunkReader columnChunkReader, @NotNull ToPageCreator<ATTR> toPageCreator, MetaDataCreator metaDataCreator) {
        if (columnChunkReader == null) {
            //noinspection unchecked
            return NULL_CREATOR;
        } else if (columnChunkReader.getPageFixedSize() >= 1) {
            return (columnDefinition, mask) -> {
                final ToPage<ATTR, ?> toPage = toPageCreator.get(columnDefinition);

                final ColumnChunkPageStore<ATTR> columnChunkPageStore =
                        new FixedPageSizeColumnChunkPageStore<>(columnChunkReader, toPage, mask);
                final ColumnChunkPageStore<Attributes.DictionaryKeys> dictionaryKeysColumnChunkPageStore =
                        new FixedPageSizeColumnChunkPageStore<>(columnChunkReader, toPage.getDictionaryKeysToPage(), mask);

                return new Values<>(columnChunkPageStore, toPage.getDictionary(), dictionaryKeysColumnChunkPageStore,
                        metaDataCreator == null ? null : metaDataCreator.get(columnDefinition));
            };
        } else {
            return (columnDefinition, mask) -> {
                final ToPage<ATTR, ?> toPage = toPageCreator.get(columnDefinition);

                final ColumnChunkPageStore<ATTR> columnChunkPageStore =
                        new VariablePageSizeColumnChunkPageStore<>(columnChunkReader, toPage, mask);
                final ColumnChunkPageStore<Attributes.DictionaryKeys> dictionaryKeysColumnChunkPageStore =
                        new VariablePageSizeColumnChunkPageStore<>(columnChunkReader, toPage.getDictionaryKeysToPage(), mask);

                return new Values<>(columnChunkPageStore, toPage.getDictionary(), dictionaryKeysColumnChunkPageStore,
                        metaDataCreator == null ? null : metaDataCreator.get(columnDefinition));
            };
        }
    }

    public ColumnChunkPageStore(ColumnChunkReader columnChunkReader, ToPage<ATTR, ?> toPage, long mask) throws IOException {
        Require.eqTrue(((mask+1) & mask) == 0, "Mask is one less than a  power of two.");

        this.toPage = toPage;
        this.mask = mask;
        this.size = Require.inRange(columnChunkReader.numRows(), "numRows", mask, "mask");
        this.columnPageReaderIterator = columnChunkReader.getPageIterator();
    }

    ChunkPage<ATTR> toPage(long offset, @NotNull ColumnPageReader columnPageReader) throws IOException {
        return toPage.toPage(offset, columnPageReader, mask);
    }

    @Override
    public long mask() { return mask; }

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
    public ChunkPage<ATTR> getPageContaining(long row) {
       return getPageContaining(DEFAULT_FILL_INSTANCE, row);
    }

}


