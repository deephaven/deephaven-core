/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.table.pagestore;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.parquet.table.pagestore.topage.ToPage;
import io.deephaven.engine.table.Releasable;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.page.ChunkPage;
import io.deephaven.engine.page.Page;
import io.deephaven.engine.page.PageStore;
import io.deephaven.parquet.base.ColumnChunkReader;
import io.deephaven.parquet.base.ColumnPageReader;
import io.deephaven.util.SafeCloseable;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ColumnChunkPageStore<ATTR extends Any>
        implements PageStore<ATTR, ATTR, ChunkPage<ATTR>>, Page<ATTR>, SafeCloseable, Releasable {

    final PageCache<ATTR> pageCache;
    private final ColumnChunkReader columnChunkReader;
    private final long mask;
    private final ToPage<ATTR, ?> toPage;

    private final long numRows;

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

    private static boolean canUseOffsetIndexBasedPageStore(@NotNull final ColumnChunkReader columnChunkReader,
            @NotNull final ColumnDefinition<?> columnDefinition) {
        if (columnChunkReader.getOffsetIndex() == null) {
            return false;
        }
        final String version = columnChunkReader.getVersion();
        if (version == null) {
            // Parquet file not written by deephaven, can use offset index
            return true;
        }
        // For vector and array column types, versions before 0.31.0 had a bug in offset index calculation, fixed as
        // part of deephaven-core#4844
        final Class<?> columnType = columnDefinition.getDataType();
        if (columnType.isArray() || Vector.class.isAssignableFrom(columnType)) {
            return hasCorrectVectorOffsetIndexes(version);
        }
        return true;
    }

    private static final Pattern VERSION_PATTERN = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

    /**
     * Check if the version is greater than or equal to 0.31.0, or it doesn't follow the versioning schema X.Y.Z
     */
    @VisibleForTesting
    public static boolean hasCorrectVectorOffsetIndexes(@NotNull final String version) {
        final Matcher matcher = VERSION_PATTERN.matcher(version);
        if (!matcher.matches()) {
            // Could be unit tests or some other versioning scheme
            return true;
        }
        final int major = Integer.parseInt(matcher.group(1));
        final int minor = Integer.parseInt(matcher.group(2));
        return major > 0 || major == 0 && minor >= 31;
    }

    public static <ATTR extends Any> CreatorResult<ATTR> create(
            @NotNull final PageCache<ATTR> pageCache,
            @NotNull final ColumnChunkReader columnChunkReader,
            final long mask,
            @NotNull final ToPage<ATTR, ?> toPage,
            @NotNull final ColumnDefinition<?> columnDefinition) throws IOException {
        final boolean canUseOffsetIndex = canUseOffsetIndexBasedPageStore(columnChunkReader, columnDefinition);
        // TODO(deephaven-core#4879): Rather than this fall back logic for supporting incorrect offset index, we should
        // instead log an error and explain to user how to fix the parquet file
        final ColumnChunkPageStore<ATTR> columnChunkPageStore = canUseOffsetIndex
                ? new OffsetIndexBasedColumnChunkPageStore<>(pageCache, columnChunkReader, mask, toPage)
                : new VariablePageSizeColumnChunkPageStore<>(pageCache, columnChunkReader, mask, toPage);
        final ToPage<DictionaryKeys, long[]> dictionaryKeysToPage =
                toPage.getDictionaryKeysToPage();
        final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysColumnChunkPageStore =
                dictionaryKeysToPage == null ? null
                        : canUseOffsetIndex
                                ? new OffsetIndexBasedColumnChunkPageStore<>(pageCache.castAttr(), columnChunkReader,
                                        mask, dictionaryKeysToPage)
                                : new VariablePageSizeColumnChunkPageStore<>(pageCache.castAttr(), columnChunkReader,
                                        mask, dictionaryKeysToPage);
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

        this.numRows = Require.inRange(columnChunkReader.numRows(), "numRows", mask, "mask");
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

    /**
     * @return The number of rows in this ColumnChunk
     */
    public long numRows() {
        return numRows;
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
    public void close() {}
}
