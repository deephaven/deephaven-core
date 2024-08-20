//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.page.PagingContextHolder;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
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
import io.deephaven.util.channel.SeekableChannelContext.ContextHolder;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ColumnChunkPageStore<ATTR extends Any>
        implements PageStore<ATTR, ATTR, ChunkPage<ATTR>>, Page<ATTR>, SafeCloseable, Releasable {

    final PageCache<ATTR> pageCache;
    final ColumnChunkReader columnChunkReader;
    private final long mask;
    final ToPage<ATTR, ?> toPage;

    private final long numRows;

    public static class CreatorResult<ATTR extends Any> {

        public final ColumnChunkPageStore<ATTR> pageStore;
        public final Supplier<Chunk<ATTR>> dictionaryChunkSupplier;
        public final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysPageStore;

        private CreatorResult(
                @NotNull final ColumnChunkPageStore<ATTR> pageStore,
                final Supplier<Chunk<ATTR>> dictionaryChunkSupplier,
                final ColumnChunkPageStore<DictionaryKeys> dictionaryKeysPageStore) {
            this.pageStore = pageStore;
            this.dictionaryChunkSupplier = dictionaryChunkSupplier;
            this.dictionaryKeysPageStore = dictionaryKeysPageStore;
        }
    }

    private static boolean canUseOffsetIndexBasedPageStore(
            @NotNull final ColumnChunkReader columnChunkReader,
            @NotNull final ColumnDefinition<?> columnDefinition) {
        if (!columnChunkReader.hasOffsetIndex()) {
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
        final ToPage<DictionaryKeys, long[]> dictionaryKeysToPage = toPage.getDictionaryKeysToPage();
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

    ColumnChunkPageStore(
            @NotNull final PageCache<ATTR> pageCache,
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

    ChunkPage<ATTR> toPage(final long offset, @NotNull final ColumnPageReader columnPageReader,
            @NotNull final SeekableChannelContext channelContext)
            throws IOException {
        return toPage.toPage(offset, columnPageReader, channelContext, mask);
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
     * @see ColumnChunkReader#usesDictionaryOnEveryPage()
     */
    public boolean usesDictionaryOnEveryPage() {
        return columnChunkReader.usesDictionaryOnEveryPage();
    }

    @Override
    public void close() {}

    /**
     * Wrapper class for holding a {@link SeekableChannelContext}.
     */
    private static class ChannelContextWrapper extends PagingContextHolder {
        @NotNull
        private final SeekableChannelContext channelContext;

        private ChannelContextWrapper(
                final int chunkCapacity,
                @Nullable final SharedContext sharedContext,
                @NotNull final SeekableChannelContext channelContext) {
            super(chunkCapacity, sharedContext);
            this.channelContext = channelContext;
        }

        @NotNull
        SeekableChannelContext getChannelContext() {
            return channelContext;
        }

        @Override
        public void close() {
            super.close();
            channelContext.close();
        }
    }

    /**
     * Take an object of {@link PagingContextHolder} and populate the inner context with values from
     * {@link #columnChunkReader}, if required.
     *
     * @param context The context to populate.
     * @return The {@link SeekableChannelContext} to use for reading pages via {@link #columnChunkReader}.
     */
    final SeekableChannelContext innerFillContext(@Nullable final FillContext context) {
        if (context != null) {
            final ChannelContextWrapper innerContext =
                    ((PagingContextHolder) context).updateInnerContext(this::fillContextUpdater);
            return innerContext.getChannelContext();
        }
        return SeekableChannelContext.NULL;
    }

    final ContextHolder ensureContext(@Nullable final FillContext context) {
        return SeekableChannelContext.ensureContext(columnChunkReader.getChannelsProvider(), innerFillContext(context));
    }

    private <T extends FillContext> T fillContextUpdater(
            int chunkCapacity,
            @Nullable final SharedContext sharedContext,
            @Nullable final Context currentInnerContext) {
        final SeekableChannelsProvider channelsProvider = columnChunkReader.getChannelsProvider();
        if (currentInnerContext instanceof ChannelContextWrapper) {
            // Check if we can reuse the channel context object
            final SeekableChannelContext channelContext =
                    ((ChannelContextWrapper) currentInnerContext).getChannelContext();
            if (channelsProvider.isCompatibleWith(channelContext)) {
                // noinspection unchecked
                return (T) currentInnerContext;
            }
        }
        // Create a new channel context object and a wrapper for holding it
        // noinspection unchecked
        return (T) new ChannelContextWrapper(chunkCapacity, sharedContext, channelsProvider.makeContext());
    }
}
