//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.page.PagingContextHolder;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
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
import io.deephaven.parquet.base.SparsePageCursor;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.channel.SeekableChannelContext.ContextHolder;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ColumnChunkPageStore<ATTR extends Any>
        implements PageStore<ATTR, ATTR, ChunkPage<ATTR>>, Page<ATTR>, SafeCloseable, Releasable {

    /**
     * A fill that requests at most this fraction of an uncached page's rows decodes just those rows into the
     * destination, rather than materializing and caching the whole page. {@code 0} disables sparse reads.
     */
    private static volatile double sparseReadMaxDensity = Configuration.getInstance()
            .getDoubleForClassWithDefault(ColumnChunkPageStore.class, "sparseReadMaxDensity", 0.125);

    /**
     * The number of sparse reads, each by a separate pass, after which a page is promoted from sparse reads to a
     * materialized and cached page.
     */
    private static volatile int sparseReadsBeforeCaching = Configuration.getInstance()
            .getIntegerForClassWithDefault(ColumnChunkPageStore.class, "sparseReadsBeforeCaching", 1);

    private static final LongAdder SPARSE_FILLS = new LongAdder();
    private static final LongAdder SPARSE_OPENS = new LongAdder();

    @TestUseOnly
    public static double setSparseReadMaxDensity(final double maxDensity) {
        final double old = sparseReadMaxDensity;
        sparseReadMaxDensity = maxDensity;
        return old;
    }

    @TestUseOnly
    public static int setSparseReadsBeforeCaching(final int readsBeforeCaching) {
        final int old = sparseReadsBeforeCaching;
        sparseReadsBeforeCaching = readsBeforeCaching;
        return old;
    }


    /**
     * @return The number of page fills, across all stores, that have been served by sparse reads
     */
    @TestUseOnly
    static long sparseFillCount() {
        return SPARSE_FILLS.sum();
    }

    /**
     * @return The number of times, across all stores, that a sparse fill has read a page rather than resuming
     */
    @TestUseOnly
    static long sparseOpenCount() {
        return SPARSE_OPENS.sum();
    }

    private enum SparseSupport {
        UNKNOWN, SUPPORTED, UNSUPPORTED
    }

    /**
     * Whether this column's pages support sparse reads; a property of the column, learned from its first page reader.
     */
    private volatile SparseSupport sparseSupport = SparseSupport.UNKNOWN;

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
     * @param row A row key, after applying {@link #mask()}
     * @return The number of the page containing {@code row}
     */
    abstract int pageNumContaining(@NotNull SeekableChannelContext channelContext, long row);

    /**
     * @return The first row of page {@code pageNum}, after applying {@link #mask()}
     */
    abstract long pageFirstRow(int pageNum);

    abstract long pageRowCount(int pageNum);

    /**
     * @return Page {@code pageNum} if it is already materialized, else {@code null}
     */
    @Nullable
    abstract ChunkPage<ATTR> getCachedPage(int pageNum);

    /**
     * @return Page {@code pageNum}, materializing and caching it if necessary
     */
    @NotNull
    abstract ChunkPage<ATTR> getPage(@NotNull SeekableChannelContext channelContext, int pageNum);

    @NotNull
    abstract ColumnPageReader getPageReader(@NotNull SeekableChannelContext channelContext, int pageNum);

    /**
     * Count a sparse read of page {@code pageNum}.
     *
     * @return The number of sparse reads of the page, including this one
     */
    abstract int recordSparseRead(int pageNum);

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence rowSequence) {
        if (sparseReadMaxDensity <= 0 || sparseSupport == SparseSupport.UNSUPPORTED) {
            PageStore.super.fillChunk(context, destination, rowSequence);
            return;
        }
        if (rowSequence.isEmpty()) {
            return;
        }
        destination.setSize(0);
        try (final RowSequence.Iterator rowSequenceIterator = rowSequence.getRowSequenceIterator()) {
            fillChunkAppend(context, destination, rowSequenceIterator);
        }
    }

    @Override
    public void fillChunkAppend(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence.Iterator rowSequenceIterator) {
        final double maxDensity = sparseReadMaxDensity;
        if (maxDensity <= 0 || sparseSupport == SparseSupport.UNSUPPORTED) {
            PageStore.super.fillChunkAppend(context, destination, rowSequenceIterator);
            return;
        }
        long firstKey = rowSequenceIterator.peekNextKey();
        final ChannelContextWrapper contextWrapper = channelContextWrapper(context);
        try (final ContextHolder holder = SeekableChannelContext.ensureContext(
                columnChunkReader.getChannelsProvider(), contextWrapper.getChannelContext())) {
            final SeekableChannelContext channelContext = holder.get();
            final long storeMaxKey = maxRow(firstKey);
            boolean firstPage = true;
            do {
                final boolean isFirstPage = firstPage;
                firstPage = false;
                final int pageNum = pageNumContaining(channelContext, firstKey & mask);
                final ChunkPage<ATTR> cachedPage = getCachedPage(pageNum);
                if (cachedPage != null) {
                    // A materialized page can serve any request.
                    cachedPage.fillChunkAppend(context, destination, rowSequenceIterator);
                    continue;
                }
                final long pageRowCount = pageRowCount(pageNum);
                final long pageFirstKey = (firstKey & ~mask) | pageFirstRow(pageNum);
                final long pageLastKey = pageFirstKey + pageRowCount - 1;
                final RowSequence pageRows = rowSequenceIterator.getNextRowSequenceThrough(pageLastKey);
                // Callers fill in chunks, so a page may be split across fills. Measure density only over the part of
                // the page this fill can see: other fills may request the rows before its first key, if this is the
                // first page it touches, and the rows after its last key, if it ends on this page.
                final long spanFirstKey = isFirstPage ? pageRows.firstRowKey() : pageFirstKey;
                final long spanLastKey = rowSequenceIterator.hasMore() ? pageLastKey : pageRows.lastRowKey();
                if (pageRows.size() <= maxDensity * (spanLastKey - spanFirstKey + 1)
                        && fillSparse(contextWrapper, channelContext, destination, pageRows, pageNum, pageFirstKey)) {
                    continue;
                }
                getPage(channelContext, pageNum).fillChunkAppend(context, destination, pageRows);
            } while (rowSequenceIterator.hasMore() && (firstKey = rowSequenceIterator.peekNextKey()) <= storeMaxKey);
        } catch (final IOException | RuntimeException e) {
            throw new UncheckedDeephavenException("Failed to read parquet page data for row: " + firstKey +
                    ", column: " + columnChunkReader.columnName() + ", uri: " + columnChunkReader.getURI(), e);
        }
    }

    private boolean supportsSparse(@NotNull final ColumnPageReader pageReader) {
        if (sparseSupport == SparseSupport.UNKNOWN) {
            sparseSupport = pageReader.supportsSparse() ? SparseSupport.SUPPORTED : SparseSupport.UNSUPPORTED;
        }
        return sparseSupport == SparseSupport.SUPPORTED;
    }

    /**
     * Decode only {@code pageRows} from the page, and append them to {@code destination} without caching the page.
     * Resumes the context's cursor if the previous sparse fill stopped on this page before these rows.
     *
     * @return Whether the page was read sparsely; if not, nothing was appended, and the caller should materialize it
     */
    private boolean fillSparse(
            @NotNull final ChannelContextWrapper contextWrapper,
            @NotNull final SeekableChannelContext channelContext,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence pageRows,
            final int pageNum,
            final long pageFirstKey) throws IOException {
        final SparsePageCursor cursor = contextWrapper.sparseCursor();
        final boolean sameStore = contextWrapper.sparseStore == this;
        final int cursorPageNum = contextWrapper.sparsePageNum;
        if (sameStore && (pageNum < cursorPageNum
                || (pageNum == cursorPageNum && pageRows.firstRowKey() - pageFirstKey < cursor.nextRow()))) {
            // The cursor only moves forward, so this context has already read past these rows: it reads the column in
            // more than one pass, e.g. through a sort's redirection. Materialize and cache the page for the later
            // passes, rather than decoding it again on every one.
            return false;
        }
        final boolean resume = sameStore && pageNum == cursorPageNum
                && pageRows.firstRowKey() - pageFirstKey >= cursor.nextRow();
        // Until this fill completes, the cursor's position is unknown.
        contextWrapper.sparseStore = null;
        if (!resume) {
            if (recordSparseRead(pageNum) > sparseReadsBeforeCaching) {
                // Other passes keep reading this page; cache it for them.
                return false;
            }
            final ColumnPageReader pageReader = getPageReader(channelContext, pageNum);
            if (!supportsSparse(pageReader)) {
                return false;
            }
            pageReader.openSparse(cursor, channelContext);
            SPARSE_OPENS.increment();
        }
        final int rowCount = pageRows.intSize();
        final LongChunk<OrderedRowKeyRanges> keyRanges = pageRows.asRowKeyRangesChunk();
        final Object values;
        try (final WritableLongChunk<?> rowRanges = WritableLongChunk.makeWritableChunk(keyRanges.size())) {
            for (int ii = 0; ii < keyRanges.size(); ++ii) {
                rowRanges.set(ii, keyRanges.get(ii) - pageFirstKey);
            }
            values = toPage.convertResult(toPage.getSparseResult(cursor, rowRanges, rowCount));
        }
        final int destinationOffset = destination.size();
        destination.copyFromArray(values, 0, destinationOffset, rowCount);
        destination.setSize(destinationOffset + rowCount);
        contextWrapper.sparseStore = this;
        contextWrapper.sparsePageNum = pageNum;
        SPARSE_FILLS.increment();
        return true;
    }

    /**
     * Wrapper class for holding a {@link SeekableChannelContext}.
     */
    private static class ChannelContextWrapper extends PagingContextHolder {
        @NotNull
        private final SeekableChannelContext channelContext;

        @Nullable
        private SparsePageCursor sparseCursor;
        /** The store and page that {@link #sparseCursor} is positioned on, or {@code null} if none. */
        @Nullable
        private ColumnChunkPageStore<?> sparseStore;
        private int sparsePageNum;

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

        @NotNull
        SparsePageCursor sparseCursor() {
            if (sparseCursor == null) {
                sparseCursor = new SparsePageCursor();
            }
            return sparseCursor;
        }

        @Override
        public void close() {
            super.close();
            channelContext.close();
            sparseCursor = null;
            sparseStore = null;
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
            return channelContextWrapper(context).getChannelContext();
        }
        return SeekableChannelContext.NULL;
    }

    private ChannelContextWrapper channelContextWrapper(@NotNull final FillContext context) {
        return ((PagingContextHolder) context).updateInnerContext(this::fillContextUpdater);
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
        return (T) new ChannelContextWrapper(chunkCapacity, sharedContext, channelsProvider.makeReadContext());
    }
}
