//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.page.PagingContextHolder;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.DefaultGetContext;
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
import java.lang.ref.WeakReference;
import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class ColumnChunkPageStore<ATTR extends Any>
        implements PageStore<ATTR, ATTR, ChunkPage<ATTR>>, Page<ATTR>, SafeCloseable, Releasable {

    /**
     * A fill that requests at most this fraction of the rows it spans on an uncached page decodes just those rows, and
     * caches their values for later reads, rather than materializing and caching the whole page. Once a page's cached
     * values would cover more than this fraction of its rows, it is materialized instead. {@code 0} disables sparse
     * reads.
     */
    private static volatile double sparseReadMaxDensity = Configuration.getInstance()
            .getDoubleForClassWithDefault(ColumnChunkPageStore.class, "sparseReadMaxDensity", 0.125);

    /**
     * The number of sparse misses of a page, each a fill that opens the page to decode rows its cached sparse values
     * lack, after which the page is materialized and cached whole. A fill that starts on the page past every row
     * requested there continues an in-order read, such as a viewport scrolling in file order, and is not a miss.
     */
    private static volatile int sparseMissesBeforeFullCaching = Configuration.getInstance()
            .getIntegerForClassWithDefault(ColumnChunkPageStore.class, "sparseMissesBeforeFullCaching", 2);

    private static final LongAdder SPARSE_FILLS = new LongAdder();
    private static final LongAdder SPARSE_OPENS = new LongAdder();
    private static final LongAdder SPARSE_HITS = new LongAdder();

    /**
     * Set the process-wide sparse read density cutoff, which starts as the
     * {@code ColumnChunkPageStore.sparseReadMaxDensity} configuration property; {@code 0} disables sparse reads.
     *
     * @return The previous setting
     */
    public static double setSparseReadMaxDensity(final double maxDensity) {
        final double old = sparseReadMaxDensity;
        sparseReadMaxDensity = maxDensity;
        return old;
    }

    /**
     * Set the process-wide number of sparse misses before a page is cached whole, which starts as the
     * {@code ColumnChunkPageStore.sparseMissesBeforeFullCaching} configuration property.
     *
     * @return The previous setting
     */
    public static int setSparseMissesBeforeFullCaching(final int missesBeforeFullCaching) {
        final int old = sparseMissesBeforeFullCaching;
        sparseMissesBeforeFullCaching = missesBeforeFullCaching;
        return old;
    }

    /**
     * @return The number of page fills, across all stores, that decoded rows sparsely
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

    /**
     * @return The number of page fills, across all stores, that have been served entirely by cached sparse values
     */
    @TestUseOnly
    static long sparseHitCount() {
        return SPARSE_HITS.sum();
    }

    /**
     * Per-page sparse read state.
     */
    static final class SparseState<ATTR extends Any> {
        /** The number of sparse misses of the page; see {@link #sparseMissesBeforeFullCaching}. */
        final AtomicInteger misses = new AtomicInteger();
        /** The highest page-relative row any sparse fill has requested, or {@code -1}. */
        final AtomicLong lastRequestedRow = new AtomicLong(-1);
        /** The page's cached sparse values, if any. */
        @Nullable
        volatile WeakReference<SparsePage<ATTR>> pageRef;
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

    /**
     * @return The number of rows in page {@code pageNum}
     */
    abstract long pageRowCount(int pageNum);

    /**
     * @return Page {@code pageNum} if it is cached, touching it, else {@code null}
     */
    @Nullable
    abstract ChunkPage<ATTR> getCachedPage(int pageNum);

    /**
     * @return Page {@code pageNum}, materializing and caching it if necessary
     */
    @NotNull
    abstract ChunkPage<ATTR> getPage(@NotNull SeekableChannelContext channelContext, int pageNum);

    /**
     * @return The reader for page {@code pageNum}, to read it sparsely rather than materialize it
     */
    @NotNull
    abstract ColumnPageReader getPageReader(@NotNull SeekableChannelContext channelContext, int pageNum);

    /**
     * @return The sparse read state of page {@code pageNum}, shared by every reader of this store and kept for its
     *         lifetime, whether or not the page is cached
     */
    @NotNull
    abstract SparseState<ATTR> sparseState(int pageNum);

    @Nullable
    private SparsePage<ATTR> getSparsePage(@NotNull final SparseState<ATTR> state) {
        final WeakReference<SparsePage<ATTR>> unlockedRef = state.pageRef;
        if (unlockedRef == null || unlockedRef.get() == null) {
            return null;
        }
        // Touch under the lock for the reason in publishSparse.
        synchronized (state) {
            final WeakReference<SparsePage<ATTR>> localRef = state.pageRef;
            final SparsePage<ATTR> page;
            if (localRef == null || (page = localRef.get()) == null) {
                return null;
            }
            pageCache.touch(page);
            return page;
        }
    }

    /**
     * Add {@code rows} and their values to the cached sparse values of page {@code pageNum}, unless it is cached whole.
     *
     * @param rows Page-relative rows; ownership passes to this method
     * @param values An array of at least {@code rows.size()} values, in the order of {@code rows}; ownership passes to
     *        this method
     */
    private void publishSparse(final int pageNum, @NotNull final WritableRowSet rows, @NotNull final Object values) {
        final SparseState<ATTR> state = sparseState(pageNum);
        final int rowCount = rows.intSize();
        synchronized (state) {
            if (getCachedPage(pageNum) != null) {
                // The page was cached whole while these rows were pending.
                rows.close();
                return;
            }
            final WeakReference<SparsePage<ATTR>> localRef = state.pageRef;
            final SparsePage<ATTR> current = localRef == null ? null : localRef.get();
            final SparsePage<ATTR> page;
            if (current == null) {
                page = new SparsePage<>(rows, Array.getLength(values) == rowCount ? values : copyOf(values, rowCount));
            } else {
                // Merge with the latest entry, so that rows published by other contexts are kept.
                final WritableRowSet union = current.rows.union(rows);
                final Object merged = Array.newInstance(values.getClass().getComponentType(), union.intSize());
                final WritableChunk<ATTR> mergedChunk = getChunkType().writableChunkWrap(merged, 0, union.intSize());
                try (final RowSet currentSource = RowSetFactory.flat(current.rows.size());
                        final RowSet currentDestination = union.invert(current.rows);
                        final RowSet rowsSource = RowSetFactory.flat(rowCount);
                        final RowSet rowsDestination = union.invert(rows)) {
                    copyPositions(current.values, currentSource, mergedChunk, 0, currentDestination);
                    copyPositions(values, rowsSource, mergedChunk, 0, rowsDestination);
                }
                rows.close();
                page = new SparsePage<>(union, merged);
                evict(current);
            }
            state.pageRef = new WeakReference<>(page);
            // Under the lock, so that a concurrent merge can't evict the page first: touching an evicted page would add
            // it to the cache as a new, dead entry, in place of a live one.
            pageCache.touch(page);
        }
    }

    /**
     * Drop the cached sparse values of page {@code pageNum}.
     */
    private void dropSparse(final int pageNum) {
        final SparseState<ATTR> state = sparseState(pageNum);
        synchronized (state) {
            final WeakReference<SparsePage<ATTR>> localRef = state.pageRef;
            final SparsePage<ATTR> current = localRef == null ? null : localRef.get();
            state.pageRef = null;
            if (current != null) {
                evict(current);
            }
        }
    }

    /**
     * Release {@code superseded} from the page cache now, as the garbage collector would, rather than leaving it to age
     * out. Readers still holding it are unaffected.
     */
    private static void evict(@NotNull final SparsePage<?> superseded) {
        superseded.getOwner().clear();
    }

    private static Object copyOf(@NotNull final Object array, final int length) {
        final Object copy = Array.newInstance(array.getClass().getComponentType(), length);
        // noinspection SuspiciousSystemArraycopy: copy is an array of array's component type
        System.arraycopy(array, 0, copy, 0, Math.min(length, Array.getLength(array)));
        return copy;
    }

    /**
     * Copy the values at {@code sourcePositions} of {@code source} to {@code destinationOffset} plus the corresponding
     * {@code destinationPositions} of {@code destination}. The two position sets have the same size.
     */
    private static void copyPositions(
            @NotNull final Object source,
            @NotNull final RowSet sourcePositions,
            @NotNull final WritableChunk<?> destination,
            final int destinationOffset,
            @NotNull final RowSet destinationPositions) {
        try (final RowSet.RangeIterator sourceRanges = sourcePositions.rangeIterator();
                final RowSet.RangeIterator destinationRanges = destinationPositions.rangeIterator()) {
            long sourcePos = 0;
            long sourceEnd = -1;
            long destinationPos = 0;
            long destinationEnd = -1;
            while (true) {
                if (sourcePos > sourceEnd) {
                    if (!sourceRanges.hasNext()) {
                        return;
                    }
                    sourceRanges.next();
                    sourcePos = sourceRanges.currentRangeStart();
                    sourceEnd = sourceRanges.currentRangeEnd();
                }
                if (destinationPos > destinationEnd) {
                    destinationRanges.next();
                    destinationPos = destinationRanges.currentRangeStart();
                    destinationEnd = destinationRanges.currentRangeEnd();
                }
                final int length = (int) Math.min(sourceEnd - sourcePos, destinationEnd - destinationPos) + 1;
                destination.copyFromArray(source, (int) sourcePos, destinationOffset + (int) destinationPos, length);
                sourcePos += length;
                destinationPos += length;
            }
        }
    }

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
    public Chunk<? extends ATTR> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (sparseReadMaxDensity <= 0 || sparseSupport == SparseSupport.UNSUPPORTED || rowSequence.isContiguous()) {
            // A contiguous request is dense, and may be a slice of a cached page.
            return PageStore.super.getChunk(context, rowSequence);
        }
        // The default materializes the first page; fill instead, so that every page may be read sparsely.
        final WritableChunk<ATTR> destination = DefaultGetContext.getWritableChunk(context);
        fillChunk(DefaultGetContext.getFillContext(context), destination, rowSequence);
        return destination;
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
                // Only reading is wrapped as a read failure, not filling from a page in memory.
                final ChunkPage<ATTR> page;
                RowSequence pageRows = null;
                try {
                    final int pageNum = pageNumContaining(channelContext, firstKey & mask);
                    final ChunkPage<ATTR> cachedPage = getCachedPage(pageNum);
                    if (cachedPage != null) {
                        // A materialized page can serve any request.
                        page = cachedPage;
                    } else {
                        final long pageFirstKey = (firstKey & ~mask) | pageFirstRow(pageNum);
                        pageRows = rowSequenceIterator.getNextRowSequenceThrough(
                                pageFirstKey + pageRowCount(pageNum) - 1);
                        page = readPage(contextWrapper, channelContext, destination, pageRows, pageNum, pageFirstKey,
                                isFirstPage, rowSequenceIterator.hasMore(), maxDensity);
                    }
                } catch (final IOException | RuntimeException e) {
                    throw new UncheckedDeephavenException("Failed to read parquet page data for row: " + firstKey +
                            ", column: " + columnChunkReader.columnName() + ", uri: " + columnChunkReader.getURI(),
                            e);
                }
                if (page == null) {
                    continue;
                }
                if (pageRows == null) {
                    page.fillChunkAppend(context, destination, rowSequenceIterator);
                } else {
                    page.fillChunkAppend(context, destination, pageRows);
                }
            } while (rowSequenceIterator.hasMore() && (firstKey = rowSequenceIterator.peekNextKey()) <= storeMaxKey);
        }
    }

    /**
     * Read {@code pageRows} sparsely into {@code destination}, or else materialize and cache the page.
     *
     * @param isFirstPage Whether this is the first page the fill touches
     * @param fillContinues Whether the fill requests rows after this page
     * @return The page to fill {@code pageRows} from, or {@code null} if they were read sparsely
     */
    @Nullable
    private ChunkPage<ATTR> readPage(
            @NotNull final ChannelContextWrapper contextWrapper,
            @NotNull final SeekableChannelContext channelContext,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence pageRows,
            final int pageNum,
            final long pageFirstKey,
            final boolean isFirstPage,
            final boolean fillContinues,
            final double maxDensity) throws IOException {
        // Callers fill in chunks, so a page may be split across fills. Measure density only over the part of the page
        // this fill can see: other fills may request the rows before its first key, if this is the first page it
        // touches, and the rows after its last key, if it ends on this page. A short contiguous run therefore looks
        // dense, and its page is cached whole; telling it apart needs the request's full extent.
        final long spanFirstKey = isFirstPage ? pageRows.firstRowKey() : pageFirstKey;
        final long spanLastKey = fillContinues ? pageFirstKey + pageRowCount(pageNum) - 1 : pageRows.lastRowKey();
        if (pageRows.size() <= maxDensity * (spanLastKey - spanFirstKey + 1)) {
            if (fillSparse(contextWrapper, channelContext, destination, pageRows, pageNum, pageFirstKey, isFirstPage,
                    maxDensity)) {
                return null;
            }
            // Cache the page whole. The sparse cursor may already hold its bytes, but this is rare enough to read them
            // again.
            contextWrapper.discardPending(this, pageNum);
        }
        final ChunkPage<ATTR> page = getPage(channelContext, pageNum);
        // Drop any sparse values only once the page is cached, so that a concurrent publish either sees the cached page
        // or is dropped here.
        dropSparse(pageNum);
        return page;
    }

    private boolean supportsSparse(@NotNull final ColumnPageReader pageReader) {
        if (sparseSupport == SparseSupport.UNKNOWN) {
            sparseSupport = pageReader.supportsSparse() ? SparseSupport.SUPPORTED : SparseSupport.UNSUPPORTED;
        }
        return sparseSupport == SparseSupport.SUPPORTED;
    }

    /**
     * Append {@code pageRows} to {@code destination} without materializing the page: from the page's cached sparse
     * values where they have them, and otherwise by decoding just the rows they lack. Resumes the context's cursor if
     * the previous sparse fill stopped on this page before these rows.
     *
     * @return Whether the page was read sparsely; if not, nothing was appended, and the caller should materialize it
     */
    private boolean fillSparse(
            @NotNull final ChannelContextWrapper contextWrapper,
            @NotNull final SeekableChannelContext channelContext,
            @NotNull final WritableChunk<? super ATTR> destination,
            @NotNull final RowSequence pageRows,
            final int pageNum,
            final long pageFirstKey,
            final boolean isFirstPage,
            final double maxDensity) throws IOException {
        final SparsePageCursor cursor = contextWrapper.sparseCursor();
        final boolean sameStore = contextWrapper.sparseStore == this;
        final int cursorPageNum = contextWrapper.sparsePageNum;
        final boolean onCursorPage = sameStore && pageNum == cursorPageNum;
        if (!onCursorPage) {
            // This fill is off the cursor's page; publish the rows this context decoded there.
            contextWrapper.publishPending();
        }
        final SparseState<ATTR> state = sparseState(pageNum);
        final SparsePage<ATTR> cached = getSparsePage(state);
        final int rowCount = pageRows.intSize();
        // Filled rather than viewed: each RowSequence allocates its own ranges chunk, and every fill sees a new one.
        final WritableLongChunk<OrderedRowKeyRanges> requestRanges = contextWrapper.requestRanges(2 * rowCount);
        pageRows.fillRowKeyRangesChunk(requestRanges);
        // A fill that starts here, past every row requested before, continues an in-order read rather than repeating
        // one; see sparseMissesBeforeFullCaching.
        final long previousLastRow = state.lastRequestedRow.getAndAccumulate(
                requestRanges.get(requestRanges.size() - 1) - pageFirstKey, Math::max);
        final boolean continuesRead =
                isFirstPage && previousLastRow >= 0 && requestRanges.get(0) - pageFirstKey > previousLastRow;
        // Page-relative ranges of the rows to decode: the requested rows that have no cached value.
        final WritableLongChunk<OrderedRowKeyRanges> decodeRanges = contextWrapper.decodeRanges(2 * rowCount);
        final int decodeCount;
        if (cached == null) {
            decodeRanges.setSize(requestRanges.size());
            for (int ii = 0; ii < requestRanges.size(); ++ii) {
                decodeRanges.set(ii, requestRanges.get(ii) - pageFirstKey);
            }
            decodeCount = rowCount;
        } else {
            decodeCount = collectMissing(cached.rows, requestRanges, pageFirstKey, decodeRanges);
        }
        if (decodeCount == 0) {
            // noinspection DataFlowIssue: pageRows is never empty, so some rows were cached
            fillMerged(cached, requestRanges, pageFirstKey, null, rowCount, destination);
            SPARSE_HITS.increment();
            return true;
        }
        if (sameStore && (pageNum < cursorPageNum || (onCursorPage && decodeRanges.get(0) < cursor.nextRow()))) {
            // The cursor only moves forward, so this context has already read past these rows: it reads the column in
            // more than one pass, e.g. through a sort's redirection. The rows it decoded may serve this request; if
            // not, materialize and cache the page for the later passes, rather than decoding it again on every one.
            if (onCursorPage) {
                contextWrapper.publishPending();
                final SparsePage<ATTR> published = getSparsePage(state);
                if (published != null
                        && collectMissing(published.rows, requestRanges, pageFirstKey, decodeRanges) == 0) {
                    fillMerged(published, requestRanges, pageFirstKey, null, rowCount, destination);
                    SPARSE_HITS.increment();
                    return true;
                }
            }
            return false;
        }
        final long cachedRows = (cached == null ? 0 : cached.rows.size())
                + (onCursorPage ? contextWrapper.pendingSize(this, pageNum) : 0);
        if (cachedRows + decodeCount > maxDensity * pageRowCount(pageNum)) {
            // Enough of the page is wanted to cache it whole.
            return false;
        }
        // Until this fill completes, the cursor's position is unknown.
        contextWrapper.sparseStore = null;
        if (!onCursorPage) {
            if (!continuesRead && state.misses.incrementAndGet() > sparseMissesBeforeFullCaching) {
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
        final Object values = toPage.convertResult(toPage.getSparseResult(cursor, decodeRanges, decodeCount));
        if (cached == null) {
            final int destinationOffset = destination.size();
            destination.setSize(destinationOffset + rowCount);
            destination.copyFromArray(values, 0, destinationOffset, rowCount);
        } else {
            fillMerged(cached, requestRanges, pageFirstKey, values, rowCount, destination);
        }
        contextWrapper.appendPending(this, pageNum, decodeRanges, decodeCount, values);
        contextWrapper.sparseStore = this;
        contextWrapper.sparsePageNum = pageNum;
        SPARSE_FILLS.increment();
        return true;
    }

    /**
     * Collect the requested rows that {@code cachedRows} lacks.
     *
     * @param cachedRows Page-relative rows
     * @param requestRanges The requested row key ranges; subtracting {@code pageFirstKey} makes them page-relative
     * @param missingRanges Set to the page-relative ranges of the missing rows; must have capacity for twice the
     *        requested row count
     * @return The number of missing rows
     */
    private static int collectMissing(
            @NotNull final RowSet cachedRows,
            @NotNull final LongChunk<OrderedRowKeyRanges> requestRanges,
            final long pageFirstKey,
            @NotNull final WritableLongChunk<OrderedRowKeyRanges> missingRanges) {
        missingRanges.setSize(0);
        int missingCount = 0;
        try (final CachedRanges cachedRanges = new CachedRanges(cachedRows)) {
            for (int ii = 0; ii < requestRanges.size(); ii += 2) {
                long first = requestRanges.get(ii) - pageFirstKey;
                final long last = requestRanges.get(ii + 1) - pageFirstKey;
                while (first <= last) {
                    final boolean found = cachedRanges.seek(first);
                    final long end;
                    if (found && cachedRanges.start <= first) {
                        end = Math.min(last, cachedRanges.end);
                    } else {
                        end = found ? Math.min(last, cachedRanges.start - 1) : last;
                        missingRanges.add(first);
                        missingRanges.add(end);
                        missingCount += (int) (end - first + 1);
                    }
                    first = end + 1;
                }
            }
        }
        return missingCount;
    }

    /**
     * Append the requested rows to {@code destination}, from {@code cached} where it has them, and otherwise from
     * {@code decoded}.
     *
     * @param requestRanges The requested row key ranges; subtracting {@code pageFirstKey} makes them page-relative
     * @param decoded The values of the requested rows that {@code cached} lacks, in order, or {@code null} if it lacks
     *        none
     * @param rowCount The number of requested rows
     */
    private void fillMerged(
            @NotNull final SparsePage<ATTR> cached,
            @NotNull final LongChunk<OrderedRowKeyRanges> requestRanges,
            final long pageFirstKey,
            @Nullable final Object decoded,
            final int rowCount,
            @NotNull final WritableChunk<? super ATTR> destination) {
        int destinationPos = destination.size();
        destination.setSize(destinationPos + rowCount);
        int decodedPos = 0;
        try (final CachedRanges cachedRanges = new CachedRanges(cached.rows)) {
            for (int ii = 0; ii < requestRanges.size(); ii += 2) {
                long first = requestRanges.get(ii) - pageFirstKey;
                final long last = requestRanges.get(ii + 1) - pageFirstKey;
                while (first <= last) {
                    final boolean found = cachedRanges.seek(first);
                    final long end;
                    if (found && cachedRanges.start <= first) {
                        end = Math.min(last, cachedRanges.end);
                        final int length = (int) (end - first + 1);
                        destination.copyFromArray(cached.values, (int) cachedRanges.positionOf(first), destinationPos,
                                length);
                        destinationPos += length;
                    } else {
                        end = found ? Math.min(last, cachedRanges.start - 1) : last;
                        final int length = (int) (end - first + 1);
                        // noinspection DataFlowIssue: decoded holds every requested row that cached lacks
                        destination.copyFromArray(decoded, decodedPos, destinationPos, length);
                        decodedPos += length;
                        destinationPos += length;
                    }
                    first = end + 1;
                }
            }
        }
    }

    /**
     * Walks a page's cached rows forward by range, tracking each range's position in the cached values. Steps over
     * nearby ranges, summing their sizes, and uses {@link RowSet#find} only on the first seek and to skip far: it
     * allocates on every call.
     */
    private static final class CachedRanges implements SafeCloseable {
        private static final int MAX_STEPS = 16;

        private final RowSet rows;
        private final RowSet.RangeIterator ranges;
        /** The current range, or {@code -1} before the first seek. */
        private long start = -1;
        private long end = -1;
        /** The position of {@link #start} in {@link #rows}. */
        private long startPosition;
        private boolean exhausted;

        private CachedRanges(@NotNull final RowSet rows) {
            this.rows = rows;
            ranges = rows.rangeIterator();
        }

        /**
         * Move to the first range that ends at or after {@code key}, which must not precede an earlier seek's.
         *
         * @return Whether there is such a range
         */
        private boolean seek(final long key) {
            for (int steps = 0; end < key; ++steps) {
                if (exhausted) {
                    return false;
                }
                if (start < 0 || steps == MAX_STEPS) {
                    if (!ranges.advance(key)) {
                        exhausted = true;
                        return false;
                    }
                    start = ranges.currentRangeStart();
                    end = ranges.currentRangeEnd();
                    startPosition = rows.find(start);
                    return true;
                }
                if (!ranges.hasNext()) {
                    exhausted = true;
                    return false;
                }
                startPosition += end - start + 1;
                ranges.next();
                start = ranges.currentRangeStart();
                end = ranges.currentRangeEnd();
            }
            return true;
        }

        /**
         * @param key A row in the current range
         */
        private long positionOf(final long key) {
            return startPosition + key - start;
        }

        @Override
        public void close() {
            ranges.close();
        }
    }

    /**
     * Holds a fill context's {@link SeekableChannelContext} and its sparse read state.
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

        /**
         * The page-relative rows this context has decoded from one page, and their values, not yet published to the
         * page's cached sparse values.
         */
        @Nullable
        private ColumnChunkPageStore<?> pendingStore;
        private int pendingPageNum;
        @Nullable
        private RowSetBuilderSequential pendingRows;
        private int pendingCount;
        @Nullable
        private Object pendingValues;

        private final SizedLongChunk<OrderedRowKeyRanges> requestRanges = new SizedLongChunk<>();
        private final SizedLongChunk<OrderedRowKeyRanges> decodeRanges = new SizedLongChunk<>();

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

        long pendingSize(@NotNull final ColumnChunkPageStore<?> store, final int pageNum) {
            return pendingStore == store && pendingPageNum == pageNum ? pendingCount : 0;
        }

        /**
         * @return A chunk with capacity for at least {@code capacity} values, owned by this context and reused by every
         *         sparse fill
         */
        @NotNull
        WritableLongChunk<OrderedRowKeyRanges> requestRanges(final int capacity) {
            return requestRanges.ensureCapacity(capacity);
        }

        /**
         * @return A second chunk like {@link #requestRanges}
         */
        @NotNull
        WritableLongChunk<OrderedRowKeyRanges> decodeRanges(final int capacity) {
            return decodeRanges.ensureCapacity(capacity);
        }

        /**
         * @param ranges Page-relative row ranges after any already pending for the page
         * @param count The number of rows in {@code ranges}
         * @param values An array of at least {@code count} values; ownership passes to this method
         */
        void appendPending(
                @NotNull final ColumnChunkPageStore<?> store,
                final int pageNum,
                @NotNull final LongChunk<OrderedRowKeyRanges> ranges,
                final int count,
                @NotNull final Object values) {
            if (pendingStore != store || pendingPageNum != pageNum) {
                publishPending();
            }
            if (pendingStore == null) {
                pendingStore = store;
                pendingPageNum = pageNum;
                pendingRows = RowSetFactory.builderSequential();
                pendingValues = values;
            } else {
                if (Array.getLength(pendingValues) < pendingCount + count) {
                    pendingValues = copyOf(pendingValues, Math.max(2 * pendingCount, pendingCount + count));
                }
                // noinspection SuspiciousSystemArraycopy: both are arrays of the page's values
                System.arraycopy(values, 0, pendingValues, pendingCount, count);
            }
            // Not via a RowSet: appending one to a builder holding a bitmap is quadratic in rows per block.
            for (int ii = 0; ii < ranges.size(); ii += 2) {
                // noinspection DataFlowIssue: set with pendingStore
                pendingRows.appendRange(ranges.get(ii), ranges.get(ii + 1));
            }
            pendingCount += count;
        }

        void publishPending() {
            final ColumnChunkPageStore<?> store = pendingStore;
            if (store == null) {
                return;
            }
            final int pageNum = pendingPageNum;
            // noinspection DataFlowIssue: set with pendingStore
            final WritableRowSet rows = pendingRows.build();
            final Object values = pendingValues;
            clearPending();
            // noinspection DataFlowIssue: set with pendingStore
            store.publishSparse(pageNum, rows, values);
        }

        void discardPending(@NotNull final ColumnChunkPageStore<?> store, final int pageNum) {
            if (pendingStore == store && pendingPageNum == pageNum) {
                clearPending();
            }
        }

        private void clearPending() {
            pendingStore = null;
            pendingRows = null;
            pendingCount = 0;
            pendingValues = null;
        }

        @Override
        public void close() {
            try {
                publishPending();
            } finally {
                super.close();
                channelContext.close();
                sparseCursor = null;
                sparseStore = null;
                requestRanges.close();
                decodeRanges.close();
            }
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
