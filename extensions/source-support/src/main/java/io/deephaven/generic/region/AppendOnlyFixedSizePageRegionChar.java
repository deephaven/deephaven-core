package io.deephaven.generic.region;

import io.deephaven.base.MathUtil;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.PageStore;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionChar;
import io.deephaven.engine.table.impl.sources.regioned.GenericColumnRegionBase;
import io.deephaven.generic.page.ChunkHolderPageChar;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.Arrays;

import static io.deephaven.base.ArrayUtil.MAX_ARRAY_SIZE;

/**
 * Region implementation that provides access to append-only pages with a fixed maximum size.
 */
public class AppendOnlyFixedSizePageRegionChar<ATTR extends Any>
        extends GenericColumnRegionBase<ATTR>
        implements PageStore<ATTR, ATTR, ChunkHolderPageChar<ATTR>>, ColumnRegionChar<ATTR> {

    private final int pageSize;
    private final AppendOnlyRegionAccessor<ATTR> accessor;

    @SuppressWarnings("unchecked")
    private volatile SoftReference<ChunkHolderPageChar<ATTR>>[] pageHolderRefs = new SoftReference[0];

    public AppendOnlyFixedSizePageRegionChar(
            final long pageMask,
            final int pageSize,
            @NotNull final AppendOnlyRegionAccessor<ATTR> accessor) {
        super(pageMask);
        this.pageSize = pageSize;
        this.accessor = accessor;
    }

    @Override
    public final ChunkType getChunkType() {
        return ChunkType.Char;
    }

    @Override
    public char getChar(final long rowKey) {
        final ChunkHolderPageChar<ATTR> page = getPageContaining(rowKey);
        try {
            return page.get(rowKey);
        } catch (Exception e) {
            throw new TableDataException(String.format("Error retrieving char at row key %s", rowKey), e);
        }
    }

    // region getBytes
    // endregion getBytes

    @Override
    @NotNull
    public final ChunkHolderPageChar<ATTR> getPageContaining(final FillContext fillContext, final long rowKey) {
        return getPageContaining(rowKey);
    }

    @NotNull
    private ChunkHolderPageChar<ATTR> getPageContaining(final long rowKey) {
        final long firstRowPosition = rowKey & mask();
        final int pageIndex = Math.toIntExact(firstRowPosition / pageSize);
        if (pageIndex >= MAX_ARRAY_SIZE) {
            throw new UnsupportedOperationException(String.format(
                    "Cannot support more than %s pages, increase page size from %s", MAX_ARRAY_SIZE, pageSize));
        }
        final long pageFirstRowInclusive = (long) pageIndex * pageSize;

        final ChunkHolderPageChar<ATTR> pageHolder = ensurePage(pageIndex, pageFirstRowInclusive);
        ensureFilled(pageHolder, pageIndex, pageFirstRowInclusive);
        return pageHolder;
    }

    private ChunkHolderPageChar<ATTR> ensurePage(final int pageIndex, final long pageFirstRowInclusive) {
        SoftReference<ChunkHolderPageChar<ATTR>>[] localPageHolderRefs;
        SoftReference<ChunkHolderPageChar<ATTR>> pageHolderRef;
        ChunkHolderPageChar<ATTR> pageHolder;
        // Look for the page
        if ((localPageHolderRefs = pageHolderRefs).length <= pageIndex
                || (pageHolderRef = localPageHolderRefs[pageIndex]) == null
                || (pageHolder = pageHolderRef.get()) == null) {
            // If we didn't find it, better grab the lock; we may need to allocate shared storage
            synchronized (this) {
                // Ensure we have enough space for the page
                if ((localPageHolderRefs = pageHolderRefs).length <= pageIndex) {
                    // Grow pageHolderRefs
                    final int numPages = Math.min(1 << MathUtil.ceilLog2(pageIndex + 1), MAX_ARRAY_SIZE);
                    pageHolderRefs = localPageHolderRefs = Arrays.copyOf(localPageHolderRefs, numPages);
                }
                // Ensure the page is allocated and stored
                if ((pageHolderRef = localPageHolderRefs[pageIndex]) == null
                        || (pageHolder = pageHolderRef.get()) == null) {
                    // Allocate the page
                    // region allocatePage
                    pageHolder = new ChunkHolderPageChar<>(mask(), pageFirstRowInclusive, new char[pageSize]);
                    // endregion allocatePage
                    pageHolderRefs[pageIndex] = new SoftReference<>(pageHolder);
                }
            }
        }
        return pageHolder;
    }

    private void ensureFilled(
            @NotNull final ChunkHolderPageChar<ATTR> pageHolder,
            final int pageIndex,
            final long pageFirstRowInclusive) {

        // If this page is already as full as it can be, don't interact with the accessor at all
        if (pageHolder.size() >= pageSize) {
            return;
        }

        final long regionSize = accessor.size();
        final long pageLastRowExclusive = Math.min(regionSize, (pageIndex + 1L) * pageSize);
        final int thisPageSize = Math.toIntExact(pageLastRowExclusive - pageFirstRowInclusive);

        // Check the current size
        if (pageHolder.size() >= thisPageSize) {
            return;
        }
        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (pageHolder) {
            // Ensure that we have enough data available
            final int currentSize = pageHolder.size();
            if (currentSize >= thisPageSize) {
                return;
            }
            // Fill the necessary page suffix
            final WritableCharChunk<ATTR> destination = pageHolder.getSliceForAppend(currentSize);
            accessor.readChunkPage(pageFirstRowInclusive + currentSize, thisPageSize - currentSize, destination);
            pageHolder.acceptAppend(destination, currentSize);
        }
    }
}
