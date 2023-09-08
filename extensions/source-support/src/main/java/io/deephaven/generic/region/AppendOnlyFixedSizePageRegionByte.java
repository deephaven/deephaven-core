/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit AppendOnlyFixedSizePageRegionChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.generic.region;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;

import io.deephaven.base.MathUtil;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.page.PageStore;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.sources.regioned.ColumnRegionByte;
import io.deephaven.engine.table.impl.sources.regioned.GenericColumnRegionBase;
import io.deephaven.generic.page.ChunkHolderPageByte;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.util.Arrays;

import static io.deephaven.base.ArrayUtil.MAX_ARRAY_SIZE;

/**
 * Region implementation that provides access to append-only pages with a fixed maximum size.
 */
public class AppendOnlyFixedSizePageRegionByte<ATTR extends Any>
        extends GenericColumnRegionBase<ATTR>
        implements PageStore<ATTR, ATTR, ChunkHolderPageByte<ATTR>>, ColumnRegionByte<ATTR> {

    private final int pageSize;
    private final AppendOnlyRegionAccessor<ATTR> accessor;

    @SuppressWarnings("unchecked")
    private volatile SoftReference<ChunkHolderPageByte<ATTR>>[] pageHolderRefs = new SoftReference[0];

    public AppendOnlyFixedSizePageRegionByte(
            final long pageMask,
            final int pageSize,
            @NotNull final AppendOnlyRegionAccessor<ATTR> accessor) {
        super(pageMask);
        this.pageSize = pageSize;
        this.accessor = accessor;
    }

    @Override
    public byte getByte(final long rowKey) {
        final ChunkHolderPageByte<ATTR> page = getPageContaining(rowKey);
        try {
            return page.get(rowKey);
        } catch (Exception e) {
            throw new TableDataException(String.format("Error retrieving byte at row key %s", rowKey), e);
        }
    }

    // region getBytes
    public byte[] getBytes(
            final long firstRowKey,
            @NotNull final byte[] destination,
            final int destinationOffset,
            final int length
    ) {
        final WritableChunk<ATTR> byteChunk = WritableByteChunk.writableChunkWrap(destination, destinationOffset, length);
        try (RowSequence rowSequence = RowSequenceFactory.forRange(firstRowKey, firstRowKey + length - 1)) {
            fillChunk(DEFAULT_FILL_INSTANCE, byteChunk, rowSequence);
        }
        return destination;
    }
    // endregion getBytes

    @Override
    @NotNull
    public final ChunkHolderPageByte<ATTR> getPageContaining(final FillContext fillContext, final long rowKey) {
        return getPageContaining(rowKey);
    }

    @NotNull
    private ChunkHolderPageByte<ATTR> getPageContaining(final long rowKey) {
        throwIfInvalidated();
        final long firstRowPosition = rowKey & mask();
        final int pageIndex = Math.toIntExact(firstRowPosition / pageSize);
        if (pageIndex >= MAX_ARRAY_SIZE) {
            throw new UnsupportedOperationException(String.format(
                    "Cannot support more than %s pages, increase page size from %s", MAX_ARRAY_SIZE, pageSize));
        }
        final long pageFirstRowInclusive = (long) pageIndex * pageSize;

        final ChunkHolderPageByte<ATTR> pageHolder = ensurePage(pageIndex, pageFirstRowInclusive);
        ensureFilled(pageHolder, pageIndex, pageFirstRowInclusive);
        return pageHolder;
    }

    private ChunkHolderPageByte<ATTR> ensurePage(final int pageIndex, final long pageFirstRowInclusive) {
        SoftReference<ChunkHolderPageByte<ATTR>>[] localPageHolderRefs;
        SoftReference<ChunkHolderPageByte<ATTR>> pageHolderRef;
        ChunkHolderPageByte<ATTR> pageHolder;
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
                    pageHolder = new ChunkHolderPageByte<>(mask(), pageFirstRowInclusive, new byte[pageSize]);
                    // endregion allocatePage
                    pageHolderRefs[pageIndex] = new SoftReference<>(pageHolder);
                }
            }
        }
        return pageHolder;
    }

    private void ensureFilled(
            @NotNull final ChunkHolderPageByte<ATTR> pageHolder,
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
            final WritableByteChunk<ATTR> destination = pageHolder.getSliceForAppend(currentSize);
            accessor.readChunkPage(pageFirstRowInclusive + currentSize, thisPageSize - currentSize, destination);
            pageHolder.acceptAppend(destination, currentSize);
        }
    }
}
