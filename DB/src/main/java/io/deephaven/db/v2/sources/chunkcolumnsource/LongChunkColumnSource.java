/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkColumnSource and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.chunkcolumnsource;

import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ImmutableColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;

/**
 * A column source backed by {@link LongChunk LongChunks}.
 * <p>
 * The address space of the column source is dense, with each chunk backing a contiguous set of indices.  The
 * {@link #getChunk(GetContext, OrderedKeys)}
 * call will return the backing chunk or a slice of the backing chunk if possible.
 */
public class LongChunkColumnSource extends AbstractColumnSource<Long> implements ImmutableColumnSourceGetDefaults.ForLong, ChunkColumnSource<Long> {
    private final ArrayList<WritableLongChunk<? extends Attributes.Values>> data = new ArrayList<>();
    private final TLongArrayList offsets = new TLongArrayList();
    private long totalSize = 0;

    // region constructor
    protected LongChunkColumnSource() {
        super(Long.class);
    }
    // endregion constructor

    @Override
    public long getLong(long index) {
        if (index < 0 || index >= totalSize) {
            return QueryConstants.NULL_LONG;
        }

        final int chunkIndex = getChunkIndex(index);
        final long offset = offsets.getQuick(chunkIndex);
        return data.get(chunkIndex).get((int) (index - offset));
    }

    private final static class ChunkGetContext<ATTR extends Attributes.Any> extends DefaultGetContext<ATTR> {
        private final ResettableLongChunk resettableLongChunk = ResettableLongChunk.makeResettableChunk();

        public ChunkGetContext(ChunkSource<ATTR> chunkSource, int chunkCapacity, SharedContext sharedContext) {
            super(chunkSource, chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            resettableLongChunk.close();
            super.close();
        }
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return new ChunkGetContext(this, chunkCapacity, sharedContext);
    }

    @Override
    public Chunk<? extends Attributes.Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        // if we can slice part of one of our backing chunks, then we will return that instead
        if (orderedKeys.isContiguous()) {
            final long firstKey = orderedKeys.firstKey();
            final int firstChunk = getChunkIndex(firstKey);
            final int lastChunk = getChunkIndex(orderedKeys.lastKey(), firstChunk);
            if (firstChunk == lastChunk) {
                final int offset = (int) (firstKey - offsets.get(firstChunk));
                final int length = orderedKeys.intSize();
                final LongChunk<? extends Attributes.Values> longChunk = data.get(firstChunk);
                if (offset == 0 && length == longChunk.size()) {
                    return longChunk;
                }
                return ((ChunkGetContext) context).resettableLongChunk.resetFromChunk(longChunk, offset, length);
            }
        }
        return getChunkByFilling(context, orderedKeys);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
        final MutableInt searchStartChunkIndex = new MutableInt(0);
        final MutableInt destinationOffset = new MutableInt(0);
        orderedKeys.forAllLongRanges((s, e) -> {
            while (s <= e) {
                final int chunkIndex = getChunkIndex(s, searchStartChunkIndex.intValue());
                final int offsetWithinChunk = (int) (s - offsets.get(chunkIndex));
                Assert.geqZero(offsetWithinChunk, "offsetWithinChunk");
                final LongChunk<? extends Attributes.Values> longChunk = data.get(chunkIndex);
                final int chunkSize = longChunk.size();
                final long rangeLength = e - s + 1;
                final int chunkRemaining = chunkSize - offsetWithinChunk;
                final int length = rangeLength > chunkRemaining ? chunkRemaining : (int) rangeLength;
                Assert.gtZero(length, "length");
                destination.copyFromChunk(longChunk, offsetWithinChunk, destinationOffset.intValue(), length);
                destinationOffset.add(length);
                searchStartChunkIndex.setValue(chunkIndex + 1);
                s += length;
            }
        });
        destination.setSize(destinationOffset.intValue());
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Attributes.Values> destination, @NotNull OrderedKeys orderedKeys) {
        // immutable, so we can delegate to fill
        fillChunk(context, destination, orderedKeys);
    }

    /**
     * Given an index within this column's address space; return the chunk that contains the index.
     *
     * @param start the data index to find the corresponding chunk for
     * @return the chunk index within data and offsets
     */
    private int getChunkIndex(final long start) {
        return getChunkIndex(start, 0);
    }

    /**
     * Given an index within this column's address space; return the chunk that contains the index.
     *
     * @param start      the data index to find the corresponding chunk for
     * @param startChunk the first chunk that may possibly contain start
     * @return the chunk index within data and offsets
     */

    private int getChunkIndex(final long start, final int startChunk) {
        int index = offsets.binarySearch(start, startChunk, offsets.size());
        if (index < 0) {
            index = -index - 2;
        }
        return index;
    }

    private void addChunk(final WritableLongChunk<? extends Attributes.Values> chunk) {
        data.add(chunk);
        offsets.add(totalSize);
        totalSize += chunk.size();
    }

    @Override
    public void addChunk(final WritableChunk<? extends Attributes.Values> chunk) {
        addChunk(chunk.asWritableLongChunk());
    }

    @Override
    public void clear() {
        totalSize = 0;
        data.forEach(SafeCloseable::close);
        data.clear();
        offsets.resetQuick();
    }
}
