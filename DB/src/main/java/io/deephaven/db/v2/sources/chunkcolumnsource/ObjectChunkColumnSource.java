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
 * A column source backed by {@link ObjectChunk ObjectChunks}.
 * <p>
 * The address space of the column source is dense, with each chunk backing a contiguous set of indices.  The
 * {@link #getChunk(GetContext, OrderedKeys)}
 * call will return the backing chunk or a slice of the backing chunk if possible.
 */
public class ObjectChunkColumnSource<T> extends AbstractColumnSource<T> implements ImmutableColumnSourceGetDefaults.ForObject<T>, ChunkColumnSource<T> {
    private final ArrayList<WritableObjectChunk<T, ? extends Attributes.Values>> data = new ArrayList<>();
    private final TLongArrayList firstOffsetForData;
    private long totalSize = 0;

    // region constructor
    protected ObjectChunkColumnSource(Class<T> type, Class<?> componentType) {
        this(type, componentType, new TLongArrayList());
    }

    protected ObjectChunkColumnSource(Class<T> type, Class<?> componentType, final TLongArrayList firstOffsetForData) {
        super(type, componentType);
        this.firstOffsetForData = firstOffsetForData;
    }
    // endregion constructor

    @Override
    public T get(final long index) {
        if (index < 0 || index >= totalSize) {
            return null;
        }

        final int chunkIndex = getChunkIndex(index);
        final long offset = firstOffsetForData.getQuick(chunkIndex);
        return data.get(chunkIndex).get((int) (index - offset));
    }

    private final static class ChunkGetContext<ATTR extends Attributes.Any> extends DefaultGetContext<ATTR> {
        private final ResettableObjectChunk resettableObjectChunk = ResettableObjectChunk.makeResettableChunk();

        public ChunkGetContext(final ChunkSource<ATTR> chunkSource, final int chunkCapacity, final SharedContext sharedContext) {
            super(chunkSource, chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            resettableObjectChunk.close();
            super.close();
        }
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new ChunkGetContext(this, chunkCapacity, sharedContext);
    }

    @Override
    public Chunk<? extends Attributes.Values> getChunk(@NotNull final GetContext context, @NotNull final OrderedKeys orderedKeys) {
        // if we can slice part of one of our backing chunks, then we will return that instead
        if (orderedKeys.isContiguous()) {
            final long firstKey = orderedKeys.firstKey();
            final int firstChunk = getChunkIndex(firstKey);
            final int lastChunk = getChunkIndex(orderedKeys.lastKey(), firstChunk);
            if (firstChunk == lastChunk) {
                final int offset = (int) (firstKey - firstOffsetForData.get(firstChunk));
                final int length = orderedKeys.intSize();
                final ObjectChunk<T, ? extends Attributes.Values> ObjectChunk = data.get(firstChunk);
                if (offset == 0 && length == ObjectChunk.size()) {
                    return ObjectChunk;
                }
                return ((ChunkGetContext) context).resettableObjectChunk.resetFromChunk(ObjectChunk, offset, length);
            }
        }
        return getChunkByFilling(context, orderedKeys);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Attributes.Values> destination, @NotNull final OrderedKeys orderedKeys) {
        final MutableInt searchStartChunkIndex = new MutableInt(0);
        destination.setSize(0);
        orderedKeys.forAllLongRanges((s, e) -> {
            while (s <= e) {
                final int chunkIndex = getChunkIndex(s, searchStartChunkIndex.intValue());
                final int offsetWithinChunk = (int) (s - firstOffsetForData.get(chunkIndex));
                Assert.geqZero(offsetWithinChunk, "offsetWithinChunk");
                final ObjectChunk<T, ? extends Attributes.Values> ObjectChunk = data.get(chunkIndex);
                final int chunkSize = ObjectChunk.size();
                final long rangeLength = e - s + 1;
                final int chunkRemaining = chunkSize - offsetWithinChunk;
                final int length = rangeLength > chunkRemaining ? chunkRemaining : (int) rangeLength;
                Assert.gtZero(length, "length");
                final int currentDestinationSize = destination.size();
                destination.copyFromChunk(ObjectChunk, offsetWithinChunk, currentDestinationSize, length);
                destination.setSize(currentDestinationSize + length);
                searchStartChunkIndex.setValue(chunkIndex + 1);
                s += length;
            }
        });
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context, @NotNull final WritableChunk<? super Attributes.Values> destination, @NotNull final OrderedKeys orderedKeys) {
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
        if (start == firstOffsetForData.get(startChunk)) {
            return startChunk;
        }
        int index = firstOffsetForData.binarySearch(start, startChunk, firstOffsetForData.size());
        if (index < 0) {
            index = -index - 2;
        }
        return index;
    }

    /**
     * Append a chunk of data to this column source.
     *
     * The chunk must not be empty (i.e., the size must be greater than zero).
     *
     * @param chunk the chunk of data to add
     */
    public void addChunk(@NotNull final WritableObjectChunk<T, ? extends Attributes.Values> chunk) {
        Assert.gtZero(chunk.size(), "chunk.size()");
        data.add(chunk);
        if (data.size() > firstOffsetForData.size()) {
            firstOffsetForData.add(totalSize);
        }
        totalSize += chunk.size();
    }

    @Override
    public void addChunk(@NotNull final WritableChunk<? extends Attributes.Values> chunk) {
        addChunk(chunk.asWritableObjectChunk());
    }

    @Override
    public void clear() {
        totalSize = 0;
        data.forEach(SafeCloseable::close);
        data.clear();
        firstOffsetForData.resetQuick();
    }

    @Override
    public long getSize() {
        return totalSize;
    }
}
