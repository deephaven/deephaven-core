//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.function.BiConsumer;

/**
 * Base for {@link RowSequence} implementations that cache the results of {@link #asRowKeyChunk()} and
 * {@link #asRowKeyRangesChunk()}.
 * <p>
 * Concurrent readers of an unmodified sequence may call either method. A chunk is filled while only the filling thread
 * can see it, and is published with a compare-and-set; a published chunk is never refilled. Invalidation moves the
 * published chunk to a stale slot, from which a later build may claim it exclusively and refill it. Invalidation and
 * {@link #close()} are mutations, so they must not run concurrently with readers, and a reader must not use a chunk
 * after the sequence is modified.
 */
public abstract class RowSequenceAsChunkImpl implements RowSequence {

    private static final VarHandle KEY_INDICES_CHUNK;
    private static final VarHandle STALE_KEY_INDICES_CHUNK;
    private static final VarHandle KEY_RANGES_CHUNK;
    private static final VarHandle STALE_KEY_RANGES_CHUNK;
    static {
        try {
            final MethodHandles.Lookup lookup = MethodHandles.lookup();
            KEY_INDICES_CHUNK = lookup.findVarHandle(
                    RowSequenceAsChunkImpl.class, "keyIndicesChunk", WritableLongChunk.class);
            STALE_KEY_INDICES_CHUNK = lookup.findVarHandle(
                    RowSequenceAsChunkImpl.class, "staleKeyIndicesChunk", WritableLongChunk.class);
            KEY_RANGES_CHUNK = lookup.findVarHandle(
                    RowSequenceAsChunkImpl.class, "keyRangesChunk", WritableLongChunk.class);
            STALE_KEY_RANGES_CHUNK = lookup.findVarHandle(
                    RowSequenceAsChunkImpl.class, "staleKeyRangesChunk", WritableLongChunk.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * The published row keys chunk, valid for the current contents of this sequence, or null.
     */
    private volatile WritableLongChunk<OrderedRowKeys> keyIndicesChunk;
    /**
     * A row keys chunk that no reader may use, available for reuse by the next build, or null.
     */
    private volatile WritableLongChunk<OrderedRowKeys> staleKeyIndicesChunk;
    /**
     * The published row key ranges chunk, valid for the current contents of this sequence, or null.
     */
    private volatile WritableLongChunk<OrderedRowKeyRanges> keyRangesChunk;
    /**
     * A row key ranges chunk that no reader may use, available for reuse by the next build, or null.
     */
    private volatile WritableLongChunk<OrderedRowKeyRanges> staleKeyRangesChunk;

    protected long runsUpperBound() {
        final long size = size();
        final long range = lastRowKey() - firstRowKey() + 1;
        final long holesUpperBound = range - size;
        final long runsUpperBound = 1 + holesUpperBound;
        return runsUpperBound;
    }

    private int sizeForRangesChunk() {
        final long runsUpperBound = runsUpperBound();
        if (runsUpperBound <= 1024) {
            return 2 * (int) runsUpperBound;
        }
        final long rangesCount = rangesCountUpperBound();
        return 2 * (int) rangesCount;
    }

    @Override
    public final LongChunk<OrderedRowKeys> asRowKeyChunk() {
        if (size() == 0) {
            return LongChunk.getEmptyChunk();
        }
        final WritableLongChunk<OrderedRowKeys> published = keyIndicesChunk;
        if (published != null) {
            return published;
        }
        return buildChunk(KEY_INDICES_CHUNK, STALE_KEY_INDICES_CHUNK, intSize(),
                RowSequenceAsChunkImpl::fillRowKeyChunk);
    }

    @Override
    public final LongChunk<OrderedRowKeyRanges> asRowKeyRangesChunk() {
        if (size() == 0) {
            return LongChunk.getEmptyChunk();
        }
        final WritableLongChunk<OrderedRowKeyRanges> published = keyRangesChunk;
        if (published != null) {
            return published;
        }
        return buildChunk(KEY_RANGES_CHUNK, STALE_KEY_RANGES_CHUNK, sizeForRangesChunk(),
                RowSequenceAsChunkImpl::fillRowKeyRangesChunk);
    }

    /**
     * Fill a chunk that only this thread can see, reusing the stale chunk when it is large enough, and publish it.
     *
     * @param publishedHandle The handle for the published chunk field
     * @param staleHandle The handle for the stale chunk field of the same kind
     * @param capacity The capacity the chunk needs
     * @param filler Fills the chunk from this sequence
     * @return The published chunk, which is ours unless another reader published first
     */
    @SuppressWarnings("unchecked")
    private <ATTR extends Any> LongChunk<ATTR> buildChunk(
            final VarHandle publishedHandle,
            final VarHandle staleHandle,
            final int capacity,
            final BiConsumer<RowSequenceAsChunkImpl, WritableLongChunk<ATTR>> filler) {
        WritableLongChunk<ATTR> chunk =
                (WritableLongChunk<ATTR>) staleHandle.getAndSet(this, (WritableLongChunk<ATTR>) null);
        if (chunk != null && chunk.capacity() < capacity) {
            ChunkPoolReleaseTracking.untracked(chunk::close);
            chunk = null;
        }
        if (chunk == null) {
            chunk = ChunkPoolReleaseTracking.untracked(() -> WritableLongChunk.makeWritableChunk(capacity));
        } else {
            chunk.setSize(chunk.capacity());
        }
        filler.accept(this, chunk);
        final WritableLongChunk<ATTR> witness = (WritableLongChunk<ATTR>) publishedHandle.compareAndExchange(
                this, (WritableLongChunk<ATTR>) null, chunk);
        if (witness == null) {
            return chunk;
        }
        // Another reader published first; our chunk was never visible to anyone else.
        ChunkPoolReleaseTracking.untracked(chunk::close);
        return witness;
    }

    abstract public long lastRowKey();

    abstract public long rangesCountUpperBound();

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() {
        closeRowSequenceAsChunkImpl();
    }

    /**
     * Close any resources associated with this RowSequenceAsChunkImpl. This is the implementation for {@link #close()
     * close}, made available for subclasses that have a need to release parent class resources independently of their
     * own {@link #close() close} implementation. Most uses should prefer to {@link #invalidateRowSequenceAsChunkImpl()
     * invalidate}, instead.
     */
    protected final void closeRowSequenceAsChunkImpl() {
        final WritableLongChunk<OrderedRowKeys> keys = keyIndicesChunk;
        if (keys != null) {
            KEY_INDICES_CHUNK.setRelease(this, null);
            ChunkPoolReleaseTracking.untracked(keys::close);
        }
        final WritableLongChunk<OrderedRowKeys> staleKeys = staleKeyIndicesChunk;
        if (staleKeys != null) {
            STALE_KEY_INDICES_CHUNK.setRelease(this, null);
            ChunkPoolReleaseTracking.untracked(staleKeys::close);
        }
        final WritableLongChunk<OrderedRowKeyRanges> ranges = keyRangesChunk;
        if (ranges != null) {
            KEY_RANGES_CHUNK.setRelease(this, null);
            ChunkPoolReleaseTracking.untracked(ranges::close);
        }
        final WritableLongChunk<OrderedRowKeyRanges> staleRanges = staleKeyRangesChunk;
        if (staleRanges != null) {
            STALE_KEY_RANGES_CHUNK.setRelease(this, null);
            ChunkPoolReleaseTracking.untracked(staleRanges::close);
        }
    }

    /**
     * Invalidate the cached chunks after a modification. The published chunks become stale and may be refilled by the
     * next build, so no reader may use a chunk obtained before the modification.
     */
    protected final void invalidateRowSequenceAsChunkImpl() {
        final WritableLongChunk<OrderedRowKeys> keys = keyIndicesChunk;
        if (keys != null) {
            // The build that published keys emptied the stale slot, and only invalidation stores into it.
            Assert.eqNull(staleKeyIndicesChunk, "staleKeyIndicesChunk");
            KEY_INDICES_CHUNK.setRelease(this, null);
            STALE_KEY_INDICES_CHUNK.setRelease(this, keys);
        }
        final WritableLongChunk<OrderedRowKeyRanges> ranges = keyRangesChunk;
        if (ranges != null) {
            Assert.eqNull(staleKeyRangesChunk, "staleKeyRangesChunk");
            KEY_RANGES_CHUNK.setRelease(this, null);
            STALE_KEY_RANGES_CHUNK.setRelease(this, ranges);
        }
    }
}
