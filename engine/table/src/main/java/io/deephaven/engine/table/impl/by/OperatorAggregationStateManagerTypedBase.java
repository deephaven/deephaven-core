/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.util.QueryConstants;

import static io.deephaven.util.SafeCloseable.closeArray;

public abstract class OperatorAggregationStateManagerTypedBase
        implements OperatorAggregationStateManager {
    public static final int CHUNK_SIZE = ChunkedOperatorAggregationHelper.CHUNK_SIZE;
    private static final long MAX_TABLE_SIZE = HashTableColumnSource.MINIMUM_OVERFLOW_HASH_SLOT;

    protected static final int EMPTY_STATE_VALUE = QueryConstants.NULL_INT;

    // the number of slots in our table
    private int tableSize;

    protected long numEntries = 0;

    /**
     * Our table size must be 2^L (i.e. a power of two); and the pivot is between 2^(L-1) and 2^L.
     *
     * <p>
     * When hashing a value, if hashCode % 2^L < tableHashPivot; then the destination location is hashCode % 2^L. If
     * hashCode % 2^L >= tableHashPivot, then the destination location is hashCode % 2^(L-1). Once the pivot reaches the
     * table size, we can simply double the table size and repeat the process.
     * </p>
     *
     * <p>
     * This has the effect of only using hash table locations < hashTablePivot. When we want to expand the table we can
     * move some of the entries from the location {@code tableHashPivot - 2^(L-1)} to tableHashPivot. This provides for
     * incremental expansion of the hash table, without the need for a full rehash.
     * </p>
     */
    protected int tableHashPivot;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;
    private final double targetLoadFactor;
    // TODO: We do not yet support contraction
    // private final double minimumLoadFactor = 0.5;

    private final IntegerArraySource freeOverflowLocations = new IntegerArraySource();
    private int freeOverflowCount = 0;

    // the keys for our hash entries
    protected final ArrayBackedColumnSource<?>[] mainKeySources;
    // the location of the first overflow entry in this bucket, parallel to keySources
    protected final IntegerArraySource mainOverflowLocationSource = new IntegerArraySource();

    // the keys for overflow
    private int nextOverflowLocation = 0;

    // the overflow chains, logically a linked list using integer pointers into these three parallel array sources
    protected final ArrayBackedColumnSource<?>[] overflowKeySources;
    // the location of the next key in an overflow bucket, parallel with overflowKeySources
    protected final IntegerArraySource overflowOverflowLocationSource = new IntegerArraySource();


    protected OperatorAggregationStateManagerTypedBase(ColumnSource<?>[] tableKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        this.tableHashPivot = tableSize;

        mainKeySources = new ArrayBackedColumnSource[tableKeySources.length];
        overflowKeySources = new ArrayBackedColumnSource[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            // the sources that we will use to store our hash table
            mainKeySources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(tableSize, tableKeySources[ii].getType());
            overflowKeySources[ii] =
                    ArrayBackedColumnSource.getMemoryColumnSource(0, tableKeySources[ii].getType());
        }

        this.maximumLoadFactor = maximumLoadFactor;
        this.targetLoadFactor = targetLoadFactor;

        ensureCapacity(tableSize);
    }

    protected void ensureCapacity(int tableSize) {
        mainOverflowLocationSource.ensureCapacity(tableSize);
        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            mainKeySources[ii].ensureCapacity(tableSize);
        }
    }

    protected abstract void ensureOverflowState(int newCapacity);

    private void ensureOverflowCapacity(final int locationsToAllocate) {
        if (freeOverflowCount >= locationsToAllocate) {
            return;
        }
        final int newCapacity = nextOverflowLocation + locationsToAllocate - freeOverflowCount;
        overflowOverflowLocationSource.ensureCapacity(newCapacity);
        ensureOverflowState(newCapacity);
        // noinspection ForLoopReplaceableByForEach
        for (int ii = 0; ii < overflowKeySources.length; ++ii) {
            overflowKeySources[ii].ensureCapacity(newCapacity);
        }
    }

    protected abstract void build(HashHandler handler, RowSequence rowSequence,
            Chunk<Values>[] sourceKeyChunks);

    protected abstract void probe(HashHandler handler, RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks);

    static class BuildContext extends BuildOrProbeContext {
        private BuildContext(ColumnSource<?>[] buildSources, int chunkSize) {
            super(buildSources, chunkSize);
        }
    }
    static class ProbeContext extends BuildOrProbeContext {
        private ProbeContext(ColumnSource<?>[] buildSources, int chunkSize) {
            super(buildSources, chunkSize);
        }
    }

    static class BuildOrProbeContext implements Context {
        final int chunkSize;
        final SharedContext sharedContext;
        final ChunkSource.GetContext[] getContexts;

        private BuildOrProbeContext(ColumnSource<?>[] buildSources, int chunkSize) {
            Assert.gtZero(chunkSize, "chunkSize");
            this.chunkSize = chunkSize;
            final boolean haveSharedContexts = buildSources.length > 1;
            if (haveSharedContexts) {
                sharedContext = SharedContext.makeSharedContext();
            } else {
                // no point in the additional work implied by these not being null.
                sharedContext = null;
            }
            getContexts = makeGetContexts(buildSources, sharedContext, chunkSize);
        }

        void resetSharedContexts() {
            if (sharedContext != null) {
                sharedContext.reset();
            }
        }

        void closeSharedContexts() {
            if (sharedContext != null) {
                sharedContext.close();
            }
        }

        @Override
        public void close() {
            closeArray(getContexts);
            closeSharedContexts();
        }
    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    public ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    protected void buildTable(
            final HashHandler handler,
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources) {
        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                ensureOverflowCapacity(chunkOk.intSize());
                handler.nextChunk(chunkOk.intSize());

                getKeyChunks(buildSources, bc.getContexts, sourceKeyChunks, chunkOk);

                build(handler, chunkOk, sourceKeyChunks);

                bc.resetSharedContexts();

                doRehash(handler);
            }
        }
    }

    protected void probeTable(
            final HashHandler handler,
            final ProbeContext pc,
            final RowSequence probeRows,
            final boolean usePrev,
            final ColumnSource<?>[] probeSources) {
        try (final RowSequence.Iterator rsIt = probeRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[probeSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(pc.chunkSize);
                ensureOverflowCapacity(chunkOk.intSize());
                handler.nextChunk(chunkOk.intSize());

                if (usePrev) {
                    getPrevKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, chunkOk);
                } else {
                    getKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, chunkOk);
                }

                probe(handler, chunkOk, sourceKeyChunks);

                pc.resetSharedContexts();
            }
        }
    }

    public void doRehash(HashHandler handler) {
        while (rehashRequired()) {
            if (tableHashPivot == tableSize) {
                tableSize *= 2;
            }

            final long targetBuckets = Math.min(MAX_TABLE_SIZE, (long) (numEntries / targetLoadFactor));
            final int bucketsToAdd = Math.max(1, (int) Math.min(targetBuckets, tableSize) - tableHashPivot);

            ensureCapacity(tableHashPivot + bucketsToAdd);

            freeOverflowLocations.ensureCapacity(freeOverflowCount + bucketsToAdd, false);

            for (int ii = 0; ii < bucketsToAdd; ++ii) {
                int checkBucket = tableHashPivot + ii - (tableSize >> 1);
                int destBucket = tableHashPivot + ii;
                rehashBucket(handler, checkBucket, destBucket, bucketsToAdd);
            }
            tableHashPivot += bucketsToAdd;
        }
    }

    protected abstract void rehashBucket(HashHandler handler, int bucket, int destBucket, int bucketsToAdd);

    public boolean rehashRequired() {
        return numEntries > (tableHashPivot * maximumLoadFactor) && tableHashPivot < MAX_TABLE_SIZE;
    }

    protected int allocateOverflowLocation() {
        if (freeOverflowCount > 0) {
            return freeOverflowLocations.getUnsafe(--freeOverflowCount);
        }
        return nextOverflowLocation++;
    }

    protected void freeOverflowLocation(int location) {
        freeOverflowLocations.ensureCapacity(freeOverflowCount + CHUNK_SIZE, false);
        freeOverflowLocations.set(freeOverflowCount++, location);
    }

    private void getKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getChunk(contexts[ii], rowSequence);
        }
    }

    private void getPrevKeyChunks(ColumnSource<?>[] sources, ColumnSource.GetContext[] contexts,
            Chunk<? extends Values>[] chunks, RowSequence rowSequence) {
        for (int ii = 0; ii < chunks.length; ++ii) {
            chunks[ii] = sources[ii].getPrevChunk(contexts[ii], rowSequence);
        }
    }

    protected int hashToTableLocation(int pivotPoint, int hash) {
        int location = hash & (tableSize - 1);
        if (location >= pivotPoint) {
            location -= (tableSize >> 1);
        }
        return location;
    }

    @Override
    abstract public int findPositionForKey(Object key);

    private static ColumnSource.GetContext[] makeGetContexts(ColumnSource<?>[] sources, final SharedContext sharedState,
            int chunkSize) {
        final ColumnSource.GetContext[] contexts = new ColumnSource.GetContext[sources.length];
        for (int ii = 0; ii < sources.length; ++ii) {
            contexts[ii] = sources[ii].makeGetContext(chunkSize, sharedState);
        }
        return contexts;
    }
}
