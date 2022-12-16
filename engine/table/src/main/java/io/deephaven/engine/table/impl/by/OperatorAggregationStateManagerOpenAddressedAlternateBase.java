/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import org.apache.commons.lang3.mutable.MutableInt;

import static io.deephaven.engine.table.impl.util.TypedHasherUtil.*;

public abstract class OperatorAggregationStateManagerOpenAddressedAlternateBase
        implements OperatorAggregationStateManager {
    public static final int CHUNK_SIZE = ChunkedOperatorAggregationHelper.CHUNK_SIZE;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size

    /** The number of slots in our table. */
    protected int tableSize;

    /**
     * The number of slots in our alternate table, to start with "1" is a lie, but rehashPointer is zero; so our
     * location value is positive and can be compared against rehashPointer safely
     */
    protected int alternateTableSize = 1;

    /** Should we rehash the entire table fully ({@code true}) or incrementally ({@code false})? */
    protected boolean fullRehash = true;

    /** How much of the alternate sources are necessary to rehash? */
    protected int rehashPointer = 0;

    protected long numEntries = 0;

    /**
     * The table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor or if
     * it falls below minimum load factor we will instead contract the table.
     */
    private final double maximumLoadFactor;

    /** The keys for our hash entries. */
    protected final WritableColumnSource[] mainKeySources;

    /** The keys for our hash entries, for the old alternative smaller table. */
    protected final ColumnSource[] alternateKeySources;

    protected OperatorAggregationStateManagerOpenAddressedAlternateBase(ColumnSource<?>[] tableKeySources,
            int tableSize,
            double maximumLoadFactor) {
        this.tableSize = tableSize;
        Require.inRange(tableSize, "tableSize", MAX_TABLE_SIZE + 1, "MAX_TABLE_SIZE + 1");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        alternateKeySources = new ColumnSource[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;
    }

    @Override
    public final int maxTableSize() {
        return Math.toIntExact(MAX_TABLE_SIZE);
    }

    protected abstract void build(RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks);

    public static class BuildContext extends BuildOrProbeContext {
        private BuildContext(ColumnSource<?>[] buildSources, int chunkSize) {
            super(buildSources, chunkSize);
        }

        final MutableInt rehashCredits = new MutableInt(0);
    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    public ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    protected abstract void onNextChunk(int nextChunkSize);

    protected void buildTable(
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources,
            final BuildHandler buildHandler) {
        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                final int nextChunkSize = chunkOk.intSize();
                onNextChunk(nextChunkSize);
                while (doRehash(bc.rehashCredits, nextChunkSize)) {
                    migrateFront();
                }

                getKeyChunks(buildSources, bc.getContexts, sourceKeyChunks, chunkOk);

                final long oldEntries = numEntries;
                buildHandler.doBuild(chunkOk, sourceKeyChunks);
                final long entriesAdded = numEntries - oldEntries;
                // if we actually added anything, then take away from the "equity" we've built up rehashing, otherwise
                // don't penalize this build call with additional rehashing
                bc.rehashCredits.subtract(entriesAdded);

                bc.resetSharedContexts();
            }
        }
    }

    abstract protected void migrateFront();

    protected void probeTable(
            final ProbeContext pc,
            final RowSequence probeRows,
            final boolean usePrev,
            final ColumnSource<?>[] probeSources,
            final ProbeHandler handler) {
        try (final RowSequence.Iterator rsIt = probeRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[probeSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(pc.chunkSize);

                if (usePrev) {
                    getPrevKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, chunkOk);
                } else {
                    getKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, chunkOk);
                }

                handler.doProbe(chunkOk, sourceKeyChunks);

                pc.resetSharedContexts();
            }
        }
    }

    @FunctionalInterface
    public interface ProbeHandler {
        void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks);
    }

    @FunctionalInterface
    public interface BuildHandler {
        void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks);
    }

    /**
     * @param rehashCredits the number of entries this operation has rehashed (input/output)
     * @param nextChunkSize the size of the chunk we are processing
     * @return true if a front migration is required
     */
    public boolean doRehash(MutableInt rehashCredits, int nextChunkSize) {
        if (rehashPointer > 0) {
            final int requiredRehash = nextChunkSize - rehashCredits.intValue();
            if (requiredRehash <= 0) {
                return false;
            }

            // before building, we need to do at least as much rehash work as we would do build work
            rehashCredits.add(rehashInternalPartial(requiredRehash));
            if (rehashPointer == 0) {
                clearAlternate();
            }
        }

        int oldTableSize = tableSize;
        while (rehashRequired(nextChunkSize)) {
            tableSize *= 2;

            if (tableSize < 0 || tableSize > MAX_TABLE_SIZE) {
                throw new UnsupportedOperationException("Hash table exceeds maximum size!");
            }
        }

        if (oldTableSize == tableSize) {
            return false;
        }

        // we can't give the caller credit for rehashes with the old table, we need to begin migrating things again
        if (rehashCredits.intValue() > 0) {
            rehashCredits.setValue(0);
        }

        if (fullRehash) {
            // if we are doing a full rehash, we need to ditch the alternate
            if (rehashPointer > 0) {
                rehashInternalPartial((int) numEntries);
                clearAlternate();
            }

            rehashInternalFull(oldTableSize);

            return false;
        }

        Assert.eqZero(rehashPointer, "rehashPointer");

        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = mainKeySources[ii];
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    alternateKeySources[ii].getType(), alternateKeySources[ii].getComponentType());
            mainKeySources[ii].ensureCapacity(tableSize);
        }
        alternateTableSize = oldTableSize;
        if (numEntries > 0) {
            rehashPointer = alternateTableSize;
        }

        newAlternate();

        return true;
    }

    protected abstract void newAlternate();

    protected void clearAlternate() {
        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = null;
        }
    }

    /**
     * @param numEntriesToRehash number of entries to rehash into main table
     * @return actual number of entries rehashed
     */
    protected abstract int rehashInternalPartial(int numEntriesToRehash);

    // full rehashInternal
    protected abstract void rehashInternalFull(int oldSize);

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    protected int hashToTableLocationAlternate(int hash) {
        return hash & (alternateTableSize - 1);
    }

    @Override
    abstract public int findPositionForKey(Object key);
}
