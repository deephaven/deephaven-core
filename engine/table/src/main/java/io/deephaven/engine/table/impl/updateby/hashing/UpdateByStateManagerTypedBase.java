//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.updateby.hashing;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class UpdateByStateManagerTypedBase extends UpdateByStateManager {
    private static final int CHUNK_SIZE = 4096;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size

    protected static final int EMPTY_RIGHT_VALUE = QueryConstants.NULL_INT;

    // the number of slots in our table
    protected int tableSize;

    // the number of slots in our alternate table, to start with "1" is a lie, but rehashPointer is zero; so our
    // location value is positive and can be compared against rehashPointer safely
    protected int alternateTableSize = 1;

    // how much of the alternate sources are necessary to rehash?
    protected int rehashPointer = 0;

    protected long numEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ChunkType[] chunkTypes;
    protected WritableColumnSource[] mainKeySources;
    protected WritableColumnSource[] alternateKeySources;

    protected ImmutableIntArraySource stateSource = new ImmutableIntArraySource();
    protected ImmutableIntArraySource alternateStateSource = new ImmutableIntArraySource();

    // the mask for insertion into the main table (this is used so that we can identify whether a slot belongs to the
    // main or alternate table)
    protected int mainInsertMask = 0;
    protected int alternateInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;

    protected UpdateByStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] keySourcesForErrorMessages, int tableSize, double maximumLoadFactor) {

        super(keySourcesForErrorMessages);

        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        alternateKeySources = new WritableColumnSource[tableKeySources.length];
        chunkTypes = new ChunkType[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            chunkTypes[ii] = tableKeySources[ii].getChunkType();
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;

        ensureCapacity(tableSize);
    }

    private void ensureCapacity(int tableSize) {
        for (WritableColumnSource<?> mainKeySource : mainKeySources) {
            mainKeySource.ensureCapacity(tableSize);
        }
        stateSource.ensureCapacity(tableSize);
    }

    private class UpdateByBuildHandler implements TypedHasherUtil.BuildHandler {
        final MutableInt nextOutputPosition;
        final WritableIntChunk<RowKeys> outputPositions;

        private UpdateByBuildHandler(MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
            this.nextOutputPosition = nextOutputPosition;
            this.outputPositions = outputPositions;
        }

        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            buildHashTable(chunkOk, sourceKeyChunks, nextOutputPosition, outputPositions);
        }
    }

    private class UpdateByProbeHandler implements TypedHasherUtil.ProbeHandler {
        final WritableIntChunk<RowKeys> outputPositions;

        private UpdateByProbeHandler(WritableIntChunk<RowKeys> outputPositions) {
            this.outputPositions = outputPositions;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            probeHashTable(chunkOk, sourceKeyChunks, outputPositions);
        }
    }

    @Override
    public void add(boolean initialBuild, SafeCloseable bc, RowSequence orderedKeys, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
        if (orderedKeys.isEmpty()) {
            return;
        }
        buildTable(initialBuild, (BuildContext) bc, orderedKeys, sources, outputPositions,
                new UpdateByBuildHandler(nextOutputPosition, outputPositions));
    }

    @Override
    public void remove(@NotNull final SafeCloseable pc,
            @NotNull final RowSequence indexToRemove,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        if (indexToRemove.isEmpty()) {
            outputPositions.setSize(0);
            return;
        }
        probeTable((ProbeContext) pc, indexToRemove, true, sources, outputPositions,
                new UpdateByProbeHandler(outputPositions));
    }

    @Override
    public void findModifications(@NotNull final SafeCloseable pc,
            @NotNull final RowSequence modifiedIndex,
            @NotNull final ColumnSource<?>[] leftSources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        if (modifiedIndex.isEmpty()) {
            outputPositions.setSize(0);
            return;
        }
        probeTable((ProbeContext) pc, modifiedIndex, false, leftSources, outputPositions,
                new UpdateByProbeHandler(outputPositions));
    }

    public static class BuildContext extends TypedHasherUtil.BuildOrProbeContext {
        private BuildContext(ColumnSource<?>[] buildSources, int chunkSize) {
            super(buildSources, chunkSize);
        }

        final MutableInt rehashCredits = new MutableInt(0);
    }

    public static class ProbeContext extends TypedHasherUtil.BuildOrProbeContext {
        private ProbeContext(ColumnSource<?>[] buildSources, int chunkSize) {
            super(buildSources, chunkSize);
        }
    }

    @Override
    public SafeCloseable makeUpdateByBuildContext(ColumnSource<?>[] keySources, long updateSize) {
        return new BuildContext(keySources, (int) Math.min(CHUNK_SIZE, updateSize));
    }

    @Override
    public SafeCloseable makeUpdateByProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(maxSize, CHUNK_SIZE));
    }

    protected void newAlternate() {
        alternateStateSource = stateSource;
        stateSource = new ImmutableIntArraySource();
        stateSource.ensureCapacity(tableSize);

        if (mainInsertMask == 0) {
            mainInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
            alternateInsertMask = 0;
        } else {
            mainInsertMask = 0;
            alternateInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
        }
    }

    protected void clearAlternate() {
        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = null;
        }
    }

    protected void buildTable(
            final boolean initialBuild,
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources,
            WritableIntChunk<RowKeys> outputPositions,
            final TypedHasherUtil.BuildHandler buildHandler) {

        outputPositions.setSize(buildRows.intSize());

        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                while (doRehash(initialBuild, bc.rehashCredits, chunkOk.intSize(), outputPositions)) {
                    migrateFront(outputPositions);
                }

                getKeyChunks(buildSources, bc.getContexts, sourceKeyChunks, chunkOk);

                final long oldEntries = numEntries;
                buildHandler.doBuild(chunkOk, sourceKeyChunks);
                final long entriesAdded = numEntries - oldEntries;
                // if we actually added anything, then take away from the "equity" we've built up rehashing, otherwise
                // don't penalize this build call with additional rehashing
                bc.rehashCredits.subtract(Math.toIntExact(entriesAdded));

                bc.resetSharedContexts();
            }
        }
    }

    protected void probeTable(
            final ProbeContext pc,
            final RowSequence probeRows,
            final boolean usePrev,
            final ColumnSource<?>[] probeSources,
            WritableIntChunk<RowKeys> outputPositions,
            final TypedHasherUtil.ProbeHandler handler) {

        outputPositions.setSize(probeRows.intSize());

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

    /**
     * @param fullRehash should we rehash the entire table (if false, we rehash incrementally)
     * @param rehashCredits the number of entries this operation has rehashed (input/output)
     * @param nextChunkSize the size of the chunk we are processing
     * @return true if a front migration is required
     */
    public boolean doRehash(boolean fullRehash, MutableInt rehashCredits, int nextChunkSize,
            WritableIntChunk<RowKeys> outputPositions) {
        if (rehashPointer > 0) {
            final int requiredRehash = nextChunkSize - rehashCredits.intValue();
            if (requiredRehash <= 0) {
                return false;
            }

            // before building, we need to do at least as much rehash work as we would do build work
            rehashCredits.add(rehashInternalPartial(requiredRehash, outputPositions));
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
                rehashInternalPartial((int) numEntries, outputPositions);
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

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    protected int hashToTableLocationAlternate(int hash) {
        return hash & (alternateTableSize - 1);
    }

    abstract protected void buildHashTable(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            MutableInt outputPositionOffset, WritableIntChunk<RowKeys> outputPositions);

    abstract protected void probeHashTable(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            WritableIntChunk<RowKeys> outputPositions);

    abstract protected int rehashInternalPartial(int entriesToRehash, WritableIntChunk<RowKeys> outputPositions);

    abstract protected void migrateFront(WritableIntChunk<RowKeys> outputPositions);

    abstract protected void rehashInternalFull(final int oldSize);
}
