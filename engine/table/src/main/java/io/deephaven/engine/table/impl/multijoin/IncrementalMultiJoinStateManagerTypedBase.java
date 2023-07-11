/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.multijoin;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.MultiJoinStateManager;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.JoinControl.MAX_TABLE_SIZE;
import static io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker.FLAG_SHIFT;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;
import static io.deephaven.util.QueryConstants.NULL_BYTE;

public abstract class IncrementalMultiJoinStateManagerTypedBase implements MultiJoinStateManager {
    protected final ColumnSource<?>[] keySourcesForErrorMessages;
    private final List<LongArraySource> indexSources = new ArrayList<>();

    public static final long NO_RIGHT_STATE_VALUE = RowSet.NULL_ROW_KEY;
    public static final long EMPTY_RIGHT_STATE = QueryConstants.NULL_LONG;
    public static final long DUPLICATE_RIGHT_STATE = -2;

    // The number of slots in our hash table.
    protected int tableSize;
    // The number of slots in our alternate table, to start with "1" is a lie, but rehashPointer is zero; so our
    // location value is positive and can be compared against rehashPointer safely.
    protected int alternateTableSize = 1;

    // The number of entries in our hash table in use.
    protected long numEntries = 0;

    // The table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table.
    private final double maximumLoadFactor;

    // The keys for our hash entries.
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource[] mainKeySources;
    protected final WritableColumnSource[] alternateKeySources;

    // The output sources representing the keys of our joined table.
    protected final WritableColumnSource[] outputKeySources;

    // Store sentinel information and maps hash slots to output row keys.
    protected ImmutableLongArraySource slotToOutputRow = new ImmutableLongArraySource();
    protected ImmutableLongArraySource alternateSlotToOutputRow;

    protected ImmutableLongArraySource mainModifiedTrackerCookieSource = new ImmutableLongArraySource();
    protected ImmutableLongArraySource alternateModifiedTrackerCookieSource;

    // how much of the alternate sources are necessary to rehash?
    protected int rehashPointer = 0;

    protected IncrementalMultiJoinStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] keySourcesForErrorMessages, int tableSize, double maximumLoadFactor) {
        this.keySourcesForErrorMessages = keySourcesForErrorMessages;

        // we start out with a chunk sized table, and will grow by rehashing as states are added
        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        alternateKeySources = new WritableColumnSource[tableKeySources.length];
        chunkTypes = new ChunkType[tableKeySources.length];

        outputKeySources = new WritableColumnSource[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            chunkTypes[ii] = tableKeySources[ii].getChunkType();
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
            outputKeySources[ii] = ArrayBackedColumnSource.getMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;

        ensureCapacity(tableSize);
    }

    public int getTableCount() {
        return indexSources.size();
    }

    private void ensureCapacity(int tableSize) {
        slotToOutputRow.ensureCapacity(tableSize);
        mainModifiedTrackerCookieSource.ensureCapacity(tableSize);
        for (WritableColumnSource<?> mainKeySource : mainKeySources) {
            mainKeySource.ensureCapacity(tableSize);
        }
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

        void startShifts(long shiftDelta) {
            if (shiftDelta > 0) {
                if (pendingShifts == null) {
                    pendingShifts = new LongArraySource();
                }
            }
            pendingShiftPointer = 0;
        }

        public int pendingShiftPointer;
        public LongArraySource pendingShifts;

        public void ensureShiftCapacity(long shiftDelta, long size) {
            if (shiftDelta > 0) {
                pendingShifts.ensureCapacity(pendingShiftPointer + 2 * size);
            }
        }
    }

    public BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    public ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    private class LocalBuildHandler implements TypedHasherUtil.BuildHandler {
        final LongArraySource tableRedirSource;
        final int tableNumber;
        final MultiJoinModifiedSlotTracker modifiedSlotTracker;
        final byte trackerFlag;

        private LocalBuildHandler(LongArraySource tableRedirSource, int tableNumber) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
            this.modifiedSlotTracker = null;
            this.trackerFlag = NULL_BYTE;
        }

        private LocalBuildHandler(LongArraySource tableRedirSource, int tableNumber,
                @NotNull MultiJoinModifiedSlotTracker slotTracker, byte trackerFlag) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
            this.modifiedSlotTracker = slotTracker;
            this.trackerFlag = trackerFlag;
        }

        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            final long maxSize = numEntries + chunkOk.intSize();
            tableRedirSource.ensureCapacity(maxSize);
            for (WritableColumnSource src : outputKeySources) {
                src.ensureCapacity(maxSize);
            }
            buildFromLeftSide(chunkOk, sourceKeyChunks, tableRedirSource, tableNumber, modifiedSlotTracker,
                    trackerFlag);
        }
    }

    private class LocalRemoveHandler implements TypedHasherUtil.ProbeHandler {
        final LongArraySource tableRedirSource;
        final int tableNumber;
        final MultiJoinModifiedSlotTracker modifiedSlotTracker;
        final byte trackerFlag;

        private LocalRemoveHandler(LongArraySource tableRedirSource, int tableNumber,
                MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
            this.modifiedSlotTracker = modifiedSlotTracker;
            this.trackerFlag = trackerFlag;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            remove(chunkOk, sourceKeyChunks, tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag);
        }
    }

    private class LocalShiftHandler implements TypedHasherUtil.ProbeHandler {
        final LongArraySource tableRedirSource;
        final int tableNumber;
        final MultiJoinModifiedSlotTracker modifiedSlotTracker;
        final byte trackerFlag;
        final long shiftDelta;


        private LocalShiftHandler(LongArraySource tableRedirSource, int tableNumber,
                MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag, long shiftDelta) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
            this.modifiedSlotTracker = modifiedSlotTracker;
            this.trackerFlag = trackerFlag;
            this.shiftDelta = shiftDelta;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            shift(chunkOk, sourceKeyChunks, tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag,
                    shiftDelta);
        }
    }

    private class LocalModifyHandler implements TypedHasherUtil.ProbeHandler {
        final LongArraySource tableRedirSource;
        final int tableNumber;
        final MultiJoinModifiedSlotTracker modifiedSlotTracker;
        final byte trackerFlag;

        private LocalModifyHandler(LongArraySource tableRedirSource, int tableNumber,
                MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
            this.modifiedSlotTracker = modifiedSlotTracker;
            this.trackerFlag = trackerFlag;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            modify(chunkOk, sourceKeyChunks, tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag);
        }
    }

    @Override
    public void build(final Table table, ColumnSource<?>[] keySources, int tableNumber) {
        if (table.isEmpty()) {
            return;
        }
        final LongArraySource tableRedirSource = indexSources.get(tableNumber);
        try (final BuildContext bc = makeBuildContext(keySources, table.size())) {
            buildTable(true, bc, table.getRowSet(), keySources, new LocalBuildHandler(tableRedirSource, tableNumber),
                    null);
        }
    }

    public void processRemoved(final RowSet rowSet, ColumnSource<?>[] sources, int tableNumber,
            @NotNull MultiJoinModifiedSlotTracker slotTracker, byte trackerFlag) {
        if (rowSet.isEmpty()) {
            return;
        }

        Assert.geq(indexSources.size(), "indexSources.size()", tableNumber, "tableNumber");
        final LongArraySource tableRedirSource = indexSources.get(tableNumber);

        try (final ProbeContext pc = makeProbeContext(sources, rowSet.size())) {
            probeTable(pc, rowSet, true, sources,
                    new LocalRemoveHandler(tableRedirSource, tableNumber, slotTracker, trackerFlag));
        }
    }

    public void processShifts(final RowSet rowSet, final RowSetShiftData rowSetShiftData, ColumnSource<?>[] sources,
            int tableNumber, @NotNull MultiJoinModifiedSlotTracker slotTracker) {
        if (rowSet.isEmpty() || rowSetShiftData.empty()) {
            return;
        }

        Assert.geq(indexSources.size(), "indexSources.size()", tableNumber, "tableNumber");
        final LongArraySource tableRedirSource = indexSources.get(tableNumber);

        // Re-use the probe context for each shift range.
        try (final ProbeContext pc = makeProbeContext(sources, rowSet.size())) {
            final RowSetShiftData.Iterator sit = rowSetShiftData.applyIterator();
            while (sit.hasNext()) {
                sit.next();
                try (final WritableRowSet indexToShift =
                        rowSet.subSetByKeyRange(sit.beginRange(), sit.endRange())) {
                    probeTable(pc, indexToShift, true, sources, new LocalShiftHandler(tableRedirSource, tableNumber,
                            slotTracker, FLAG_SHIFT, sit.shiftDelta()));
                }
            }
        }
    }

    public void processModified(final RowSet rowSet, ColumnSource<?>[] sources, int tableNumber,
            @NotNull MultiJoinModifiedSlotTracker slotTracker, byte trackerFlag) {
        if (rowSet.isEmpty()) {
            return;
        }

        Assert.geq(indexSources.size(), "indexSources.size()", tableNumber, "tableNumber");
        final LongArraySource tableRedirSource = indexSources.get(tableNumber);

        try (final ProbeContext pc = makeProbeContext(sources, rowSet.size())) {
            probeTable(pc, rowSet, false, sources,
                    new LocalModifyHandler(tableRedirSource, tableNumber, slotTracker, trackerFlag));
        }
    }

    public void processAdded(final RowSet rowSet, ColumnSource<?>[] sources, int tableNumber,
            @NotNull MultiJoinModifiedSlotTracker slotTracker, byte trackerFlag) {
        if (rowSet.isEmpty()) {
            return;
        }

        Assert.geq(indexSources.size(), "indexSources.size()", tableNumber, "tableNumber");
        final LongArraySource tableRedirSource = indexSources.get(tableNumber);

        try (final BuildContext bc = makeBuildContext(sources, rowSet.size())) {
            buildTable(false, bc, rowSet, sources,
                    new LocalBuildHandler(tableRedirSource, tableNumber, slotTracker, trackerFlag), slotTracker);
        }
    }

    protected abstract void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag);

    protected abstract void remove(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag);

    protected abstract void shift(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag, long shiftDelta);

    protected abstract void modify(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag);

    abstract protected void migrateFront(MultiJoinModifiedSlotTracker modifiedSlotTracker);

    protected void buildTable(
            final boolean initialBuild,
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources,
            final LocalBuildHandler buildHandler,
            MultiJoinModifiedSlotTracker modifiedSlotTracker) {
        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                final int nextChunkSize = chunkOk.intSize();
                while (doRehash(initialBuild, bc.rehashCredits, nextChunkSize, modifiedSlotTracker)) {
                    migrateFront(modifiedSlotTracker);
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

    protected void probeTable(
            final ProbeContext pc,
            final RowSequence probeRows,
            final boolean usePrev,
            final ColumnSource<?>[] probeSources,
            final TypedHasherUtil.ProbeHandler handler) {
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
            MultiJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rehashPointer > 0) {
            final int requiredRehash = nextChunkSize - rehashCredits.intValue();
            if (requiredRehash <= 0) {
                return false;
            }

            // before building, we need to do at least as much rehash work as we would do build work
            rehashCredits.add(rehashInternalPartial(requiredRehash, modifiedSlotTracker));
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
                rehashInternalPartial((int) numEntries, modifiedSlotTracker);
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

    protected void newAlternate() {
        alternateSlotToOutputRow = slotToOutputRow;
        slotToOutputRow = new ImmutableLongArraySource();
        slotToOutputRow.ensureCapacity(tableSize);

        alternateModifiedTrackerCookieSource = mainModifiedTrackerCookieSource;
        mainModifiedTrackerCookieSource = new ImmutableLongArraySource();
        mainModifiedTrackerCookieSource.ensureCapacity(tableSize);
    }

    protected void clearAlternate() {
        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = null;
        }
    }

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    abstract protected void rehashInternalFull(final int oldSize);

    // abstract protected void migrateFront(MultiJoinModifiedSlotTracker modifiedSlotTracker);

    /**
     * @param numEntriesToRehash number of entries to rehash into main table
     * @return actual number of entries rehashed
     */
    protected abstract int rehashInternalPartial(int numEntriesToRehash,
            MultiJoinModifiedSlotTracker modifiedSlotTracker);

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    protected int hashToTableLocationAlternate(int hash) {
        return hash & (alternateTableSize - 1);
    }

    // produce a pretty key for error messages
    protected String keyString(Chunk[] sourceKeyChunks, int chunkPosition) {
        return ChunkUtils.extractKeyStringFromChunks(chunkTypes, sourceKeyChunks, chunkPosition);
    }

    public void getCurrentRedirections(long slot, long[] redirections) {
        for (int tt = 0; tt < indexSources.size(); ++tt) {
            final long redirection = indexSources.get(tt).getLong(slot);
            redirections[tt] = redirection == QueryConstants.NULL_LONG ? NULL_ROW_KEY : redirection;
        }
    }

    @Override
    public long getResultSize() {
        return numEntries;
    }

    @Override
    public ColumnSource<?>[] getKeyHashTableSources() {
        return outputKeySources;
    }

    @Override
    public WritableRowRedirection getRowRedirectionForTable(int tableNumber) {
        return new LongColumnSourceWritableRowRedirection(indexSources.get(tableNumber));
    }

    @Override
    public void ensureTableCapacity(int tables) {
        while (indexSources.size() < tables) {
            final LongArraySource newRedirection = new LongArraySource();
            newRedirection.ensureCapacity(numEntries);
            indexSources.add(newRedirection);
        }
    }

    @Override
    public void setTargetLoadFactor(double targetLoadFactor) {}

    @Override
    public void setMaximumLoadFactor(double maximumLoadFactor) {}
}
