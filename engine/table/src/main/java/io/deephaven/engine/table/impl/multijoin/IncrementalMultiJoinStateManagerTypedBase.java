//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
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
    private final List<LongArraySource> redirectionSources = new ArrayList<>();

    public static final long NO_REDIRECTION = QueryConstants.NULL_LONG;
    public static final int EMPTY_OUTPUT_ROW = QueryConstants.NULL_INT;
    public static final long EMPTY_COOKIE_SLOT = -1;

    /** The number of slots in our hash table. */
    protected int tableSize;
    /**
     * The number of slots in our alternate table, to start with "1" is a lie, but rehashPointer is zero; so our
     * location value is positive and can be compared against rehashPointer safely.
     */
    protected int alternateTableSize = 1;

    /** The number of entries in our hash table in use. */
    protected int numEntries = 0;

    /**
     * The table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor.
     */
    private final double maximumLoadFactor;

    /** The keys for our hash entries. */
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource<?>[] mainKeySources;
    protected final WritableColumnSource<?>[] alternateKeySources;

    /** The output sources representing the keys of our joined table. */
    protected final WritableColumnSource[] outputKeySources;

    /** Store sentinel information and maps hash slots to output row keys. */
    protected ImmutableIntArraySource slotToOutputRow = new ImmutableIntArraySource();
    protected ImmutableIntArraySource alternateSlotToOutputRow;

    protected ImmutableLongArraySource mainModifiedTrackerCookieSource = new ImmutableLongArraySource();
    protected ImmutableLongArraySource alternateModifiedTrackerCookieSource;

    /** how much of the alternate sources are necessary to rehash? */
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

        // This is called only once.
        ensureCapacity(tableSize);
    }

    public int getTableCount() {
        return redirectionSources.size();
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
        private ProbeContext(ColumnSource<?>[] probeSources, int chunkSize) {
            super(probeSources, chunkSize);
        }
    }

    public BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    public ProbeContext makeProbeContext(ColumnSource<?>[] probeSources, long maxSize) {
        return new ProbeContext(probeSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    private class BuildHandler implements TypedHasherUtil.BuildHandler {
        final LongArraySource tableRedirSource;
        final int tableNumber;
        final MultiJoinModifiedSlotTracker modifiedSlotTracker;
        final byte trackerFlag;

        private BuildHandler(LongArraySource tableRedirSource, int tableNumber) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
            this.modifiedSlotTracker = null;
            this.trackerFlag = NULL_BYTE;
        }

        private BuildHandler(LongArraySource tableRedirSource, int tableNumber,
                @NotNull MultiJoinModifiedSlotTracker slotTracker, byte trackerFlag) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
            this.modifiedSlotTracker = slotTracker;
            this.trackerFlag = trackerFlag;
        }

        @Override
        public void doBuild(RowSequence rows, Chunk<Values>[] sourceKeyChunks) {
            final long maxSize = numEntries + rows.intSize();
            tableRedirSource.ensureCapacity(maxSize);
            for (WritableColumnSource<?> src : outputKeySources) {
                src.ensureCapacity(maxSize);
            }
            buildFromTable(rows, sourceKeyChunks, tableRedirSource, tableNumber, modifiedSlotTracker,
                    trackerFlag);
        }
    }

    private static abstract class ProbeHandler implements TypedHasherUtil.ProbeHandler {
        final LongArraySource tableRedirSource;
        final int tableNumber;
        final MultiJoinModifiedSlotTracker modifiedSlotTracker;
        final byte trackerFlag;

        private ProbeHandler(LongArraySource tableRedirSource, int tableNumber,
                MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
            this.tableRedirSource = tableRedirSource;
            this.tableNumber = tableNumber;
            this.modifiedSlotTracker = modifiedSlotTracker;
            this.trackerFlag = trackerFlag;
        }
    }

    private class RemoveHandler extends ProbeHandler {
        private RemoveHandler(LongArraySource tableRedirSource, int tableNumber,
                MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
            super(tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag);
        }

        @Override
        public void doProbe(RowSequence rows, Chunk<Values>[] sourceKeyChunks) {
            remove(rows, sourceKeyChunks, tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag);
        }
    }

    private class ShiftHandler extends ProbeHandler {
        final long shiftDelta;

        private ShiftHandler(LongArraySource tableRedirSource, int tableNumber,
                MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag, long shiftDelta) {
            super(tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag);
            this.shiftDelta = shiftDelta;
        }

        @Override
        public void doProbe(RowSequence rows, Chunk<Values>[] sourceKeyChunks) {
            shift(rows, sourceKeyChunks, tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag,
                    shiftDelta);
        }
    }

    private class ModifyHandler extends ProbeHandler {
        private ModifyHandler(LongArraySource tableRedirSource, int tableNumber,
                MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag) {
            super(tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag);
        }

        @Override
        public void doProbe(RowSequence rows, Chunk<Values>[] sourceKeyChunks) {
            modify(rows, sourceKeyChunks, tableRedirSource, tableNumber, modifiedSlotTracker, trackerFlag);
        }
    }

    @Override
    public void build(final Table table, ColumnSource<?>[] keySources, int tableNumber) {
        if (table.isEmpty()) {
            return;
        }
        final LongArraySource tableRedirSource = redirectionSources.get(tableNumber);
        try (final BuildContext bc = makeBuildContext(keySources, table.size())) {
            buildTable(true, bc, table.getRowSet(), keySources, new BuildHandler(tableRedirSource, tableNumber));
        }
    }

    public void processRemoved(final RowSet rowSet, ColumnSource<?>[] sources, int tableNumber,
            @NotNull MultiJoinModifiedSlotTracker slotTracker, byte trackerFlag) {
        if (rowSet.isEmpty()) {
            return;
        }

        Assert.geq(redirectionSources.size(), "redirectionSources.size()", tableNumber, "tableNumber");
        final LongArraySource tableRedirSource = redirectionSources.get(tableNumber);

        try (final ProbeContext pc = makeProbeContext(sources, rowSet.size())) {
            probeTable(pc, rowSet, true, sources,
                    new RemoveHandler(tableRedirSource, tableNumber, slotTracker, trackerFlag));
        }
    }

    public void processShifts(final RowSet rowSet, final RowSetShiftData rowSetShiftData, ColumnSource<?>[] sources,
            int tableNumber, @NotNull MultiJoinModifiedSlotTracker slotTracker) {
        if (rowSet.isEmpty() || rowSetShiftData.empty()) {
            return;
        }

        Assert.geq(redirectionSources.size(), "redirectionSources.size()", tableNumber, "tableNumber");
        final LongArraySource tableRedirSource = redirectionSources.get(tableNumber);

        // Re-use the probe context for each shift range.
        try (final ProbeContext pc = makeProbeContext(sources, rowSet.size())) {
            final RowSetShiftData.Iterator sit = rowSetShiftData.applyIterator();
            while (sit.hasNext()) {
                sit.next();
                try (final WritableRowSet rowSetToShift =
                        rowSet.subSetByKeyRange(sit.beginRange(), sit.endRange())) {
                    probeTable(pc, rowSetToShift, true, sources, new ShiftHandler(tableRedirSource, tableNumber,
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

        Assert.geq(redirectionSources.size(), "redirectionSources.size()", tableNumber, "tableNumber");
        final LongArraySource tableRedirSource = redirectionSources.get(tableNumber);

        try (final ProbeContext pc = makeProbeContext(sources, rowSet.size())) {
            probeTable(pc, rowSet, false, sources,
                    new ModifyHandler(tableRedirSource, tableNumber, slotTracker, trackerFlag));
        }
    }

    public void processAdded(final RowSet rowSet, ColumnSource<?>[] sources, int tableNumber,
            @NotNull MultiJoinModifiedSlotTracker slotTracker, byte trackerFlag) {
        if (rowSet.isEmpty()) {
            return;
        }

        Assert.geq(redirectionSources.size(), "redirectionSources.size()", tableNumber, "tableNumber");
        final LongArraySource tableRedirSource = redirectionSources.get(tableNumber);

        try (final BuildContext bc = makeBuildContext(sources, rowSet.size())) {
            buildTable(false, bc, rowSet, sources,
                    new BuildHandler(tableRedirSource, tableNumber, slotTracker, trackerFlag));
        }
    }

    protected abstract void buildFromTable(RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag);

    protected abstract void remove(RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag);

    protected abstract void shift(RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag, long shiftDelta);

    protected abstract void modify(RowSequence rowSequence, Chunk<Values>[] sourceKeyChunks,
            LongArraySource tableRedirSource, int tableNumber,
            MultiJoinModifiedSlotTracker modifiedSlotTracker, byte trackerFlag);

    abstract protected void migrateFront();

    private void buildTable(
            final boolean initialBuild,
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources,
            final BuildHandler buildHandler) {
        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence rows = rsIt.getNextRowSequenceWithLength(bc.chunkSize);
                final int nextChunkSize = rows.intSize();
                while (doRehash(initialBuild, bc.rehashCredits, nextChunkSize)) {
                    migrateFront();
                }

                getKeyChunks(buildSources, bc.getContexts, sourceKeyChunks, rows);

                final long oldEntries = numEntries;
                buildHandler.doBuild(rows, sourceKeyChunks);
                final long entriesAdded = numEntries - oldEntries;
                // if we actually added anything, then take away from the "equity" we've built up rehashing, otherwise
                // don't penalize this build call with additional rehashing
                bc.rehashCredits.subtract(Math.toIntExact(entriesAdded));

                bc.resetSharedContexts();
            }
        }
    }

    private void probeTable(
            final ProbeContext pc,
            final RowSequence probeRows,
            final boolean usePrev,
            final ColumnSource<?>[] probeSources,
            final TypedHasherUtil.ProbeHandler handler) {
        try (final RowSequence.Iterator rsIt = probeRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[probeSources.length];

            while (rsIt.hasMore()) {
                final RowSequence rows = rsIt.getNextRowSequenceWithLength(pc.chunkSize);

                if (usePrev) {
                    getPrevKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, rows);
                } else {
                    getKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, rows);
                }

                handler.doProbe(rows, sourceKeyChunks);

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
    public boolean doRehash(boolean fullRehash, MutableInt rehashCredits, int nextChunkSize) {
        if (rehashPointer > 0) {
            final int requiredRehash = nextChunkSize - rehashCredits.get();
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
        if (rehashCredits.get() > 0) {
            rehashCredits.set(0);
        }

        if (fullRehash) {
            // if we are doing a full rehash, we need to ditch the alternate
            if (rehashPointer > 0) {
                rehashInternalPartial(numEntries);
                clearAlternate();
            }

            rehashInternalFull(oldTableSize);

            return false;
        }

        setupNewAlternate(oldTableSize);
        adviseNewAlternate();
        return true;
    }

    /**
     * After creating the new alternate key states, advise the derived classes, so they can cast them to the typed
     * versions of the column source and adjust the derived class pointers.
     */
    protected abstract void adviseNewAlternate();

    private void setupNewAlternate(int oldTableSize) {
        Assert.eqZero(rehashPointer, "rehashPointer");

        alternateSlotToOutputRow = slotToOutputRow;
        slotToOutputRow = new ImmutableIntArraySource();
        slotToOutputRow.ensureCapacity(tableSize);

        alternateModifiedTrackerCookieSource = mainModifiedTrackerCookieSource;
        mainModifiedTrackerCookieSource = new ImmutableLongArraySource();
        mainModifiedTrackerCookieSource.ensureCapacity(tableSize);

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
    }

    protected void clearAlternate() {
        alternateSlotToOutputRow = null;
        alternateModifiedTrackerCookieSource = null;
        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = null;
        }
        alternateTableSize = 1;
    }

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    abstract protected void rehashInternalFull(final int oldSize);

    /**
     * @param numEntriesToRehash number of entries to rehash into main table
     * @return actual number of entries rehashed
     */
    protected abstract int rehashInternalPartial(int numEntriesToRehash);

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    protected int hashToTableLocationAlternate(int hash) {
        return hash & (alternateTableSize - 1);
    }

    /** produce a pretty key for error messages. */
    protected String keyString(Chunk<Values>[] sourceKeyChunks, int chunkPosition) {
        return ChunkUtils.extractKeyStringFromChunks(chunkTypes, sourceKeyChunks, chunkPosition);
    }

    public void getCurrentRedirections(long slot, long[] redirections) {
        for (int tt = 0; tt < redirectionSources.size(); ++tt) {
            final long redirection = redirectionSources.get(tt).getLong(slot);
            redirections[tt] = redirection == QueryConstants.NULL_LONG ? NULL_ROW_KEY : redirection;
        }
    }

    public void startTrackingPrevRedirectionValues(int tableNumber) {
        redirectionSources.get(tableNumber).startTrackingPrevValues();
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
    public RowRedirection getRowRedirectionForTable(int tableNumber) {
        return new LongColumnSourceRowRedirection<>(redirectionSources.get(tableNumber));
    }

    @Override
    public void ensureTableCapacity(int tables) {
        while (redirectionSources.size() < tables) {
            final LongArraySource newRedirection = new LongArraySource();
            newRedirection.ensureCapacity(numEntries);
            redirectionSources.add(newRedirection);
        }
    }

    @Override
    public void setTargetLoadFactor(double targetLoadFactor) {}

    @Override
    public void setMaximumLoadFactor(double maximumLoadFactor) {}
}
