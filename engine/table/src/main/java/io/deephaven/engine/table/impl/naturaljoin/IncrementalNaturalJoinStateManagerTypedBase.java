//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import io.deephaven.api.NaturalJoinType;
import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.util.SafeCloseableArray;
import io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.JoinControl.MAX_TABLE_SIZE;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class IncrementalNaturalJoinStateManagerTypedBase extends StaticNaturalJoinStateManager
        implements IncrementalNaturalJoinStateManager, BothIncrementalNaturalJoinStateManager {

    public static final long EMPTY_RIGHT_STATE = QueryConstants.NULL_LONG;
    public static final long TOMBSTONE_RIGHT_STATE = RowSet.NULL_ROW_KEY - 1;
    public static final long FIRST_DUPLICATE = TOMBSTONE_RIGHT_STATE - 1;

    // the number of slots in our table
    protected int tableSize;

    // the number of slots in our alternate table, to start with "1" is a lie, but rehashPointer is zero; so our
    // location value is positive and can be compared against rehashPointer safely
    protected int alternateTableSize = 1;

    // how much of the alternate sources are necessary to rehash?
    protected int rehashPointer = 0;

    /** How many entries are taking up slots in the main hash table (includes tombstones)? */
    protected long numEntries = 0;
    /** How many values do we have that are live (in both main and alternate)? */
    protected long liveEntries = 0;
    /** How many entries are in the alternate table (includes tombstones)? */
    protected long alternateEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ChunkType[] chunkTypes;
    protected final WritableColumnSource[] mainKeySources;
    protected final WritableColumnSource[] alternateKeySources;

    // per key-column equality kernels, used to detect which modified left rows actually changed key value
    private final ChunkEquals[] keyChunkEquals;
    // per key-column compaction kernels, used to compact previous key chunks down to the changed rows
    private final CompactKernel[] keyCompactKernels;

    /**
     * <p>
     * We use a RowSet.NULL_ROW_KEY for a state that exists, but has no right hand side; the column sources are
     * initialized with NULL_LONG for something that does not exist. When there are multiple right rows, we store a
     * value less than TOMBSTONE_RIGHT_STATE, which is a position in the rightSideDuplicateRowSets (-3 maps to 0, -4 to
     * 1, etc.). We must maintain the right side duplicates so that we do not need a rescan; but in the common (as
     * opposed to impending error) case of a single value we do not want to allocate any objects
     *
     * The TOMBSTONE_RIGHT_STATE indicates that the row was deleted.
     * </p>
     */
    protected ImmutableLongArraySource mainRightRowKey = new ImmutableLongArraySource();
    protected ImmutableLongArraySource alternateRightRowKey;

    protected ImmutableObjectArraySource<WritableRowSet> mainLeftRowSet =
            new ImmutableObjectArraySource(WritableRowSet.class, null);
    protected ImmutableLongArraySource mainModifiedTrackerCookieSource = new ImmutableLongArraySource();

    protected ImmutableObjectArraySource<WritableRowSet> alternateLeftRowSet;
    protected ImmutableLongArraySource alternateModifiedTrackerCookieSource;

    protected ObjectArraySource<WritableRowSet> rightSideDuplicateRowSets =
            new ObjectArraySource<>(WritableRowSet.class);
    protected long nextDuplicateRightSide = 0;
    protected LongArrayList freeDuplicateValues = new LongArrayList();

    // the mask for insertion into the main table (this is used so that we can identify whether a slot belongs to the
    // main or alternate table)
    protected int mainInsertMask = 0;
    protected int alternateInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;

    protected IncrementalNaturalJoinStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] keySourcesForErrorMessages, int tableSize, double maximumLoadFactor,
            NaturalJoinType joinType, boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);

        // we start out with a chunk sized table, and will grow by rehashing as states are added
        this.tableSize = CHUNK_SIZE;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        alternateKeySources = new WritableColumnSource[tableKeySources.length];
        chunkTypes = new ChunkType[tableKeySources.length];
        keyChunkEquals = new ChunkEquals[tableKeySources.length];
        keyCompactKernels = new CompactKernel[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            chunkTypes[ii] = tableKeySources[ii].getChunkType();
            keyChunkEquals[ii] = ChunkEquals.makeEqual(chunkTypes[ii]);
            keyCompactKernels[ii] = CompactKernel.makeCompact(chunkTypes[ii]);
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;

        ensureCapacity(tableSize);
    }


    private void ensureCapacity(int tableSize) {
        mainLeftRowSet.ensureCapacity(tableSize);
        mainRightRowKey.ensureCapacity(tableSize);
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

    @Override
    public BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    @Override
    public ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    protected void buildTable(
            final boolean initialBuild,
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources,
            final TypedHasherUtil.BuildHandler buildHandler,
            final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
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

    /* @formatter:off

    void checkAlternateEntries() {
        int expected = 0;
        int expectedLive = 0;
        if (alternateRightRowKey != null) {
            for (int ii = 0; ii < rehashPointer; ++ii) {
                long state = alternateRightRowKey.getUnsafe(ii);
                if (state != EMPTY_RIGHT_STATE) {
                    if (state != TOMBSTONE_RIGHT_STATE) {
                        expectedLive++;
                    }
                    expected++;
                }
            }
        }
        Assert.eq(alternateEntries, "alternateEntries", expected, "expected");
    }

    void checkMainEntries() {
        int expected = 0;
        for (int ii = 0; ii < tableSize; ++ii) {
            if (mainRightRowKey.getUnsafe(ii) != EMPTY_RIGHT_STATE) {
                expected++;
            }
        }
        Assert.eq(numEntries, "numEntries", expected, "expected");
    }

    @formatter:on */

    /**
     * @param fullRehash should we rehash the entire table (if false, we rehash incrementally)
     * @param rehashCredits the number of entries this operation has rehashed (input/output)
     * @param nextChunkSize the size of the chunk we are processing
     * @return true if a front migration is required
     */
    public boolean doRehash(boolean fullRehash, MutableInt rehashCredits, int nextChunkSize,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rehashPointer > 0) {
            final int requiredRehash = nextChunkSize - rehashCredits.get();
            if (requiredRehash <= 0) {
                return false;
            }

            // before building, we need to do at least as much rehash work as we would do build work
            rehashCredits.add(rehashInternalPartial(requiredRehash, modifiedSlotTracker));
            if (rehashPointer == 0) {
                clearAlternate();
            }
        }

        if (!rehashRequired(nextChunkSize)) {
            return false;
        }

        int oldTableSize = tableSize;
        tableSize = computeTableSize(nextChunkSize);

        // we can't give the caller credit for rehashes with the old table, we need to begin migrating things again
        if (rehashCredits.get() > 0) {
            rehashCredits.set(0);
        }

        if (fullRehash) {
            // we need to ditch the alternate table before continuing on a full rehash
            if (rehashPointer > 0) {
                rehashInternalPartial((int) alternateEntries, modifiedSlotTracker);
                Assert.eqZero(alternateEntries, "alternateEntries");
                clearAlternate();
            }
            rehashInternalFull(oldTableSize);
            return false;
        }
        Assert.eqZero(rehashPointer, "rehashPointer");

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

        alternateEntries = numEntries;
        numEntries = 0;
        alternateRightRowKey = mainRightRowKey;
        mainRightRowKey = new ImmutableLongArraySource();
        mainRightRowKey.ensureCapacity(tableSize);

        alternateLeftRowSet = mainLeftRowSet;
        mainLeftRowSet = new ImmutableObjectArraySource(WritableRowSet.class, null);
        mainLeftRowSet.ensureCapacity(tableSize);

        alternateModifiedTrackerCookieSource = mainModifiedTrackerCookieSource;
        mainModifiedTrackerCookieSource = new ImmutableLongArraySource();
        mainModifiedTrackerCookieSource.ensureCapacity(tableSize);

        if (mainInsertMask == 0) {
            mainInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
            alternateInsertMask = 0;
        } else {
            mainInsertMask = 0;
            alternateInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
        }
    }

    protected void clearAlternate() {
        alternateEntries = 0;
        rehashPointer = 0;
        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = null;
        }
    }

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    public int computeTableSize(int nextChunkSize) {
        // we use the number of liveEntries multiplied by 2, so that as we rehash we can both consume a slot for the
        // live entry from the alternate table; and also consume a slot for the new value. This ensures that we will
        // burn down our rehash requirements before we need to initiate a new partial rehash.

        final long desiredEntries = Math.max(liveEntries * 2, liveEntries + nextChunkSize);
        final long tableSize =
                MathUtil.roundUpPowerOf2(Math.max(this.tableSize, (long) (desiredEntries / maximumLoadFactor)));
        if (tableSize <= 1 || tableSize > MAX_TABLE_SIZE) {
            throw new UnsupportedOperationException("Hash table exceeds maximum size!");
        }
        return Math.toIntExact(tableSize);
    }

    abstract protected void rehashInternalFull(final int oldSize);

    abstract protected void migrateFront(NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    /**
     * @param numEntriesToRehash number of entries to rehash into main table
     * @return actual number of entries rehashed
     */
    protected abstract int rehashInternalPartial(int numEntriesToRehash,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    protected int hashToTableLocationAlternate(int hash) {
        return hash & (alternateTableSize - 1);
    }

    protected long duplicateLocationFromRowKey(long rowKey) {
        Assert.leq(rowKey, "rowKey", FIRST_DUPLICATE);
        return -rowKey + FIRST_DUPLICATE;
    }

    protected long rowKeyFromDuplicateLocation(long duplicateLocation) {
        return -duplicateLocation + FIRST_DUPLICATE;
    }

    protected long allocateDuplicateLocation() {
        if (freeDuplicateValues.isEmpty()) {
            rightSideDuplicateRowSets.ensureCapacity(nextDuplicateRightSide + 1);
            return nextDuplicateRightSide++;
        } else {
            return freeDuplicateValues.removeLong(freeDuplicateValues.size() - 1);
        }
    }

    protected void freeDuplicateLocation(long duplicateLocation) {
        freeDuplicateValues.add(duplicateLocation);
    }

    @Override
    public long getRightRowKey(int slot) {
        final long rightRowKey;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            // slot needs to represent whether we are in the main or alternate using main insert mask!
            rightRowKey = mainRightRowKey.getUnsafe(slot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
        } else {
            rightRowKey = alternateRightRowKey.getUnsafe(slot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
        }
        if (rightRowKey <= FIRST_DUPLICATE) {
            return DUPLICATE_RIGHT_VALUE;
        }
        return rightRowKey;
    }

    @Override
    public RowSet getLeftRowSet(int slot) {
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            return mainLeftRowSet.getUnsafe(slot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
        } else {
            return alternateLeftRowSet.getUnsafe(slot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
        }
    }

    @Override
    public RowSet getRightRowSet(int slot) {
        final long rightRowKey;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            // slot needs to represent whether we are in the main or alternate using main insert mask!
            rightRowKey = mainRightRowKey.getUnsafe(slot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
        } else {
            rightRowKey = alternateRightRowKey.getUnsafe(slot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
        }
        return rightSideDuplicateRowSets.getUnsafe(duplicateLocationFromRowKey(rightRowKey));
    }

    @Override
    public String keyString(int slot) {
        final long firstLeftRowKey;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            firstLeftRowKey =
                    mainLeftRowSet.getUnsafe(slot & AlternatingColumnSource.ALTERNATE_INNER_MASK).firstRowKey();
        } else {
            firstLeftRowKey =
                    alternateLeftRowSet.getUnsafe(slot & AlternatingColumnSource.ALTERNATE_INNER_MASK).firstRowKey();
        }
        Assert.neq(firstLeftRowKey, "firstLeftRowKey", RowSet.NULL_ROW_KEY);
        return extractKeyStringFromSourceTable(firstLeftRowKey);
    }

    private long getRightRowKeyFromState(
            final long leftRowKey,
            final long rightRowKeyForState) {
        if (rightRowKeyForState > FIRST_DUPLICATE) {
            return rightRowKeyForState;
        }
        if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
            throw new IllegalStateException("Natural Join found duplicate right key for "
                    + extractKeyStringFromSourceTable(leftRowKey));
        }
        final long location = duplicateLocationFromRowKey(rightRowKeyForState);
        final WritableRowSet rightRowSet = rightSideDuplicateRowSets.getUnsafe(location);
        return joinType == NaturalJoinType.FIRST_MATCH
                ? rightRowSet.firstRowKey()
                : rightRowSet.lastRowKey();
    }

    public WritableRowRedirection buildIndexedRowRedirection(
            QueryTable leftTable,
            InitialBuildContext ibc,
            ColumnSource<RowSet> indexRowSets,
            JoinControl.RedirectionType redirectionType) {
        Assert.eqZero(rehashPointer, "rehashPointer");

        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat() || leftTable.getRowSet().lastRowKey() > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];

                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.mainLeftRowSet.getUnsafe(ii);
                    if (leftRowSet != null && !leftRowSet.isEmpty()) {
                        Assert.eq(leftRowSet.size(), "leftRowSet.size()", 1);
                        // Load the row set from the index row set column.
                        final RowSet leftRowSetForKey = indexRowSets.get(leftRowSet.firstRowKey());
                        // Reset mainLeftRowSet to contain the indexed row set.
                        mainLeftRowSet.set(ii, leftRowSetForKey.copy());
                        final long leftRowKey = leftRowSetForKey.firstRowKey();
                        final long rightState = mainRightRowKey.getUnsafe(ii);
                        final long rightRowKey = getRightRowKeyFromState(leftRowKey, rightState);
                        checkExactMatch(leftRowKey, rightRowKey);
                        // Set unconditionally, need to populate the entire array with NULL_ROW_KEY or the RHS key
                        leftRowSetForKey.forAllRowKeys(pos -> innerIndex[(int) pos] = rightRowKey);
                    }
                }

                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();
                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.mainLeftRowSet.getUnsafe(ii);
                    if (leftRowSet != null && !leftRowSet.isEmpty()) {
                        Assert.eq(leftRowSet.size(), "leftRowSet.size()", 1);
                        // Load the row set from the index row set column.
                        final RowSet leftRowSetForKey = indexRowSets.get(leftRowSet.firstRowKey());
                        // Reset mainLeftRowSet to contain the indexed row set.
                        mainLeftRowSet.set(ii, leftRowSetForKey.copy());
                        final long leftRowKey = leftRowSetForKey.firstRowKey();
                        final long rightState = mainRightRowKey.getUnsafe(ii);
                        final long rightRowKey = getRightRowKeyFromState(leftRowKey, rightState);
                        if (rightRowKey == RowSet.NULL_ROW_KEY) {
                            checkExactMatch(leftRowKey, rightRowKey);
                        } else {
                            leftRowSetForKey.forAllRowKeys(pos -> sparseRedirections.set(pos, rightRowKey));
                        }
                    }
                }
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection =
                        WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());
                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.mainLeftRowSet.getUnsafe(ii);
                    if (leftRowSet != null && !leftRowSet.isEmpty()) {
                        Assert.eq(leftRowSet.size(), "leftRowSet.size()", 1);
                        // Load the row set from the index row set column.
                        final RowSet leftRowSetForKey = indexRowSets.get(leftRowSet.firstRowKey());
                        // Reset mainLeftRowSet to contain the indexed row set.
                        mainLeftRowSet.set(ii, leftRowSetForKey.copy());
                        final long leftRowKey = leftRowSetForKey.firstRowKey();
                        final long rightState = mainRightRowKey.getUnsafe(ii);
                        final long rightRowKey = getRightRowKeyFromState(leftRowKey, rightState);
                        if (rightRowKey == RowSet.NULL_ROW_KEY) {
                            checkExactMatch(leftRowKey, rightRowKey);
                        } else {
                            leftRowSetForKey.forAllRowKeys(pos -> rowRedirection.put(pos, rightRowKey));
                        }
                    }
                }
                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    public WritableRowRedirection buildRowRedirectionFromRedirections(QueryTable leftTable, InitialBuildContext ibc,
            JoinControl.RedirectionType redirectionType) {
        Assert.eqZero(rehashPointer, "rehashPointer");

        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat() || leftTable.getRowSet().lastRowKey() > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];

                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.mainLeftRowSet.getUnsafe(ii);
                    if (leftRowSet != null && !leftRowSet.isEmpty()) {
                        final long leftRowKey = leftRowSet.firstRowKey();
                        final long rightState = mainRightRowKey.getUnsafe(ii);
                        final long rightRowKey = getRightRowKeyFromState(leftRowKey, rightState);
                        checkExactMatch(leftRowKey, rightRowKey);
                        // Set unconditionally, need to populate the entire array with NULL_ROW_KEY or the RHS key
                        leftRowSet.forAllRowKeys(pos -> innerIndex[(int) pos] = rightRowKey);
                    }
                }
                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();
                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.mainLeftRowSet.getUnsafe(ii);
                    if (leftRowSet != null && !leftRowSet.isEmpty()) {
                        final long leftRowKey = leftRowSet.firstRowKey();
                        final long rightState = mainRightRowKey.getUnsafe(ii);
                        final long rightRowKey = getRightRowKeyFromState(leftRowKey, rightState);
                        if (rightRowKey == RowSet.NULL_ROW_KEY) {
                            checkExactMatch(leftRowKey, rightRowKey);
                        } else {
                            leftRowSet.forAllRowKeys(pos -> sparseRedirections.set(pos, rightRowKey));
                        }
                    }
                }
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection =
                        WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());
                for (int ii = 0; ii < tableSize; ++ii) {
                    final WritableRowSet leftRowSet = this.mainLeftRowSet.getUnsafe(ii);
                    if (leftRowSet != null && !leftRowSet.isEmpty()) {
                        final long leftRowKey = leftRowSet.firstRowKey();
                        final long rightState = mainRightRowKey.getUnsafe(ii);
                        final long rightRowKey = getRightRowKeyFromState(leftRowKey, rightState);
                        if (rightRowKey == RowSet.NULL_ROW_KEY) {
                            checkExactMatch(leftRowKey, rightRowKey);
                        } else {
                            leftRowSet.forAllRowKeys(pos -> rowRedirection.put(pos, rightRowKey));
                        }
                    }
                }
                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    protected abstract void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    protected abstract void applyRightShift(RowSequence rowSequence, Chunk[] sourceKeyChunks, long shiftDelta,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker, ProbeContext pc);

    @Override
    public void modifyByRight(Context pc, RowSet modified, ColumnSource<?>[] rightSources,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (modified.isEmpty()) {
            return;
        }
        probeTable((ProbeContext) pc, modified, false, rightSources,
                (chunkOk, sourceKeyChunks) -> modifyByRight(chunkOk, sourceKeyChunks, modifiedSlotTracker));
    }

    protected abstract void modifyByRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    public void removeRight(Context pc, RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        probeTable((ProbeContext) pc, rightRowSet, true, rightSources,
                (chunkOk, sourceKeyChunks) -> removeRight(chunkOk, sourceKeyChunks, modifiedSlotTracker));
    }

    protected abstract void removeRight(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    public void buildFromRightSide(Table rightTable, ColumnSource<?>[] rightSources) {
        if (rightTable.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(CHUNK_SIZE, rightTable.size());
        try (BuildContext bc = new BuildContext(rightSources, chunkSize)) {
            buildTable(true, bc, rightTable.getRowSet(), rightSources,
                    this::buildFromRightSide,
                    null);
        }
    }

    @Override
    public void decorateLeftSide(RowSet leftRows, ColumnSource<?>[] leftSources, InitialBuildContext ibc) {
        if (leftRows.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(CHUNK_SIZE, leftRows.size());
        try (BuildContext bc = new BuildContext(leftSources, chunkSize)) {
            // we are not actually decorating the left side in the initial build context, we allow rehashes to occur
            // which means that we do not want to go back and maintain the hash slots. we instead will iterate the
            // complete hash table at the end to build our redirection index
            buildTable(true, bc, leftRows, leftSources,
                    this::buildFromLeftSide, null);
        }
    }

    protected abstract void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks);

    @Override
    public void addRightSide(Context bc, RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        buildTable(false, (BuildContext) bc, rightRowSet, rightSources,
                (chunkOk, sourceKeyChunks) -> addRightSide(chunkOk, sourceKeyChunks, modifiedSlotTracker),
                modifiedSlotTracker);
    }

    protected abstract void addRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    public void addLeftSide(Context bc, RowSequence leftRowSet, ColumnSource<?>[] leftSources,
            LongArraySource leftRedirections, @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (leftRowSet.isEmpty()) {
            return;
        }
        final MutableLong redirectionOffset = new MutableLong(0);
        buildTable(false, (BuildContext) bc, leftRowSet, leftSources, (chunkOk, sourceKeyChunks) -> {
            addLeftSide(chunkOk, sourceKeyChunks, leftRedirections, redirectionOffset.get(), modifiedSlotTracker);
            redirectionOffset.add(chunkOk.size());
        }, modifiedSlotTracker);
        // Perform the accumulated additions to each slot's left row set in a single bulk insert per slot.
        applyLeftAdditions(modifiedSlotTracker);
    }

    /**
     * Apply the left additions that were accumulated into the modified slot tracker by the generated
     * {@code addLeftSide} handler. Each slot's added keys are inserted into its left row set in a single bulk
     * {@link WritableRowSet#insert} call (rather than one key at a time). The tracker discards each slot's builder as
     * it is processed.
     */
    private void applyLeftAdditions(final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        modifiedSlotTracker.forAllLeftAdditions((slot, addedKeys) -> {
            final boolean main = (slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask;
            final long location = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;
            final WritableRowSet leftRowSet = main
                    ? mainLeftRowSet.getUnsafe(location)
                    : alternateLeftRowSet.getUnsafe(location);
            leftRowSet.insert(addedKeys);
        });
    }

    protected abstract void addLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            LongArraySource leftRedirections, long redirectionOffset,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    public void removeLeft(Context pc, RowSequence leftIndex, ColumnSource<?>[] leftSources,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (leftIndex.isEmpty()) {
            return;
        }
        probeTable((ProbeContext) pc, leftIndex, true, leftSources,
                (chunkOk, sourceKeyChunks) -> removeLeft(chunkOk, sourceKeyChunks, modifiedSlotTracker));
        applyLeftRemovals(modifiedSlotTracker);
    }

    /**
     * Apply the left removals that were accumulated into the modified slot tracker by the generated {@code removeLeft}
     * handler. Each slot's removed keys are removed from its left row set in a single bulk
     * {@link WritableRowSet#remove} call (rather than one key at a time), and a slot that becomes empty with no right
     * match is tombstoned. The tracker discards each slot's builder as it is processed.
     */
    private void applyLeftRemovals(final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        modifiedSlotTracker.forAllLeftRemovals((slot, removedKeys) -> {
            final boolean main = (slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask;
            final long location = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;
            final WritableRowSet leftRowSet = main
                    ? mainLeftRowSet.getUnsafe(location)
                    : alternateLeftRowSet.getUnsafe(location);
            leftRowSet.remove(removedKeys);
            if (leftRowSet.isEmpty()) {
                final long rightState = main
                        ? mainRightRowKey.getUnsafe(location)
                        : alternateRightRowKey.getUnsafe(location);
                if (rightState == RowSet.NULL_ROW_KEY) {
                    // the slot only existed because of left rows, and they are all gone now
                    if (main) {
                        mainRightRowKey.set(location, TOMBSTONE_RIGHT_STATE);
                    } else {
                        alternateRightRowKey.set(location, TOMBSTONE_RIGHT_STATE);
                    }
                    liveEntries--;
                }
            }
        });
    }

    @Override
    public void removeLeftModifications(
            final ColumnSource<?>[] leftSources,
            final RowSet modifiedPreShift, final RowSet modifiedPostShift,
            final RowSetBuilderSequential changedPreShift, final RowSetBuilderSequential changedPostShift,
            final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (modifiedPostShift.isEmpty()) {
            return;
        }
        final int numColumns = leftSources.length;
        final int chunkSize = (int) Math.min(CHUNK_SIZE, modifiedPostShift.size());

        final ChunkSource.FillContext[] prevContexts = new ChunkSource.FillContext[numColumns];
        final ChunkSource.GetContext[] currentContexts = new ChunkSource.GetContext[numColumns];
        // noinspection unchecked
        final WritableChunk<Values>[] prevKeys = new WritableChunk[numColumns];

        try (
                final SafeCloseableArray<ChunkSource.FillContext> ignored = new SafeCloseableArray<>(prevContexts);
                final SafeCloseableArray<ChunkSource.GetContext> ignored2 = new SafeCloseableArray<>(currentContexts);
                final SafeCloseableArray<WritableChunk<Values>> ignored3 = new SafeCloseableArray<>(prevKeys);
                final WritableBooleanChunk<Any> comparisonResults = WritableBooleanChunk.makeWritableChunk(chunkSize);
                final WritableLongChunk<OrderedRowKeys> compactedPreRowKeys =
                        WritableLongChunk.makeWritableChunk(chunkSize);
                final SharedContext prevShared = SharedContext.makeSharedContext();
                final SharedContext currentShared = SharedContext.makeSharedContext();
                final RowSequence.Iterator preIt = modifiedPreShift.getRowSequenceIterator();
                final RowSequence.Iterator postIt = modifiedPostShift.getRowSequenceIterator()) {
            for (int cc = 0; cc < numColumns; ++cc) {
                prevContexts[cc] = leftSources[cc].makeFillContext(chunkSize, prevShared);
                currentContexts[cc] = leftSources[cc].makeGetContext(chunkSize, currentShared);
                prevKeys[cc] = chunkTypes[cc].makeWritableChunk(chunkSize);
            }

            while (postIt.hasMore()) {
                final RowSequence preChunkRows = preIt.getNextRowSequenceWithLength(chunkSize);
                final RowSequence postChunkRows = postIt.getNextRowSequenceWithLength(chunkSize);

                // Initialize comparisonResults to true if previous and current are equal (the sense inverts later)
                final int chunkRsSize = postChunkRows.intSize();
                for (int cc = 0; cc < numColumns; ++cc) {
                    leftSources[cc].fillPrevChunk(prevContexts[cc], prevKeys[cc], preChunkRows);
                    final Chunk<? extends Values> currentValues =
                            leftSources[cc].getChunk(currentContexts[cc], postChunkRows);
                    if (cc == 0) {
                        keyChunkEquals[cc].equal(prevKeys[cc], currentValues, comparisonResults);
                    } else {
                        keyChunkEquals[cc].andEqual(prevKeys[cc], currentValues, comparisonResults);
                    }
                }

                final LongChunk<OrderedRowKeys> preKeys = preChunkRows.asRowKeyChunk();
                final LongChunk<OrderedRowKeys> postKeys = postChunkRows.asRowKeyChunk();
                int changedInChunk = 0;
                for (int ii = 0; ii < chunkRsSize; ++ii) {
                    final boolean changed = !comparisonResults.get(ii);
                    // Note that comparisonResults inverts meaning for the chunk compaction.
                    comparisonResults.set(ii, changed);
                    if (changed) {
                        changedPreShift.appendKey(preKeys.get(ii));
                        changedPostShift.appendKey(postKeys.get(ii));
                        compactedPreRowKeys.set(changedInChunk++, preKeys.get(ii));
                    }
                }
                comparisonResults.setSize(chunkRsSize);

                if (changedInChunk > 0) {
                    for (int cc = 0; cc < numColumns; ++cc) {
                        keyCompactKernels[cc].compact(prevKeys[cc], comparisonResults);
                    }
                    compactedPreRowKeys.setSize(changedInChunk);
                    // Accumulate the changed rows' removals into the per-slot builders in the tracker, keyed by their
                    // (previous-key) hash slots, reusing the previous key values we already read.
                    try (final RowSequence removeRows =
                            RowSequenceFactory.wrapRowKeysChunkAsRowSequence(compactedPreRowKeys)) {
                        removeLeft(removeRows, prevKeys, modifiedSlotTracker);
                    }
                }

                prevShared.reset();
                currentShared.reset();
            }
        }
        // Perform the accumulated removals from each slot's left row set in a single bulk remove per slot.
        applyLeftRemovals(modifiedSlotTracker);
    }

    protected abstract void removeLeft(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            NaturalJoinModifiedSlotTracker modifiedSlotTracker);

    @Override
    public void applyLeftShift(Context pc, ColumnSource<?>[] leftSources, RowSet shiftedRowSet, long shiftDelta) {
        if (shiftedRowSet.isEmpty()) {
            return;
        }
        final ProbeContext pc1 = (ProbeContext) pc;
        pc1.startShifts(shiftDelta);
        probeTable(pc1, shiftedRowSet, false, leftSources, (chunkOk, sourceKeyChunks) -> {
            pc1.ensureShiftCapacity(shiftDelta, chunkOk.size());
            applyLeftShift(chunkOk, sourceKeyChunks, shiftDelta, pc1);
        });
        for (int ii = pc1.pendingShiftPointer - 2; ii >= 0; ii -= 2) {
            final long location = pc1.pendingShifts.getUnsafe(ii);
            final long indexKey = pc1.pendingShifts.getUnsafe(ii + 1);
            if ((location & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) != 0) {
                shiftLeftIndexAlternate(location & AlternatingColumnSource.ALTERNATE_INNER_MASK, indexKey, shiftDelta);
            } else {
                shiftLeftIndexMain(location, indexKey, shiftDelta);
            }
        }
    }

    @Override
    public void applyRightShift(Context pc, ColumnSource<?>[] rightSources, RowSet shiftedRowSet, long shiftDelta,
            @NotNull NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (shiftedRowSet.isEmpty()) {
            return;
        }
        final ProbeContext pc1 = (ProbeContext) pc;
        pc1.startShifts(shiftDelta);
        probeTable(pc1, shiftedRowSet, false, rightSources, (chunkOk, sourceKeyChunks) -> {
            pc1.ensureShiftCapacity(shiftDelta, chunkOk.size());
            applyRightShift(chunkOk, sourceKeyChunks, shiftDelta, modifiedSlotTracker, pc1);
        });
        for (int ii = pc1.pendingShiftPointer - 2; ii >= 0; ii -= 2) {
            final long location = pc1.pendingShifts.getUnsafe(ii);
            final long indexKey = pc1.pendingShifts.getUnsafe(ii + 1);
            shiftRightDuplicate(location, indexKey, shiftDelta);
        }
    }

    private void shiftRightDuplicate(long duplicateLocation, long shiftedKey, long shiftDelta) {
        final WritableRowSet duplicate = rightSideDuplicateRowSets.getUnsafe(duplicateLocation);
        Assert.neqNull(duplicate, "duplicate");
        shiftOneKey(duplicate, shiftedKey, shiftDelta);
    }

    private void shiftLeftIndexMain(long tableLocation, long shiftedKey, long shiftDelta) {
        final WritableRowSet existingLeftRowSet = mainLeftRowSet.getUnsafe(tableLocation);
        Assert.neqNull(existingLeftRowSet, "existingLeftRowSet");
        shiftOneKey(existingLeftRowSet, shiftedKey, shiftDelta);
    }

    private void shiftLeftIndexAlternate(long tableLocation, long shiftedKey, long shiftDelta) {
        final WritableRowSet existingLeftRowSet = alternateLeftRowSet.getUnsafe(tableLocation);
        Assert.neqNull(existingLeftRowSet, "existingLeftRowSet");
        shiftOneKey(existingLeftRowSet, shiftedKey, shiftDelta);
    }

    protected abstract void applyLeftShift(RowSequence rowSequence, Chunk[] sourceKeyChunks, long shiftDelta,
            ProbeContext pc);

    @Override
    public BothIncrementalNaturalJoinStateManager.InitialBuildContext makeInitialBuildContext() {
        return null;
    }

    @Override
    public void decorateLeftSide(RowSet leftRowSet, ColumnSource<?>[] leftSources,
            LongArraySource leftRedirections) {
        throw new UnsupportedOperationException("Not used with right incremental.");
    }

    @Override
    public void compactAll() {
        for (int ii = 0; ii < tableSize; ++ii) {
            final WritableRowSet rowSet = mainLeftRowSet.get(ii);
            if (rowSet != null) {
                rowSet.compact();
            }
        }
        for (int ii = 0; ii < rehashPointer; ++ii) {
            final WritableRowSet rowSet = alternateLeftRowSet.get(ii);
            if (rowSet != null) {
                rowSet.compact();
            }
        }
        for (int ii = 0; ii < nextDuplicateRightSide; ++ii) {
            WritableRowSet rowSet = rightSideDuplicateRowSets.get(ii);
            if (rowSet != null) {
                rowSet.compact();
            }
        }
    }
}
