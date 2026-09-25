//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones
        implements IncrementalOperatorAggregationStateManager {
    /** The number of rehashes begun by all instances, for benchmarking. */
    public static final java.util.concurrent.atomic.LongAdder REHASH_COUNT =
            new java.util.concurrent.atomic.LongAdder();

    public static final int CHUNK_SIZE = ChunkedOperatorAggregationHelper.CHUNK_SIZE;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size

    /*
     * This is an invalid output position, so we can use it to represent a deleted state. It must equal UNKNOWN_ROW: the
     * generated findPositionForKey returns the state of the slot whose key matches, so a removed key reports unknown.
     */
    protected static final int TOMBSTONE_STATE = UNKNOWN_ROW;

    /** The number of slots in our table. */
    protected int tableSize;

    /**
     * The number of slots in our alternate table, to start with "1" is a lie, but rehashPointer is zero; so our
     * location value is positive and can be compared against rehashPointer safely
     */
    protected int alternateTableSize = 1;

    /** How many entries are taking up slots in the main hash table (includes tombstones)? */
    protected long numEntries = 0;
    /** How many values do we have that are live (in both main and alternate)? */
    protected long liveEntries = 0;
    /** How many entries are in the alternate table (includes tombstones)? */
    protected long alternateEntries = 0;

    /** Should we rehash the entire table fully ({@code true}) or incrementally ({@code false})? */
    protected boolean fullRehash = true;

    /** How much of the alternate sources are necessary to rehash? */
    protected int rehashPointer = 0;

    /**
     * The table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor or if
     * it falls below minimum load factor we will instead contract the table.
     */
    private final double maximumLoadFactor;

    /** The keys for our hash entries. */
    protected final WritableColumnSource[] mainKeySources;

    /** The keys for our hash entries, for the old alternative smaller table. */
    protected final WritableColumnSource[] alternateKeySources;


    /** Our state value used when nothing is there. */
    protected static final int EMPTY_OUTPUT_POSITION = QueryConstants.NULL_INT;

    /**
     * The state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation).
     */
    protected ImmutableIntArraySource mainOutputPosition = new ImmutableIntArraySource();

    /**
     * The state value for the bucket, parallel to alternateKeySources (the state is an output row key for the
     * aggregation).
     */
    protected ImmutableIntArraySource alternateOutputPosition;

    /**
     * Used as a row redirection for the output key sources, updated using the mainInsertMask to identify the main vs.
     * alternate values.
     */
    protected final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    /** State variables that exist as part of the update. */
    protected MutableInt nextOutputPosition;
    protected WritableIntChunk<RowKeys> outputPositions;

    /** Output alternating column sources. */
    protected AlternatingColumnSource[] alternatingColumnSources;

    protected final WritableRowSet freeOutputPositions = RowSetFactory.empty();

    /**
     * The mask for insertion into the main table (this tells our alternating column sources which of the two sources to
     * access for a given key).
     */
    protected int mainInsertMask = 0;

    protected IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBaseWithTombstones(
            ColumnSource<?>[] tableKeySources,
            int tableSize,
            double maximumLoadFactor) {
        this.tableSize = tableSize;
        Require.inRange(tableSize, "tableSize", MAX_TABLE_SIZE + 1, "MAX_TABLE_SIZE + 1");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        alternateKeySources = new WritableColumnSource[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;
        mainOutputPosition.ensureCapacity(tableSize);
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
                if ((long) nextOutputPosition.get() + nextChunkSize > Integer.MAX_VALUE) {
                    // output positions are never reused, so a long-lived aggregation whose keys churn can run out
                    throw new UnsupportedOperationException(
                            "Aggregation output positions exhausted: " + nextOutputPosition.get()
                                    + " states have been created");
                }
                outputPositionToHashSlot.ensureCapacity(nextOutputPosition.get() + nextChunkSize, false);
                while (doRehash(bc.rehashCredits, nextChunkSize)) {
                    migrateFront();
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

        if (!rehashRequired(nextChunkSize)) {
            return false;
        }

        if (rehashPointer > 0) {
            // The rehash in progress chose a size at which its alternate drains before the main table fills; let it
            // finish.
            return false;
        }

        // Every slot in the main table is filled by a migration or an insert, so it holds at most the live entries plus
        // the entries inserted since the rehash began; a tombstone marks a slot that was already counted. Each insert
        // is preceded by examining REHASH_SLOTS_PER_ENTRY alternate slots (see the generated rehashInternalPartial), so
        // the alternate drains before the main table needs another rehash if the inserts that fit under the load factor
        // pay for examining every alternate slot. Doubling always satisfies that when live entries force the rehash.
        // When tombstones alone crossed the load factor, the current size may satisfy it, and the rehash leaves the
        // tombstones behind. A full rehash, used while building the initial state, completes immediately, so it only
        // needs room for the live entries.
        final int oldTableSize = tableSize;
        while (!alternateDrainsInTime(oldTableSize, nextChunkSize)) {
            tableSize *= 2;

            if (tableSize < 0 || tableSize > MAX_TABLE_SIZE) {
                throw new UnsupportedOperationException("Hash table exceeds maximum size!");
            }
        }

        // we can't give the caller credit for rehashes with the old table, we need to begin migrating things again
        if (rehashCredits.get() > 0) {
            rehashCredits.set(0);
        }

        if (fullRehash) {
            // States are only removed once update cycles begin, so a full rehash never sees a tombstone and the table
            // has grown above.
            Assert.eq(numEntries, "numEntries", liveEntries, "liveEntries");
            // if we are doing a full rehash, we need to ditch the alternate
            if (rehashPointer > 0) {
                // TODO: this change probably belongs in the non-tombstone version as well!
                rehashInternalPartial((int) alternateEntries);
                clearAlternate();
            }

            rehashInternalFull(oldTableSize);

            return false;
        }

        REHASH_COUNT.increment();
        setupNewAlternate(oldTableSize);
        adviseNewAlternate();

        return true;
    }

    /** The alternate slots a partial rehash examines for each entry inserted; matches the generated hashers. */
    private static final int REHASH_SLOTS_PER_ENTRY = 3;

    /**
     * @param alternateSize the size the alternate table will have
     * @param nextChunkSize the size of the chunk about to be built
     * @return whether, at the current {@link #tableSize}, the alternate is migrated before the main table fills
     */
    private boolean alternateDrainsInTime(final int alternateSize, final int nextChunkSize) {
        final double insertsBeforeFull = tableSize * maximumLoadFactor - liveEntries - nextChunkSize;
        if (fullRehash) {
            return insertsBeforeFull >= 0;
        }
        return insertsBeforeFull * REHASH_SLOTS_PER_ENTRY >= alternateSize;
    }

    /**
     * After creating the new alternate key states, advise the derived classes, so they can cast them to the typed
     * versions of the column source and adjust the derived class pointers.
     */
    protected abstract void adviseNewAlternate();

    protected void clearAlternate() {
        Assert.eqZero(alternateEntries, "alternateEntries");
        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = null;
        }
        this.alternateOutputPosition = null;
    }

    /**
     * @param numEntriesToRehash number of entries to rehash into main table
     * @return actual number of entries rehashed
     */
    protected abstract int rehashInternalPartial(int numEntriesToRehash);

    // full rehashInternal
    protected abstract void rehashInternalFull(int oldSize);

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + alternateEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    protected int hashToTableLocationAlternate(int hash) {
        return hash & (alternateTableSize - 1);
    }

    @Override
    abstract public int findPositionForKey(Object key);

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

        alternateOutputPosition = mainOutputPosition;
        mainOutputPosition = new ImmutableIntArraySource();
        mainOutputPosition.ensureCapacity(tableSize);
        if (mainInsertMask == 0) {
            if (alternatingColumnSources != null) {
                for (int ai = 0; ai < alternatingColumnSources.length; ++ai) {
                    alternatingColumnSources[ai].setSources(alternateKeySources[ai], mainKeySources[ai]);
                }
            }
            mainInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
        } else {
            if (alternatingColumnSources != null) {
                for (int ai = 0; ai < alternatingColumnSources.length; ++ai) {
                    alternatingColumnSources[ai].setSources(mainKeySources[ai], alternateKeySources[ai]);
                }
            }
            mainInsertMask = 0;
        }
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return makeBuildContext(buildSources, maxSize);
    }

    @Override
    public void add(
            @NotNull final SafeCloseable bc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final MutableInt nextOutputPosition,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.nextOutputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        buildTable((BuildContext) bc, rowSequence, sources, this::build);
        this.outputPositions = null;
        this.nextOutputPosition = null;
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final RowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[mainKeySources.length];
        Assert.eqNull(alternatingColumnSources, "alternatingColumnSources");
        alternatingColumnSources = new AlternatingColumnSource[mainKeySources.length];
        for (int kci = 0; kci < mainKeySources.length; ++kci) {
            final Class<?> dataType = mainKeySources[kci].getType();
            final Class<?> componentType = mainKeySources[kci].getComponentType();
            if (mainInsertMask == 0) {
                alternatingColumnSources[kci] = new AlternatingColumnSource<>(dataType, componentType,
                        mainKeySources[kci], alternateKeySources[kci]);
            } else {
                alternatingColumnSources[kci] = new AlternatingColumnSource<>(dataType, componentType,
                        alternateKeySources[kci], mainKeySources[kci]);
            }
            // noinspection unchecked
            keyHashTableSources[kci] =
                    RedirectedColumnSource.maybeRedirect(resultIndexToHashSlot, alternatingColumnSources[kci]);
        }

        return keyHashTableSources;
    }

    @Override
    public boolean canReclaim() {
        return true;
    }

    @Override
    public void removeStates(RowSet removed) {
        liveEntries -= removed.intSize();
        // A tombstone keeps its key: probes and builds stop at a tombstone whose key matches, which is only correct
        // if a deleted slot cannot be mistaken for a live key's slot. The keys are released when a rehash drops the
        // tombstone or a new state reuses the slot.
        freeOutputPositions.insert(removed);
        removed.forAllRowKeys(this::tombstone);
    }

    @Override
    public void tombstoneStates(final RowSet removed) {
        liveEntries -= removed.intSize();
        removed.forAllRowKeys(this::tombstone);
    }

    @Override
    public void shiftAllOutputPositions(final RowSet liveStates, final RowSetShiftData shiftData) {
        // removed states' slots are tombstones, which must stay tombstones, so only the live states are updated; the
        // ranges may cover positions that hold no state, whose entries in outputPositionToHashSlot are stale
        for (int ri = 0; ri < shiftData.size(); ++ri) {
            final long delta = shiftData.getShiftDelta(ri);
            try (final RowSet moving =
                    liveStates.subSetByKeyRange(shiftData.getBeginRange(ri), shiftData.getEndRange(ri))) {
                moving.forAllRowKeys(outputPosition -> {
                    final int hashSlot = outputPositionToHashSlot.getUnsafe(outputPosition);
                    final int slot = Math.toIntExact(hashSlot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
                    final int newOutputPosition = Math.toIntExact(outputPosition + delta);
                    if ((hashSlot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
                        mainOutputPosition.set(slot, newOutputPosition);
                    } else {
                        alternateOutputPosition.set(slot, newOutputPosition);
                    }
                });
            }
        }
        outputPositionToHashSlot.shift(shiftData);
    }

    @Override
    public void shiftOutputPositions(final RowSetShiftData shiftData) {
        final RowSetShiftData.Iterator it = shiftData.applyIterator();
        while (it.hasNext()) {
            it.next();
            final long begin = it.beginRange();
            final long end = it.endRange();
            final long delta = it.shiftDelta();
            // states only move toward lower positions, so walking forward never overwrites a state yet to move
            Assert.leqZero(delta, "delta");
            for (long pos = begin; pos <= end; pos++) {
                final int hashSlot = outputPositionToHashSlot.getUnsafe(pos);
                final int newOutputPosition = Math.toIntExact(pos + delta);
                outputPositionToHashSlot.set(newOutputPosition, hashSlot);
                final int slot = Math.toIntExact(hashSlot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
                if ((hashSlot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
                    mainOutputPosition.set(slot, newOutputPosition);
                } else {
                    alternateOutputPosition.set(slot, newOutputPosition);
                }
            }
        }
    }

    @Override
    public void releaseOutputPositionBlocks(final long firstOutputPosition, final long lastOutputPosition) {
        outputPositionToHashSlot.releaseBlocks(firstOutputPosition, lastOutputPosition);
    }

    private void tombstone(final long outputPosition) {
        // we never actually delete anything from the output position table; the state is live, so it is in range
        final int hashSlot = outputPositionToHashSlot.getUnsafe(outputPosition);
        final int slot = Math.toIntExact(hashSlot & AlternatingColumnSource.ALTERNATE_INNER_MASK);
        if ((hashSlot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            mainOutputPosition.set(slot, TOMBSTONE_STATE);
            return;
        }
        alternateOutputPosition.set(slot, TOMBSTONE_STATE);
    }

    @Override
    public void reclaimFreedRows(TrackingWritableRowSet resultRowset, TableUpdateImpl downstream,
            MutableInt nextOutputPosition, long maxShiftedStates, IterativeChunkedAggregationOperator[] operators) {
        if (freeOutputPositions.isEmpty()) {
            resultRowset.remove(downstream.removed());
            resultRowset.insert(downstream.added());
            return;
        }

        // we need to clear out the results if we are freeing slots
        final int originalLastSlot = nextOutputPosition.get() - 1;

        // we can easily reclaim anything past the end of the table
        final RowSet.SearchIterator revit = freeOutputPositions.reverseIterator();;
        while (revit.hasNext()) {
            final long lastFreePosition = revit.nextLong();
            if (lastFreePosition + 1 == nextOutputPosition.get()) {
                nextOutputPosition.set(Math.toIntExact(lastFreePosition));
            } else {
                break;
            }
        }
        // no longer free, we'll just use them as necessary
        freeOutputPositions.removeRange(nextOutputPosition.get(), Long.MAX_VALUE);

        // move no more states than the input rows added, modified, and removed this cycle
        final MutableLong shiftedValues = new MutableLong();
        final MutableLong firstFreeKey = new MutableLong(freeOutputPositions.firstRowKey());

        try (final WritableRowSet effectiveRowSet = resultRowset.copy()) {
            // we need the added positions to be accounted for
            effectiveRowSet.insert(downstream.added);
            // and the free positions to be removed, so that we know the leftover ranges
            effectiveRowSet.remove(freeOutputPositions);
            // and we already removed things that were at the end
            effectiveRowSet.removeRange(nextOutputPosition.get(), Long.MAX_VALUE);

            final RowSet.RangeIterator rangeIterator = effectiveRowSet.rangeIterator();

            final RowSetShiftData.Builder shiftDataBuilder = new RowSetShiftData.Builder();

            final boolean completed = freeOutputPositions.forEachRowKeyRange((long start, long end) -> {
                if (!rangeIterator.advance(end + 1)) {
                    throw new IllegalStateException("Free output positions went past the end of the result rowset!");
                }
                final long firstToShift = rangeIterator.currentRangeStart();
                long lastToShift = rangeIterator.currentRangeEnd();
                // we've exhausted the effective rowset, so have nothing left to do on subsequent
                final boolean exhaustedResultRowset = lastToShift == effectiveRowSet.lastRowKey();

                final long permittedShift = maxShiftedStates - shiftedValues.get();
                if (lastToShift - firstToShift > permittedShift) {
                    lastToShift = firstToShift + permittedShift;
                }

                // we want to fill in the free destination
                final long shiftDelta = firstFreeKey.get() - firstToShift;
                shiftDataBuilder.shiftRange(firstToShift, lastToShift, shiftDelta);
                shiftedValues.add(lastToShift - firstToShift + 1);
                firstFreeKey.set(lastToShift + shiftDelta + 1);

                return shiftedValues.get() < maxShiftedStates && !exhaustedResultRowset;
            });

            // we've figured out what we should shift, let's do it now
            downstream.shifted = shiftDataBuilder.build();

            shiftOutputPositions(downstream.shifted);

            // shift the indices
            resultRowset.remove(downstream.removed());
            downstream.shifted().apply(resultRowset.writableCast());
            downstream.shifted().apply(downstream.added.writableCast());
            resultRowset.insert(downstream.added());
            downstream.shifted().apply(downstream.modified.writableCast());


            if (completed) {
                Assert.assertion(resultRowset.isFlat(), "resultRowset.isFlat()");
            }

            // don't leave a useless gap at the end
            nextOutputPosition.set(Math.toIntExact(resultRowset.lastRowKey() + 1));
            // fix up the operators
            for (int oi = 0; oi < operators.length; ++oi) {
                operators[oi].shift(downstream.shifted);
            }
            if (nextOutputPosition.get() <= originalLastSlot) {
                for (int oi = 0; oi < operators.length; ++oi) {
                    operators[oi].clear(nextOutputPosition.get(), originalLastSlot);
                }
            }

            // we've actually freed these positions now
            final RowSet toFree = RowSetFactory.flat(resultRowset.lastRowKey() + 1).minus(resultRowset);
            freeOutputPositions.resetTo(toFree);
        }
    }

    @Override
    public void beginUpdateCycle() {
        // Once we're past initial state processing, we want to rehash incrementally.
        fullRehash = false;
        // At the beginning of the update cycle, we always want to do some rehash work so that we can eventually ditch
        // the alternate table.
        if (rehashPointer > 0) {
            rehashInternalPartial(CHUNK_SIZE);
            if (rehashPointer == 0) {
                clearAlternate();
            }
        }
    }

    protected abstract void probe(RowSequence chunkOk, Chunk[] sourceKeyChunks);

    @Override
    public void remove(
            @NotNull final SafeCloseable pc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        probeTable((ProbeContext) pc, rowSequence, true, sources, this::probe);
        this.outputPositions = null;
    }

    @Override
    public void findModifications(
            @NotNull final SafeCloseable pc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPositions = outputPositions;
        probeTable((ProbeContext) pc, rowSequence, false, sources, this::probe);
        this.outputPositions = null;
    }

    @Override
    public void startTrackingPrevValues() {}
}
