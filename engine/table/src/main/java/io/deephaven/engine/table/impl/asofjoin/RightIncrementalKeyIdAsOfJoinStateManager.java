//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.join.IncrementalKeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext;
import io.deephaven.util.QueryConstants;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

/**
 * The as-of join state manager when the right table is refreshing, and the left table is static or refreshing.
 * <p>
 * Each distinct key has an id from an {@link IncrementalKeyIdHasherTypedBase}, which is the key's slot: its state byte
 * and the builder, row set or SSA of each side are kept by id. Ids never move, so a slot reported by a build or probe
 * stays valid until {@link #releaseEmptyBuckets()} removes its key, after which a later key may reuse the id.
 * <p>
 * Each operation that gathers rows by slot gives every slot it touches a cookie, numbered from zero in the order the
 * slots are first touched; the caller's slots and sequential builders are indexed by cookie, so that the operation
 * reports each slot once. A slot's cookie is stamped with the operation's generation, so a new operation starts afresh
 * without clearing them.
 */
public class RightIncrementalKeyIdAsOfJoinStateManager extends RightIncrementalHashedAsOfJoinStateManager {

    /**
     * The state of a slot whose key has been released; a candidate that repeats is skipped once released.
     */
    private static final byte RELEASED_STATE = 0x40;

    private final IncrementalKeyIdHasherTypedBase hasher;

    /**
     * Each side of a slot is held by a builder while its rows are first collected. When the slot is activated, meaning
     * it has rows on the other side, the side becomes an SSA; a side that is updated while inactive becomes a row set.
     * The state byte tells which each side holds, using a nibble for the left and a nibble for the right.
     */
    private final ObjectArraySource<Object> leftRowSet = new ObjectArraySource<>(Object.class);
    private final ObjectArraySource<Object> rightRowSet = new ObjectArraySource<>(Object.class);
    private final ByteArraySource state = new ByteArraySource();

    // the generation-stamped cookie of each slot, by id
    private final LongArraySource cookies = new LongArraySource();
    private long cookieGeneration;
    private int nextCookie;

    // the slots whose sides may have become empty this cycle, which may repeat
    private final IntegerArraySource tombstoneCandidates = new IntegerArraySource();
    private int tombstoneCandidateCount = 0;

    // the ids of the keys released by releaseEmptyBuckets, removed from the hasher together
    private final IntArrayList deadIds = new IntArrayList();

    /**
     * @param tableKeySources the key sources the hash table is built from
     * @param keySourcesForErrorMessages the key sources that error messages render keys from
     * @param tableSize the initial number of hash table slots, a power of two
     * @param maximumLoadFactor the fraction of the slots that may be occupied before the table grows, greater than
     *        {@code 1 / IncrementalKeyIdHasherTypedBase.REHASH_SLOTS_PER_ENTRY}
     */
    public RightIncrementalKeyIdAsOfJoinStateManager(
            final ColumnSource<?>[] tableKeySources,
            final ColumnSource<?>[] keySourcesForErrorMessages,
            final int tableSize,
            final double maximumLoadFactor) {
        super(keySourcesForErrorMessages);
        hasher = IncrementalKeyIdHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor);
    }

    private void ensureIdCapacity() {
        final int capacity = hasher.idCapacity();
        leftRowSet.ensureCapacity(capacity);
        rightRowSet.ensureCapacity(capacity);
        state.ensureCapacity(capacity);
        cookies.ensureCapacity(capacity);
    }

    private void resetCookie() {
        cookieGeneration += 10 + nextCookie;
        nextCookie = 0;
    }

    /**
     * @return the cookie of {@code slot} in the current operation, giving it the next one if it has none yet
     */
    private int getCookie(final int slot) {
        long cookie = cookies.getUnsafe(slot);
        if (cookie == QueryConstants.NULL_LONG || cookie < cookieGeneration) {
            cookies.set(slot, cookie = cookieGeneration + nextCookie);
            nextCookie++;
        }
        return (int) (cookie - cookieGeneration);
    }

    private void addLeftKey(final int slot, final long rowKey, final byte currentState) {
        if ((currentState & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_EMPTY) {
            state.set(slot, (byte) ((currentState & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_BUILDER));
            leftRowSet.set(slot, newBuilder(rowKey));
        } else {
            ((RowSetBuilderSequential) leftRowSet.getUnsafe(slot)).appendKey(rowKey);
        }
    }

    private void addRightKey(final int slot, final long rowKey, final byte currentState) {
        if ((currentState & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_EMPTY) {
            state.set(slot, (byte) ((currentState & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_BUILDER));
            rightRowSet.set(slot, newBuilder(rowKey));
        } else {
            ((RowSetBuilderSequential) rightRowSet.getUnsafe(slot)).appendKey(rowKey);
        }
    }

    private static RowSetBuilderSequential newBuilder(final long rowKey) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        builder.appendKey(rowKey);
        return builder;
    }

    /**
     * Give a key that a build just added a fresh state, since its id may have belonged to a released key.
     */
    private void resetSlot(final int slot) {
        state.set(slot, (byte) (ENTRY_RIGHT_IS_EMPTY | ENTRY_LEFT_IS_EMPTY));
        leftRowSet.set(slot, null);
        rightRowSet.set(slot, null);
    }

    @Override
    public long getNumEntries() {
        return hasher.size();
    }

    @Override
    public int getSlotCapacity() {
        return hasher.idCapacity();
    }

    // region builds

    /**
     * The initial build of one side: each row's rows go to its slot's builder for that side, and each slot touched is
     * reported at its cookie, continuing the cookies of an earlier initial build.
     */
    private int buildInitial(final boolean isLeftSide, final RowSequence rows, final ColumnSource<?>[] sources,
            final IntegerArraySource slots) {
        final int startCookie = nextCookie;
        // nothing has been removed yet, so the initial build may rehash all at once
        hasher.buildWithFullRehash(rows, sources,
                (chunkRows, ids, statuses) -> buildChunk(isLeftSide, chunkRows, ids, statuses, slots, null));
        return nextCookie - startCookie;
    }

    @Override
    public int buildFromLeftSide(final RowSequence leftRows, final ColumnSource<?>[] leftSources,
            @NotNull final IntegerArraySource addedSlots) {
        return buildInitial(true, leftRows, leftSources, addedSlots);
    }

    @Override
    public int buildFromRightSide(final RowSequence rightRows, final ColumnSource<?>[] rightSources,
            @NotNull final IntegerArraySource addedSlots, final int usedSlots) {
        return usedSlots + buildInitial(false, rightRows, rightSources, addedSlots);
    }

    @Override
    public int buildAdditions(final boolean isLeftSide, final RowSet additions, final ColumnSource<?>[] sources,
            final IntegerArraySource slots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        resetCookie();
        hasher.build(additions, sources,
                (chunkRows, ids, statuses) -> buildChunk(isLeftSide, chunkRows, ids, statuses, slots,
                        sequentialBuilders));
        return nextCookie;
    }

    /**
     * Gather the rows of one built chunk by slot. With {@code sequentialBuilders}, each slot's rows go to the builder
     * at its cookie and a new key starts with both sides empty; without, they go to the slot's builder for the side.
     */
    private void buildChunk(final boolean isLeftSide, final RowSequence chunkRows, final IntChunk<Values> ids,
            final ByteChunk<Values> statuses, final IntegerArraySource slots,
            @Nullable final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        ensureIdCapacity();
        // each built row reports at most one new slot
        final long slotCapacity = (long) nextCookie + ids.size();
        slots.ensureCapacity(slotCapacity);
        if (sequentialBuilders != null) {
            sequentialBuilders.ensureCapacity(slotCapacity);
        }
        final LongChunk<OrderedRowKeys> rowKeys = chunkRows.asRowKeyChunk();
        final int size = ids.size();
        for (int ii = 0; ii < size; ++ii) {
            final int slot = ids.get(ii);
            final long rowKey = rowKeys.get(ii);
            final int cookie = getCookie(slot);
            slots.set(cookie, slot);
            if (statuses.get(ii) == KeyIdHasher.ADDED) {
                resetSlot(slot);
            }
            if (sequentialBuilders != null) {
                addToSequentialBuilder(cookie, sequentialBuilders, rowKey);
            } else if (isLeftSide) {
                addLeftKey(slot, rowKey, state.getUnsafe(slot));
            } else {
                addRightKey(slot, rowKey, state.getUnsafe(slot));
            }
        }
    }

    // endregion builds

    // region probes

    /**
     * A context for probing many row sets in one cycle: the key chunks and the hasher context, allocated once.
     */
    private static final class ProbeContext extends BuildOrProbeContext {
        private final KeyIdHasher.Context hasherContext;
        private final Chunk<Values>[] keyChunks;

        private ProbeContext(final KeyIdHasher hasher, final ColumnSource<?>[] sources, final long maxSize) {
            super(sources, (int) Math.max(1, Math.min(CHUNK_SIZE, maxSize)));
            hasherContext = hasher.makeContext(chunkSize);
            // noinspection unchecked
            keyChunks = new Chunk[sources.length];
        }

        @Override
        public void close() {
            hasherContext.close();
            super.close();
        }
    }

    @FunctionalInterface
    private interface ProbeChunkHandler {
        void accept(LongChunk<OrderedRowKeys> rowKeys, IntChunk<Values> ids);
    }

    private void probe(final ProbeContext pc, final RowSequence rows, final ColumnSource<?>[] sources,
            final boolean usePrev, final ProbeChunkHandler handler) {
        try (final RowSequence.Iterator rsIt = rows.getRowSequenceIterator()) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(pc.chunkSize);
                if (usePrev) {
                    getPrevKeyChunks(sources, pc.getContexts, pc.keyChunks, chunkOk);
                } else {
                    getKeyChunks(sources, pc.getContexts, pc.keyChunks, chunkOk);
                }
                hasher.probe(pc.hasherContext, pc.keyChunks);
                handler.accept(chunkOk.asRowKeyChunk(), pc.hasherContext.ids());
                pc.resetSharedContexts();
            }
        }
    }

    @Override
    public Context makeProbeContext(final ColumnSource<?>[] probeSources, final long maxSize) {
        return new ProbeContext(hasher, probeSources, maxSize);
    }

    @Override
    public void probeRightInitial(final RowSequence rowsToProbe, final ColumnSource<?>[] rightSources) {
        if (rowsToProbe.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = new ProbeContext(hasher, rightSources, rowsToProbe.size())) {
            probe(pc, rowsToProbe, rightSources, false, (rowKeys, ids) -> {
                final int size = ids.size();
                for (int ii = 0; ii < size; ++ii) {
                    final int slot = ids.get(ii);
                    if (slot != KeyIdHasher.NULL_ID) {
                        addRightKey(slot, rowKeys.get(ii), state.getUnsafe(slot));
                    }
                }
            });
        }
    }

    @Override
    public int markForRemoval(final RowSet restampRemovals, final ColumnSource<?>[] sources,
            final IntegerArraySource slots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateRowSets(restampRemovals, sources, slots, sequentialBuilders, true);
    }

    @Override
    public int probeAdditions(final RowSet restampAdditions, final ColumnSource<?>[] sources,
            final IntegerArraySource slots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateRowSets(restampAdditions, sources, slots, sequentialBuilders, false);
    }

    @Override
    public int gatherShiftRowSet(final RowSet restampAdditions, final ColumnSource<?>[] sources,
            final IntegerArraySource slots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateRowSets(restampAdditions, sources, slots, sequentialBuilders, true);
    }

    @Override
    public int gatherShiftRowSet(final Context probeContext, final RowSet rowSet, final ColumnSource<?>[] sources,
            final IntegerArraySource slots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateRowSets((ProbeContext) probeContext, rowSet, sources, slots, sequentialBuilders, true);
    }

    @Override
    public int gatherModifications(final RowSet restampAdditions, final ColumnSource<?>[] sources,
            final IntegerArraySource slots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateRowSets(restampAdditions, sources, slots, sequentialBuilders, false);
    }

    private int accumulateRowSets(final RowSet rowSet, final ColumnSource<?>[] sources,
            final IntegerArraySource slots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders,
            final boolean usePrev) {
        if (rowSet.isEmpty()) {
            resetCookie();
            return 0;
        }
        try (final ProbeContext pc = new ProbeContext(hasher, sources, rowSet.size())) {
            return accumulateRowSets(pc, rowSet, sources, slots, sequentialBuilders, usePrev);
        }
    }

    /**
     * Gather the rows of {@code rowSet} whose keys are in the table by slot: each slot is reported once, at its cookie,
     * and its rows go to the sequential builder at the same cookie.
     *
     * @return the number of slots reported
     */
    private int accumulateRowSets(final ProbeContext pc, final RowSet rowSet, final ColumnSource<?>[] sources,
            final IntegerArraySource slots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders,
            final boolean usePrev) {
        resetCookie();
        if (rowSet.isEmpty()) {
            return 0;
        }
        probe(pc, rowSet, sources, usePrev, (rowKeys, ids) -> {
            // each probed row reports at most one new slot, and there are at most as many slots as live keys
            final long slotCapacity = Math.min((long) nextCookie + ids.size(), hasher.size());
            slots.ensureCapacity(slotCapacity);
            sequentialBuilders.ensureCapacity(slotCapacity);
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int slot = ids.get(ii);
                if (slot == KeyIdHasher.NULL_ID) {
                    continue;
                }
                final int cookie = getCookie(slot);
                slots.set(cookie, slot);
                addToSequentialBuilder(cookie, sequentialBuilders, rowKeys.get(ii));
            }
        });
        return nextCookie;
    }

    // endregion probes

    // region slot state

    @Override
    public byte getState(final int slot) {
        return state.getUnsafe(slot);
    }

    @Override
    public WritableRowSet getAndClearLeftRowSet(final int slot) {
        final Object leftSide = leftRowSet.getAndSetUnsafe(slot, null);
        if (leftSide == null) {
            return null;
        }
        // this might already be a row set loaded from an index table
        if (leftSide instanceof WritableRowSet) {
            return (WritableRowSet) leftSide;
        }
        return ((RowSetBuilderSequential) leftSide).build();
    }

    @Override
    public SegmentedSortedArray getRightSsa(final int slot) {
        if ((state.getUnsafe(slot) & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_SSA) {
            return (SegmentedSortedArray) rightRowSet.getUnsafe(slot);
        }
        throw new IllegalStateException();
    }

    @Override
    public SegmentedSortedArray getRightSsa(final int slot, final Function<RowSet, SegmentedSortedArray> ssaFactory) {
        final byte entryType = state.getUnsafe(slot);
        final byte ssaState = (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_SSA);
        return getSsa(slot, getRightEntryType(entryType), rightRowSet, ssaState, ssaFactory);
    }

    @Override
    public SegmentedSortedArray getLeftSsa(final int slot) {
        if ((state.getUnsafe(slot) & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_SSA) {
            return (SegmentedSortedArray) leftRowSet.getUnsafe(slot);
        }
        throw new IllegalStateException();
    }

    @Override
    public SegmentedSortedArray getLeftSsa(final int slot, final Function<RowSet, SegmentedSortedArray> ssaFactory) {
        final byte entryType = state.getUnsafe(slot);
        final byte ssaState = (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_SSA);
        return getSsa(slot, leftEntryAsRightType(entryType), leftRowSet, ssaState, ssaFactory);
    }

    /**
     * Get one side of a slot as an SSA, making it from whatever the side holds if it is not one yet.
     *
     * @param sideType the entry type of the side, expressed with the right side constants
     */
    private SegmentedSortedArray getSsa(final int slot, final byte sideType, final ObjectArraySource<Object> side,
            final byte ssaState, final Function<RowSet, SegmentedSortedArray> ssaFactory) {
        final RowSet rowSet;
        switch (sideType) {
            case ENTRY_RIGHT_IS_SSA:
                return (SegmentedSortedArray) side.getUnsafe(slot);
            case ENTRY_RIGHT_IS_EMPTY:
                rowSet = RowSetFactory.empty();
                break;
            case ENTRY_RIGHT_IS_ROWSET:
                rowSet = (RowSet) side.getUnsafe(slot);
                break;
            case ENTRY_RIGHT_IS_BUILDER: {
                final RowSetBuilderSequential builder = (RowSetBuilderSequential) side.getUnsafe(slot);
                rowSet = builder == null ? RowSetFactory.empty() : builder.build();
                break;
            }
            default:
                throw new IllegalStateException();
        }
        state.set(slot, ssaState);
        final SegmentedSortedArray ssa = ssaFactory.apply(rowSet);
        rowSet.close();
        side.set(slot, ssa);
        return ssa;
    }

    @Override
    public SegmentedSortedArray getLeftSsaOrRowSet(final int slot, final MutableObject<WritableRowSet> indexOutput) {
        final byte entryType = state.getUnsafe(slot);
        final byte rowSetState = (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_ROWSET);
        return getSsaOrRowSet(slot, leftEntryAsRightType(entryType), leftRowSet, rowSetState, indexOutput);
    }

    @Override
    public SegmentedSortedArray getRightSsaOrRowSet(final int slot, final MutableObject<WritableRowSet> indexOutput) {
        final byte entryType = state.getUnsafe(slot);
        final byte rowSetState = (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_ROWSET);
        return getSsaOrRowSet(slot, getRightEntryType(entryType), rightRowSet, rowSetState, indexOutput);
    }

    /**
     * Get one side of a slot as an SSA if it is one; otherwise make it a row set, store that in {@code indexOutput},
     * and return null.
     *
     * @param sideType the entry type of the side, expressed with the right side constants
     */
    @Nullable
    private SegmentedSortedArray getSsaOrRowSet(final int slot, final byte sideType,
            final ObjectArraySource<Object> side, final byte rowSetState,
            final MutableObject<WritableRowSet> indexOutput) {
        final WritableRowSet rowSet;
        switch (sideType) {
            case ENTRY_RIGHT_IS_SSA:
                return (SegmentedSortedArray) side.getUnsafe(slot);
            case ENTRY_RIGHT_IS_ROWSET:
                indexOutput.setValue((WritableRowSet) side.getUnsafe(slot));
                return null;
            case ENTRY_RIGHT_IS_EMPTY:
                rowSet = RowSetFactory.empty();
                break;
            case ENTRY_RIGHT_IS_BUILDER:
                rowSet = ((RowSetBuilderSequential) side.getUnsafe(slot)).build();
                break;
            default:
                throw new IllegalStateException();
        }
        side.set(slot, rowSet);
        state.set(slot, rowSetState);
        indexOutput.setValue(rowSet);
        return null;
    }

    @Override
    public WritableRowSet getLeftRowSet(final int slot) {
        final byte entryType = state.getUnsafe(slot);
        final byte rowSetState = (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_ROWSET);
        return getRowSet(slot, leftEntryAsRightType(entryType), leftRowSet, rowSetState);
    }

    @Override
    public WritableRowSet getRightRowSet(final int slot) {
        final byte entryType = state.getUnsafe(slot);
        final byte rowSetState = (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_ROWSET);
        return getRowSet(slot, getRightEntryType(entryType), rightRowSet, rowSetState);
    }

    /**
     * Get one side of a slot as a row set, building it from the side's builder if it is one.
     *
     * @param sideType the entry type of the side, expressed with the right side constants
     */
    private WritableRowSet getRowSet(final int slot, final byte sideType, final ObjectArraySource<Object> side,
            final byte rowSetState) {
        if (sideType == ENTRY_RIGHT_IS_ROWSET) {
            return (WritableRowSet) side.getUnsafe(slot);
        }
        if (sideType == ENTRY_RIGHT_IS_BUILDER) {
            final WritableRowSet rowSet = ((RowSetBuilderSequential) side.getUnsafe(slot)).build();
            side.set(slot, rowSet);
            state.set(slot, rowSetState);
            return rowSet;
        }
        throw new IllegalStateException();
    }

    @Override
    public void setLeftRowSet(final int slot, final RowSet rowSet) {
        final byte entryType = state.getUnsafe(slot);
        if ((entryType & ENTRY_LEFT_MASK) != ENTRY_LEFT_IS_EMPTY) {
            throw new IllegalStateException();
        }
        state.set(slot, (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_ROWSET));
        leftRowSet.set(slot, rowSet);
    }

    @Override
    public void setRightRowSet(final int slot, final RowSet rowSet) {
        final byte entryType = state.getUnsafe(slot);
        if ((entryType & ENTRY_RIGHT_MASK) != ENTRY_RIGHT_IS_EMPTY) {
            throw new IllegalStateException();
        }
        state.set(slot, (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_ROWSET));
        rightRowSet.set(slot, rowSet);
    }

    @Override
    public void populateLeftRowSetsFromIndexTable(final IntegerArraySource slots, final int slotCount,
            final ColumnSource<RowSet> rowSetSource) {
        populateFromIndexTable(slots, slotCount, rowSetSource, leftRowSet, ENTRY_RIGHT_MASK, ENTRY_LEFT_IS_ROWSET);
    }

    @Override
    public void populateRightRowSetsFromIndexTable(final IntegerArraySource slots, final int slotCount,
            final ColumnSource<RowSet> rowSetSource) {
        populateFromIndexTable(slots, slotCount, rowSetSource, rightRowSet, ENTRY_LEFT_MASK, ENTRY_RIGHT_IS_ROWSET);
    }

    /**
     * Replace the builder of one side of each slot, which holds the slot's single data index row, with a copy of that
     * index row's row set; the original belongs to the index.
     *
     * @param otherSideMask the state mask of the other side, which is kept
     * @param rowSetType the state of the side when it holds a row set
     */
    private void populateFromIndexTable(final IntegerArraySource slots, final int slotCount,
            final ColumnSource<RowSet> rowSetSource, final ObjectArraySource<Object> side, final byte otherSideMask,
            final byte rowSetType) {
        for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
            final int slot = slots.getInt(slotIndex);
            final RowSetBuilderSequential builder = (RowSetBuilderSequential) side.getUnsafe(slot);
            if (builder == null) {
                continue;
            }
            final byte entryType = state.getUnsafe(slot);
            try (final WritableRowSet indexRows = builder.build()) {
                if (indexRows.isEmpty()) {
                    side.set(slot, null);
                    state.set(slot, (byte) (entryType & otherSideMask));
                } else if (indexRows.size() == 1) {
                    side.set(slot, rowSetSource.get(indexRows.firstRowKey()).copy());
                    state.set(slot, (byte) ((entryType & otherSideMask) | rowSetType));
                } else {
                    throw new IllegalStateException("Index-built row set should have exactly one value: " + indexRows);
                }
            }
        }
    }

    // endregion slot state

    // region release

    @Override
    public void ensureTombstoneCandidateCapacity(final int additionalCandidates) {
        tombstoneCandidates.ensureCapacity((long) tombstoneCandidateCount + additionalCandidates);
    }

    @Override
    public void addTombstoneCandidate(final int slot) {
        tombstoneCandidates.set(tombstoneCandidateCount++, slot);
    }

    @Override
    public void releaseEmptyBuckets() {
        for (int ii = 0; ii < tombstoneCandidateCount; ++ii) {
            final int slot = tombstoneCandidates.getUnsafe(ii);
            final byte entryType = state.getUnsafe(slot);
            if (entryType == RELEASED_STATE) {
                continue;
            }
            final byte leftType = leftEntryAsRightType(entryType);
            final byte rightType = getRightEntryType(entryType);
            final Object leftSide = leftRowSet.getUnsafe(slot);
            final Object rightSide = rightRowSet.getUnsafe(slot);
            if (!isSideEmpty(leftType, leftSide) || !isSideEmpty(rightType, rightSide)) {
                continue;
            }
            if (leftType == ENTRY_RIGHT_IS_ROWSET) {
                ((WritableRowSet) leftSide).close();
            }
            if (rightType == ENTRY_RIGHT_IS_ROWSET) {
                ((WritableRowSet) rightSide).close();
            }
            leftRowSet.set(slot, null);
            rightRowSet.set(slot, null);
            state.set(slot, RELEASED_STATE);
            deadIds.add(slot);
        }
        tombstoneCandidateCount = 0;
        if (!deadIds.isEmpty()) {
            hasher.remove(IntChunk.chunkWrap(deadIds.elements(), 0, deadIds.size()));
            deadIds.clear();
        }
    }

    /**
     * @param sideType the entry type of one side of a slot, expressed with the right side constants
     * @param sideObject the builder, row set, or SSA that holds that side
     * @return true if that side of the slot holds no rows
     */
    private static boolean isSideEmpty(final byte sideType, final Object sideObject) {
        switch (sideType) {
            case ENTRY_RIGHT_IS_EMPTY:
                return true;
            case ENTRY_RIGHT_IS_BUILDER:
                // a builder is only created to hold a row
                return false;
            case ENTRY_RIGHT_IS_ROWSET:
                return ((RowSet) sideObject).isEmpty();
            case ENTRY_RIGHT_IS_SSA:
                return ((SegmentedSortedArray) sideObject).size() == 0;
            default:
                throw new IllegalStateException("Unexpected side type " + sideType);
        }
    }

    // endregion release
}
