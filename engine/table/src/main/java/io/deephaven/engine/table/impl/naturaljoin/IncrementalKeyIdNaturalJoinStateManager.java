//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.DuplicateRightKeyException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BothIncrementalNaturalJoinStateManager;
import io.deephaven.engine.table.impl.IncrementalNaturalJoinStateManager;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.StaticNaturalJoinStateManager;
import io.deephaven.engine.table.impl.join.ChangedKeyRows;
import io.deephaven.engine.table.impl.join.IncrementalKeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;

/**
 * The natural join state manager when both tables refresh.
 * <p>
 * Each distinct key from either side has an id from an {@link IncrementalKeyIdHasherTypedBase}, and its state is kept
 * by id: its left rows, its right state, and its modified slot tracker cookie. The tracker's slots are these ids. A key
 * whose last left row and last right row are gone is tombstoned: its state and tracker entry are discarded at once, and
 * its id is removed from the hasher when the operation that emptied it finishes, so that a later key can reuse it.
 */
public class IncrementalKeyIdNaturalJoinStateManager extends StaticNaturalJoinStateManager
        implements IncrementalNaturalJoinStateManager, BothIncrementalNaturalJoinStateManager {

    private final IncrementalKeyIdHasherTypedBase hasher;

    // the left rows of each key, by id; null once the key is tombstoned
    private final ObjectArraySource<WritableRowSet> leftRowSet = new ObjectArraySource<>(WritableRowSet.class);
    // the right state of each key, by id: a right row key, NULL_ROW_KEY, or a duplicate location token
    private final LongArraySource rightState = new LongArraySource();
    // the modified slot tracker cookie of each key, by id
    private final LongArraySource modifiedTrackerCookieSource = new LongArraySource();

    private final RightDuplicateRowSets duplicates = new RightDuplicateRowSets();

    // detects which modified rows actually changed key value, on either side
    private final ChangedKeyRows changedKeyRows;

    // the ids tombstoned by the current operation, removed from the hasher when it finishes
    private final IntArrayList deadIds = new IntArrayList();

    /**
     * @param tableKeySources the key sources the hash table is built from
     * @param keySourcesForErrorMessages the left key sources that error messages render keys from
     * @param tableSize the initial number of hash table slots, a power of two
     * @param maximumLoadFactor the fraction of the slots that may be occupied before the table grows, greater than
     *        {@code 1 / IncrementalKeyIdHasherTypedBase.REHASH_SLOTS_PER_ENTRY}
     * @param joinType the join type
     * @param addOnly whether the right table only adds rows
     */
    public IncrementalKeyIdNaturalJoinStateManager(
            final ColumnSource<?>[] tableKeySources,
            final ColumnSource<?>[] keySourcesForErrorMessages,
            final int tableSize,
            final double maximumLoadFactor,
            final NaturalJoinType joinType,
            final boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);
        hasher = IncrementalKeyIdHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor);
        changedKeyRows = new ChangedKeyRows(
                Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new));
    }

    private void ensureIdCapacity() {
        final int capacity = hasher.idCapacity();
        leftRowSet.ensureCapacity(capacity);
        rightState.ensureCapacity(capacity);
        modifiedTrackerCookieSource.ensureCapacity(capacity);
    }

    // region initial build

    @Override
    public void buildFromRightSide(final Table rightTable, final ColumnSource<?>[] rightSources) {
        if (rightTable.isEmpty()) {
            return;
        }
        // nothing is waiting on the initial build, so it can rehash all at once
        hasher.buildWithFullRehash(rightTable.getRowSet(), rightSources, (rows, ids, statuses) -> {
            ensureIdCapacity();
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            for (int ii = 0; ii < ids.size(); ++ii) {
                final int id = ids.get(ii);
                final long inputKey = rowKeys.get(ii);
                if (statuses.get(ii) == KeyIdHasher.ADDED) {
                    leftRowSet.set(id, RowSetFactory.empty());
                    rightState.set(id, inputKey);
                    modifiedTrackerCookieSource.set(id, -1L);
                    continue;
                }
                final long existingRightRowKey = rightState.getUnsafe(id);
                if (existingRightRowKey == RowSet.NULL_ROW_KEY) {
                    rightState.set(id, inputKey);
                } else if (existingRightRowKey <= FIRST_DUPLICATE) {
                    duplicates.get(existingRightRowKey).insert(inputKey);
                } else if (addOnly && joinType == NaturalJoinType.FIRST_MATCH) {
                    // nop, we already have the first match
                } else if (addOnly && joinType == NaturalJoinType.LAST_MATCH) {
                    // always update the RHS key since this is the last match
                    rightState.set(id, inputKey);
                } else {
                    rightState.set(id, duplicates.allocate(RowSetFactory.fromKeys(existingRightRowKey, inputKey)));
                }
            }
        });
    }

    @Override
    public void decorateLeftSide(final RowSet leftRows, final ColumnSource<?>[] leftSources,
            final InitialBuildContext ibc) {
        if (leftRows.isEmpty()) {
            return;
        }
        // The redirection is built afterward from each key's left rows, so the ids of the left rows are not recorded;
        // a duplicate right key is reported then too, from a left table row key, since the rows built here may be
        // data index table rows.
        hasher.buildWithFullRehash(leftRows, leftSources, (rows, ids, statuses) -> {
            ensureIdCapacity();
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            for (int ii = 0; ii < ids.size(); ++ii) {
                final int id = ids.get(ii);
                if (statuses.get(ii) == KeyIdHasher.ADDED) {
                    leftRowSet.set(id, RowSetFactory.fromKeys(rowKeys.get(ii)));
                    rightState.set(id, RowSet.NULL_ROW_KEY);
                    modifiedTrackerCookieSource.set(id, -1L);
                } else {
                    leftRowSet.getUnsafe(id).insert(rowKeys.get(ii));
                }
            }
        });
    }

    @Override
    public void compactAll() {
        final int capacity = hasher.idCapacity();
        for (int id = 0; id < capacity; ++id) {
            final WritableRowSet rowSet = leftRowSet.getUnsafe(id);
            if (rowSet != null) {
                rowSet.compact();
            }
        }
        duplicates.compactAll();
    }

    private long getRightRowKeyFromState(final long leftRowKey, final long rightRowKeyForState) {
        if (rightRowKeyForState > FIRST_DUPLICATE) {
            return rightRowKeyForState;
        }
        if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
            throw new DuplicateRightKeyException("Natural Join found duplicate right key for "
                    + extractKeyStringFromSourceTable(leftRowKey));
        }
        final WritableRowSet rightRowSet = duplicates.get(rightRowKeyForState);
        return joinType == NaturalJoinType.FIRST_MATCH
                ? rightRowSet.firstRowKey()
                : rightRowSet.lastRowKey();
    }

    /**
     * Receives the left rows of a key and the right row key they are redirected to.
     */
    @FunctionalInterface
    private interface RedirectionConsumer {
        void accept(RowSet leftRows, long leftRowKey, long rightRowKey);
    }

    /**
     * Visit every key with left rows, after the initial build.
     *
     * @param indexRowSets if not null, the data index row set column; each key's single left row is a data index table
     *        row, which is replaced by the left rows of its group
     */
    private void forAllLeftKeys(final ColumnSource<RowSet> indexRowSets, final RedirectionConsumer consumer) {
        final int capacity = hasher.idCapacity();
        for (int id = 0; id < capacity; ++id) {
            final WritableRowSet leftRows = leftRowSet.getUnsafe(id);
            if (leftRows == null || leftRows.isEmpty()) {
                continue;
            }
            final RowSet keyLeftRows;
            if (indexRowSets == null) {
                keyLeftRows = leftRows;
            } else {
                Assert.eq(leftRows.size(), "leftRows.size()", 1);
                // Replace the single-key placeholder with the indexed row set.
                final RowSet leftRowSetForKey = indexRowSets.get(leftRows.firstRowKey());
                leftRowSet.set(id, leftRowSetForKey.copy());
                leftRows.close();
                keyLeftRows = leftRowSetForKey;
            }
            final long leftRowKey = keyLeftRows.firstRowKey();
            consumer.accept(keyLeftRows, leftRowKey, getRightRowKeyFromState(leftRowKey, rightState.getUnsafe(id)));
        }
    }

    private WritableRowRedirection buildKeyRowRedirection(final QueryTable leftTable,
            final ColumnSource<RowSet> indexRowSets, final JoinControl.RedirectionType redirectionType) {
        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat() || leftTable.getRowSet().lastRowKey() > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];
                forAllLeftKeys(indexRowSets, (leftRows, leftRowKey, rightRowKey) -> {
                    checkExactMatch(leftRowKey, rightRowKey);
                    // Set unconditionally, need to populate the entire array with NULL_ROW_KEY or the RHS key
                    leftRows.forAllRowKeys(pos -> innerIndex[(int) pos] = rightRowKey);
                });
                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();
                forAllLeftKeys(indexRowSets, (leftRows, leftRowKey, rightRowKey) -> {
                    if (rightRowKey == RowSet.NULL_ROW_KEY) {
                        checkExactMatch(leftRowKey, rightRowKey);
                    } else {
                        leftRows.forAllRowKeys(pos -> sparseRedirections.set(pos, rightRowKey));
                    }
                });
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection =
                        WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());
                forAllLeftKeys(indexRowSets, (leftRows, leftRowKey, rightRowKey) -> {
                    if (rightRowKey == RowSet.NULL_ROW_KEY) {
                        checkExactMatch(leftRowKey, rightRowKey);
                    } else {
                        leftRows.forAllRowKeys(pos -> rowRedirection.put(pos, rightRowKey));
                    }
                });
                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    @Override
    public WritableRowRedirection buildIndexedRowRedirection(final QueryTable leftTable,
            final InitialBuildContext ibc, final ColumnSource<RowSet> indexRowSets,
            final JoinControl.RedirectionType redirectionType) {
        return buildKeyRowRedirection(leftTable, indexRowSets, redirectionType);
    }

    @Override
    public WritableRowRedirection buildRowRedirectionFromRedirections(final QueryTable leftTable,
            final InitialBuildContext ibc, final JoinControl.RedirectionType redirectionType) {
        return buildKeyRowRedirection(leftTable, null, redirectionType);
    }

    @Override
    public InitialBuildContext makeInitialBuildContext() {
        return null;
    }

    // endregion initial build

    // region slot state

    /**
     * Read the right state of a key that is live. The empty and tombstone states are contract violations: the tombstone
     * shares its value with {@link #DUPLICATE_RIGHT_VALUE}, so a caller that asked about a dead key would misread it as
     * a duplicate.
     */
    private long rightStateForLiveSlot(final int slot) {
        final long state = rightState.getUnsafe(slot);
        Assert.neq(state, "rightState", EMPTY_RIGHT_STATE, "EMPTY_RIGHT_STATE");
        Assert.neq(state, "rightState", TOMBSTONE_RIGHT_STATE, "TOMBSTONE_RIGHT_STATE");
        return state;
    }

    @Override
    public long getRightRowKey(final int slot) {
        final long state = rightStateForLiveSlot(slot);
        if (state <= FIRST_DUPLICATE) {
            return DUPLICATE_RIGHT_VALUE;
        }
        return state;
    }

    @Override
    public RowSet getLeftRowSet(final int slot) {
        return leftRowSet.getUnsafe(slot);
    }

    @Override
    public RowSet getRightRowSet(final int slot) {
        return duplicates.get(rightStateForLiveSlot(slot));
    }

    @Override
    public String keyString(final int slot) {
        final long firstLeftRowKey = leftRowSet.getUnsafe(slot).firstRowKey();
        Assert.neq(firstLeftRowKey, "firstLeftRowKey", RowSet.NULL_ROW_KEY);
        return extractKeyStringFromSourceTable(firstLeftRowKey);
    }

    private void addMain(final int id, final long originalRightValue, final byte flags,
            final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        modifiedTrackerCookieSource.set(id, modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(id), id,
                originalRightValue, flags));
    }

    private void addMainRightAdd(final int id, final long originalRightValue, final long addedRightRowKey,
            final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        modifiedTrackerCookieSource.set(id, modifiedSlotTracker.addMainRightAdd(
                modifiedTrackerCookieSource.getUnsafe(id), id, originalRightValue, addedRightRowKey,
                NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
    }

    /**
     * Mark a key dead once its last left row and last right row are gone. Any modified slot tracker entry the key holds
     * this cycle describes the key that just died, so it is discarded rather than applied to a key that later reuses
     * the id. The key's (now empty) left row set is released, and its id is removed from the hasher by
     * {@link #removeDeadIds()} once the current operation finishes.
     */
    private void tombstoneSlot(final int id, final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        rightState.set(id, TOMBSTONE_RIGHT_STATE);
        modifiedSlotTracker.removeEntry(modifiedTrackerCookieSource.getUnsafe(id));
        modifiedTrackerCookieSource.set(id, -1L);
        leftRowSet.getAndSetUnsafe(id, null).close();
        deadIds.add(id);
    }

    /**
     * Remove the keys tombstoned by the current operation from the hasher, so that their ids can be reused.
     */
    private void removeDeadIds() {
        if (deadIds.isEmpty()) {
            return;
        }
        hasher.remove(IntChunk.chunkWrap(deadIds.elements(), 0, deadIds.size()));
        deadIds.clear();
    }

    // endregion slot state

    // region right updates

    @Override
    public Context makeProbeContext(final ColumnSource<?>[] probeSources, final long maxSize) {
        return new KeyIdJoinContext(hasher, probeSources, maxSize);
    }

    @Override
    public Context makeBuildContext(final ColumnSource<?>[] buildSources, final long maxSize) {
        return new KeyIdJoinContext(hasher, buildSources, maxSize);
    }

    @Override
    public void addRightSide(final Context bc, final RowSequence rightRowSet, final ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        ((KeyIdJoinContext) bc).build(rightRowSet, rightSources, (rowKeys, ids, statuses) -> {
            ensureIdCapacity();
            for (int ii = 0; ii < ids.size(); ++ii) {
                final int id = ids.get(ii);
                final long inputKey = rowKeys.get(ii);
                if (statuses.get(ii) == KeyIdHasher.ADDED) {
                    leftRowSet.set(id, RowSetFactory.empty());
                    rightState.set(id, inputKey);
                    modifiedTrackerCookieSource.set(id, modifiedSlotTracker.addMain(-1, id, EMPTY_RIGHT_STATE,
                            NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
                    continue;
                }
                final long existingRightRowKey = rightState.getUnsafe(id);
                if (existingRightRowKey == RowSet.NULL_ROW_KEY) {
                    rightState.set(id, inputKey);
                    addMain(id, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE,
                            modifiedSlotTracker);
                } else if (existingRightRowKey <= FIRST_DUPLICATE) {
                    final WritableRowSet duplicateRows = duplicates.get(existingRightRowKey);
                    final long duplicateSize = duplicateRows.size();
                    final long newKey = addRightRowKeyToDuplicates(duplicateRows, inputKey, joinType);
                    Assert.eq(duplicateSize, "duplicateSize", duplicateRows.size() - 1, "duplicates.size() - 1");
                    if (!leftRowSet.getUnsafe(id).isEmpty() && inputKey == newKey) {
                        // we have a new output key for the LHS rows
                        addMainRightAdd(id, existingRightRowKey, inputKey, modifiedSlotTracker);
                    }
                } else if (addOnly && joinType == NaturalJoinType.FIRST_MATCH) {
                    final long newKey = Math.min(existingRightRowKey, inputKey);
                    if (newKey != existingRightRowKey) {
                        rightState.set(id, newKey);
                        addMainRightAdd(id, existingRightRowKey, inputKey, modifiedSlotTracker);
                    }
                } else if (addOnly && joinType == NaturalJoinType.LAST_MATCH) {
                    final long newKey = Math.max(existingRightRowKey, inputKey);
                    if (newKey != existingRightRowKey) {
                        rightState.set(id, newKey);
                        addMainRightAdd(id, existingRightRowKey, inputKey, modifiedSlotTracker);
                    }
                } else {
                    final WritableRowSet duplicateRows = RowSetFactory.fromKeys(existingRightRowKey, inputKey);
                    rightState.set(id, duplicates.allocate(duplicateRows));
                    if (duplicateCreationChangesState(duplicateRows, existingRightRowKey, joinType)) {
                        addMainRightAdd(id, existingRightRowKey, inputKey, modifiedSlotTracker);
                    }
                }
            }
        });
    }

    @Override
    public void removeRight(final Context pc, final RowSequence rightRowSet, final ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        ((KeyIdJoinContext) pc).probe(rightRowSet, rightSources, true,
                (rowKeys, ids, statuses) -> removeRight(rowKeys, ids, statuses, modifiedSlotTracker));
        removeDeadIds();
    }

    private void removeRight(final LongChunk<OrderedRowKeys> rowKeys, final IntChunk<Values> ids,
            final ByteChunk<Values> statuses, final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        for (int ii = 0; ii < ids.size(); ++ii) {
            if (statuses.get(ii) != KeyIdHasher.FOUND) {
                throw Assert.statementNeverExecuted("Could not find existing state for removed right row");
            }
            final int id = ids.get(ii);
            final long inputKey = rowKeys.get(ii);
            final long existingRightRowKey = rightState.getUnsafe(id);
            if (existingRightRowKey <= FIRST_DUPLICATE) {
                final WritableRowSet duplicateRows = duplicates.get(existingRightRowKey);
                final long duplicateSize = duplicateRows.size();
                final long originalKey = removeRightRowKeyFromDuplicates(duplicateRows, inputKey, joinType);
                Assert.eq(duplicateSize, "duplicateSize", duplicateRows.size() + 1, "duplicates.size() + 1");
                if (!leftRowSet.getUnsafe(id).isEmpty() && originalKey == inputKey) {
                    // we have a new output key for the LHS rows
                    addMain(id, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE,
                            modifiedSlotTracker);
                }
                if (duplicateRows.size() == 1) {
                    rightState.set(id, getRightRowKeyFromDuplicates(duplicateRows, joinType));
                    duplicates.free(existingRightRowKey);
                }
            } else if (existingRightRowKey != inputKey) {
                throw Assert.statementNeverExecuted("Could not find existing right row in state");
            } else if (leftRowSet.getUnsafe(id).isEmpty()) {
                // the key's last right row is gone, and it has no left rows
                tombstoneSlot(id, modifiedSlotTracker);
            } else {
                rightState.set(id, RowSet.NULL_ROW_KEY);
                addMain(id, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE,
                        modifiedSlotTracker);
            }
        }
    }

    @Override
    public void removeRightModifications(
            final ColumnSource<?>[] rightSources,
            final RowSet modifiedPreShift, final RowSet modifiedPostShift,
            final RowSetBuilderSequential changedPreShift, final RowSetBuilderSequential changedPostShift,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (modifiedPostShift.isEmpty()) {
            return;
        }
        try (final KeyIdHasher.Context context =
                hasher.makeContext((int) Math.min(CHUNK_SIZE, modifiedPostShift.size()))) {
            changedKeyRows.findChanged(rightSources, modifiedPreShift, modifiedPostShift, changedPreShift,
                    changedPostShift,
                    (changedRows, previousKeys) -> KeyIdJoinContext.probeChunk(hasher, context, changedRows,
                            previousKeys,
                            (rowKeys, ids, statuses) -> removeRight(rowKeys, ids, statuses, modifiedSlotTracker)));
        }
        removeDeadIds();
    }

    @Override
    public void modifyByRight(final Context pc, final RowSet modified, final ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (modified.isEmpty()) {
            return;
        }
        ((KeyIdJoinContext) pc).probe(modified, rightSources, false, (rowKeys, ids, statuses) -> {
            for (int ii = 0; ii < ids.size(); ++ii) {
                if (statuses.get(ii) != KeyIdHasher.FOUND) {
                    throw Assert.statementNeverExecuted("Could not find existing state for modified right row");
                }
                final int id = ids.get(ii);
                final long existingRightRowKey = rightState.getUnsafe(id);
                // a key with several right rows redirects its left rows to just one of them, so a modification of any
                // other duplicate row leaves the left rows' values unchanged
                final boolean selectedRightRowModified =
                        !IncrementalNaturalJoinStateManager.isDuplicateRightState(existingRightRowKey)
                                || getRightRowKeyFromDuplicates(duplicates.get(existingRightRowKey),
                                        joinType) == rowKeys.get(ii);
                if (selectedRightRowModified) {
                    addMain(id, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_MODIFY_PROBE,
                            modifiedSlotTracker);
                }
            }
        });
    }

    @Override
    public void applyRightShift(final Context pc, final ColumnSource<?>[] rightSources, final RowSet shiftedRowSet,
            final long shiftDelta, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (shiftedRowSet.isEmpty()) {
            return;
        }
        final KeyIdJoinContext jc = (KeyIdJoinContext) pc;
        jc.startShifts(shiftDelta);
        jc.probe(shiftedRowSet, rightSources, false, (rowKeys, ids, statuses) -> {
            for (int ii = 0; ii < ids.size(); ++ii) {
                if (statuses.get(ii) != KeyIdHasher.FOUND) {
                    throw Assert.statementNeverExecuted("Could not find existing state for shifted right row");
                }
                final int id = ids.get(ii);
                final long existingRightRowKey = rightState.getUnsafe(id);
                final long keyToShift = rowKeys.get(ii);
                if (existingRightRowKey == keyToShift - shiftDelta) {
                    rightState.set(id, keyToShift);
                    addMain(id, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT,
                            modifiedSlotTracker);
                } else if (existingRightRowKey <= FIRST_DUPLICATE) {
                    if (shiftDelta < 0) {
                        shiftOneKey(duplicates.get(existingRightRowKey), keyToShift, shiftDelta);
                    } else {
                        jc.addPendingShift(RightDuplicateRowSets.locationFromState(existingRightRowKey), keyToShift);
                    }
                    if (!leftRowSet.getUnsafe(id).isEmpty()) {
                        // we may have a new output key for the LHS rows
                        addMain(id, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT,
                                modifiedSlotTracker);
                    }
                } else {
                    throw Assert.statementNeverExecuted("Could not find existing index for shifted right row");
                }
            }
        });
        jc.forAllPendingShiftsReversed((location, shiftedRowKey) -> {
            final WritableRowSet duplicateRows = duplicates.getAtLocation(location);
            Assert.neqNull(duplicateRows, "duplicate");
            shiftOneKey(duplicateRows, shiftedRowKey, shiftDelta);
        });
    }

    // endregion right updates

    // region left updates

    @Override
    public void addLeftSide(final Context bc, final RowSequence leftRows, final ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker,
            final boolean addedToTable) {
        if (leftRows.isEmpty()) {
            return;
        }
        final long[] offset = {0};
        ((KeyIdJoinContext) bc).build(leftRows, leftSources, (rowKeys, ids, statuses) -> {
            ensureIdCapacity();
            final long chunkOffset = offset[0];
            for (int ii = 0; ii < ids.size(); ++ii) {
                final int id = ids.get(ii);
                final long leftRowKey = rowKeys.get(ii);
                if (statuses.get(ii) == KeyIdHasher.ADDED) {
                    // a new (or reused) id has no existing left rows: build the row set from this key directly
                    leftRowSet.set(id, RowSetFactory.fromKeys(leftRowKey));
                    rightState.set(id, RowSet.NULL_ROW_KEY);
                    modifiedTrackerCookieSource.set(id, -1L);
                    leftRedirections.set(chunkOffset + ii, RowSet.NULL_ROW_KEY);
                    continue;
                }
                final long rightRowKeyForState = rightState.getUnsafe(id);
                final long rightRowKey;
                if (rightRowKeyForState <= FIRST_DUPLICATE) {
                    if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE
                            || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
                        throw new DuplicateRightKeyException("Natural Join found duplicate right key for "
                                + extractKeyStringFromSourceTable(leftRowKey));
                    }
                    rightRowKey = getRightRowKeyFromDuplicates(duplicates.get(rightRowKeyForState), joinType);
                } else {
                    rightRowKey = rightRowKeyForState;
                }
                // accumulate the added key for one bulk insert per key, rather than inserting one at a time
                modifiedTrackerCookieSource.set(id, modifiedSlotTracker.addLeftAddition(
                        modifiedTrackerCookieSource.getUnsafe(id), id, leftRowKey, rightRowKeyForState));
                leftRedirections.set(chunkOffset + ii, rightRowKey);
            }
            offset[0] = chunkOffset + ids.size();
        });
        // Perform the accumulated additions to each key's left row set in a single bulk insert per key.
        modifiedSlotTracker.forAllLeftAdditions(addedToTable,
                (slot, addedKeys) -> leftRowSet.getUnsafe(slot).insert(addedKeys));
    }

    @Override
    public void removeLeft(final Context pc, final RowSequence leftRows, final ColumnSource<?>[] leftSources,
            final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (leftRows.isEmpty()) {
            return;
        }
        ((KeyIdJoinContext) pc).probe(leftRows, leftSources, true,
                (rowKeys, ids, statuses) -> removeLeft(rowKeys, ids, statuses, modifiedSlotTracker));
        applyLeftRemovals(modifiedSlotTracker);
        removeDeadIds();
    }

    private void removeLeft(final LongChunk<OrderedRowKeys> rowKeys, final IntChunk<Values> ids,
            final ByteChunk<Values> statuses, final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        for (int ii = 0; ii < ids.size(); ++ii) {
            if (statuses.get(ii) != KeyIdHasher.FOUND) {
                throw Assert.statementNeverExecuted("Could not find existing state for removed left row");
            }
            final int id = ids.get(ii);
            final long rightRowKeyForState = rightState.getUnsafe(id);
            final WritableRowSet left = leftRowSet.getUnsafe(id);
            if (left.size() == 1) {
                // single-row key: removing empties it, so remove directly and skip the tracker
                left.remove(rowKeys.get(ii));
                if (rightRowKeyForState == RowSet.NULL_ROW_KEY) {
                    // no right match remains, so the key is now dead
                    tombstoneSlot(id, modifiedSlotTracker);
                }
            } else {
                // multi-row key: accumulate for one bulk remove per key
                modifiedTrackerCookieSource.set(id, modifiedSlotTracker.addLeftRemoval(
                        modifiedTrackerCookieSource.getUnsafe(id), id, rowKeys.get(ii), rightRowKeyForState));
            }
        }
    }

    /**
     * Apply the left removals accumulated in the modified slot tracker by {@link #removeLeft}. Each key's removed rows
     * are removed from its left row set in a single bulk remove, and a key that becomes empty with no right match is
     * tombstoned.
     */
    private void applyLeftRemovals(final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        modifiedSlotTracker.forAllLeftRemovals((slot, removedKeys) -> {
            final WritableRowSet left = leftRowSet.getUnsafe(slot);
            left.remove(removedKeys);
            if (left.isEmpty() && rightState.getUnsafe(slot) == RowSet.NULL_ROW_KEY) {
                // the key only existed because of left rows, and they are all gone now
                tombstoneSlot(slot, modifiedSlotTracker);
            }
        });
    }

    @Override
    public void removeLeftModifications(
            final ColumnSource<?>[] leftSources,
            final RowSet modifiedPreShift, final RowSet modifiedPostShift,
            final RowSetBuilderSequential changedPreShift, final RowSetBuilderSequential changedPostShift,
            final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        // The changed rows' removals accumulate into the per-key builders in the tracker, keyed by their previous-key
        // ids, reusing the previous key values read for the comparison.
        if (modifiedPostShift.isEmpty()) {
            return;
        }
        try (final KeyIdHasher.Context context =
                hasher.makeContext((int) Math.min(CHUNK_SIZE, modifiedPostShift.size()))) {
            changedKeyRows.findChanged(leftSources, modifiedPreShift, modifiedPostShift, changedPreShift,
                    changedPostShift,
                    (changedRows, previousKeys) -> KeyIdJoinContext.probeChunk(hasher, context, changedRows,
                            previousKeys,
                            (rowKeys, ids, statuses) -> removeLeft(rowKeys, ids, statuses, modifiedSlotTracker)));
        }
        // Perform the accumulated removals from each key's left row set in a single bulk remove per key.
        applyLeftRemovals(modifiedSlotTracker);
        removeDeadIds();
    }

    @Override
    public void applyLeftShift(final Context pc, final ColumnSource<?>[] leftSources, final RowSet shiftedRowSet,
            final long shiftDelta, @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (shiftedRowSet.isEmpty()) {
            return;
        }
        // Accumulate each shifted row's post-shift key into its key's builder in the tracker; the left row sets are
        // then moved in bulk, one remove of the pre-shift keys and one insert of the post-shift keys per key, rather
        // than one key at a time.
        ((KeyIdJoinContext) pc).probe(shiftedRowSet, leftSources, false, (rowKeys, ids, statuses) -> {
            for (int ii = 0; ii < ids.size(); ++ii) {
                if (statuses.get(ii) != KeyIdHasher.FOUND) {
                    throw Assert.statementNeverExecuted("Could not find existing state for shifted left row");
                }
                final int id = ids.get(ii);
                modifiedTrackerCookieSource.set(id, modifiedSlotTracker.addLeftShift(
                        modifiedTrackerCookieSource.getUnsafe(id), id, rowKeys.get(ii), rightState.getUnsafe(id)));
            }
        });
        modifiedSlotTracker.forAllLeftShifts((slot, shiftedKeys) -> {
            final WritableRowSet left = leftRowSet.getUnsafe(slot);
            try (final WritableRowSet preShiftKeys = shiftedKeys.shift(-shiftDelta)) {
                left.remove(preShiftKeys);
            }
            left.insert(shiftedKeys);
        });
    }

    @Override
    public void decorateLeftSide(final RowSet leftRowSet, final ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections) {
        throw new UnsupportedOperationException("Not used with both incremental.");
    }

    // endregion left updates
}
