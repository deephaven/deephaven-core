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
import io.deephaven.engine.table.impl.IncrementalNaturalJoinStateManager;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.RightIncrementalNaturalJoinStateManager;
import io.deephaven.engine.table.impl.join.ChangedKeyRows;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.join.KeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.util.ContiguousWritableRowRedirection;
import io.deephaven.engine.table.impl.util.LongColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirectionLockFree;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;

/**
 * The natural join state manager when the right table refreshes and the left table is static.
 * <p>
 * The left table's keys are built into a {@link KeyIdHasherTypedBase} once; a right key that is not on the left can
 * never match, so right rows only probe and the table never changes afterward. Each key's state is kept by id: its left
 * rows, its right state, and its modified slot tracker cookie. The tracker's slots are these ids.
 */
public class RightIncrementalKeyIdNaturalJoinStateManager extends RightIncrementalNaturalJoinStateManager {

    private final KeyIdHasherTypedBase hasher;

    // the left rows of each key, by id
    private final ObjectArraySource<WritableRowSet> leftRowSet = new ObjectArraySource<>(WritableRowSet.class);
    // the right state of each key, by id: a right row key, NULL_ROW_KEY, or a duplicate location token
    private final LongArraySource rightRowKey = new LongArraySource();
    // the modified slot tracker cookie of each key, by id
    private final LongArraySource modifiedTrackerCookieSource = new LongArraySource();

    private final RightDuplicateRowSets duplicates = new RightDuplicateRowSets();

    // detects which modified right rows actually changed key value
    private final ChangedKeyRows changedKeyRows;

    /**
     * @param tableKeySources the key sources the hash table is built from
     * @param keySourcesForErrorMessages the left key sources that error messages render keys from
     * @param tableSize the initial number of hash table slots, a power of two
     * @param maximumLoadFactor the fraction of the slots that may be occupied before the table grows
     * @param joinType the join type
     * @param addOnly whether the right table only adds rows
     */
    public RightIncrementalKeyIdNaturalJoinStateManager(
            final ColumnSource<?>[] tableKeySources,
            final ColumnSource<?>[] keySourcesForErrorMessages,
            final int tableSize,
            final double maximumLoadFactor,
            final NaturalJoinType joinType,
            final boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);
        hasher = KeyIdHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor);
        changedKeyRows = new ChangedKeyRows(
                Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new));
    }

    private void ensureIdCapacity() {
        final int capacity = hasher.idCapacity();
        leftRowSet.ensureCapacity(capacity);
        rightRowKey.ensureCapacity(capacity);
        modifiedTrackerCookieSource.ensureCapacity(capacity);
    }

    @Override
    public void buildFromLeftSide(final Table leftTable, final ColumnSource<?>[] leftSources,
            final InitialBuildContext ibc) {
        if (leftTable.isEmpty()) {
            return;
        }
        hasher.build(leftTable.getRowSet(), leftSources, (rows, ids, statuses) -> {
            ensureIdCapacity();
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int id = ids.get(ii);
                final long leftRowKey = rowKeys.get(ii);
                if (statuses.get(ii) == KeyIdHasher.ADDED) {
                    leftRowSet.set(id, RowSetFactory.fromKeys(leftRowKey));
                    rightRowKey.set(id, RowSet.NULL_ROW_KEY);
                    modifiedTrackerCookieSource.set(id, -1L);
                } else {
                    leftRowSet.getUnsafe(id).insert(leftRowKey);
                }
            }
        });
    }

    @Override
    public void convertLeftDataIndex(final int groupingSize, final InitialBuildContext ibc,
            final ColumnSource<RowSet> rowSetSource) {
        final int capacity = hasher.idCapacity();
        for (int id = 0; id < capacity; ++id) {
            final WritableRowSet leftRows = leftRowSet.getUnsafe(id);
            if (leftRows.isEmpty()) {
                throw new IllegalStateException("When converting left group position an empty LHS rowset was found!");
            }
            if (leftRows.size() != 1) {
                throw new IllegalStateException(
                        "When converting left group position to row keys more than one LHS value was found!");
            }
            // Replace the single-key placeholder with the indexed row set.
            leftRowSet.set(id, rowSetSource.get(leftRows.firstRowKey()).copy());
            leftRows.close();
        }
    }

    @Override
    public void addRightSide(final RowSequence rightRowSet, final ColumnSource<?>[] rightSources) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        try (final KeyIdJoinContext pc = new KeyIdJoinContext(hasher, rightSources, rightRowSet.size())) {
            pc.probe(rightRowSet, rightSources, false, (rowKeys, ids, statuses) -> {
                for (int ii = 0; ii < ids.size(); ++ii) {
                    if (statuses.get(ii) != KeyIdHasher.FOUND) {
                        // a right key that is not on the left matches nothing
                        continue;
                    }
                    final int id = ids.get(ii);
                    final long inputKey = rowKeys.get(ii);
                    final long rightRowKeyForState = rightRowKey.getUnsafe(id);
                    if (rightRowKeyForState == RowSet.NULL_ROW_KEY) {
                        // we have a matching LHS row, add this new RHS row
                        rightRowKey.set(id, inputKey);
                    } else if (rightRowKeyForState <= FIRST_DUPLICATE) {
                        // another duplicate, add it to the list
                        duplicates.get(rightRowKeyForState).insert(inputKey);
                    } else if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE
                            || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
                        throw new DuplicateRightKeyException("Natural Join found duplicate right key for "
                                + extractKeyStringFromSourceTable(leftRowSet.getUnsafe(id).firstRowKey()));
                    } else if (addOnly && joinType == NaturalJoinType.FIRST_MATCH) {
                        // nop, we already have the first match
                    } else if (addOnly && joinType == NaturalJoinType.LAST_MATCH) {
                        // we have a later match
                        rightRowKey.set(id, inputKey);
                    } else {
                        // create a duplicate rowset and add the new row to it
                        rightRowKey.set(id,
                                duplicates.allocate(RowSetFactory.fromKeys(rightRowKeyForState, inputKey)));
                    }
                }
            });
        }
    }

    private long addMain(final int id, final long originalRightValue, final byte flags,
            final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        final long cookie = modifiedSlotTracker.addMain(modifiedTrackerCookieSource.getUnsafe(id), id,
                originalRightValue, flags);
        modifiedTrackerCookieSource.set(id, cookie);
        return cookie;
    }

    private void addMainRightAdd(final int id, final long originalRightValue, final long addedRightRowKey,
            final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        modifiedTrackerCookieSource.set(id, modifiedSlotTracker.addMainRightAdd(
                modifiedTrackerCookieSource.getUnsafe(id), id, originalRightValue, addedRightRowKey,
                NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE));
    }

    @Override
    public void addRightSide(final Context pc, final RowSequence rightRowSet, final ColumnSource<?>[] rightSources,
            @NotNull final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        if (rightRowSet.isEmpty()) {
            return;
        }
        ((KeyIdJoinContext) pc).probe(rightRowSet, rightSources, false, (rowKeys, ids, statuses) -> {
            for (int ii = 0; ii < ids.size(); ++ii) {
                if (statuses.get(ii) != KeyIdHasher.FOUND) {
                    // a right key that is not on the left matches nothing
                    continue;
                }
                final int id = ids.get(ii);
                final long inputKey = rowKeys.get(ii);
                final long rightRowKeyForState = rightRowKey.getUnsafe(id);
                if (rightRowKeyForState == RowSet.NULL_ROW_KEY) {
                    // we have a matching LHS row, add this new RHS row
                    rightRowKey.set(id, inputKey);
                    addMain(id, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE,
                            modifiedSlotTracker);
                } else if (rightRowKeyForState <= FIRST_DUPLICATE) {
                    // another duplicate, add it to the list
                    final WritableRowSet duplicateRows = duplicates.get(rightRowKeyForState);
                    final long duplicateSize = duplicateRows.size();
                    final long newKey = addRightRowKeyToDuplicates(duplicateRows, inputKey, joinType);
                    Assert.eq(duplicateSize, "duplicateSize", duplicateRows.size() - 1, "duplicates.size() - 1");
                    if (inputKey == newKey) {
                        // we have a new output key for the LHS rows
                        addMainRightAdd(id, rightRowKeyForState, inputKey, modifiedSlotTracker);
                    }
                } else if (joinType == NaturalJoinType.ERROR_ON_DUPLICATE
                        || joinType == NaturalJoinType.EXACTLY_ONE_MATCH) {
                    throw new DuplicateRightKeyException("Natural Join found duplicate right key for "
                            + extractKeyStringFromSourceTable(leftRowSet.getUnsafe(id).firstRowKey()));
                } else if (addOnly && joinType == NaturalJoinType.FIRST_MATCH) {
                    final long newKey = Math.min(rightRowKeyForState, inputKey);
                    if (newKey != rightRowKeyForState) {
                        rightRowKey.set(id, newKey);
                        addMainRightAdd(id, rightRowKeyForState, inputKey, modifiedSlotTracker);
                    }
                } else if (addOnly && joinType == NaturalJoinType.LAST_MATCH) {
                    final long newKey = Math.max(rightRowKeyForState, inputKey);
                    if (newKey != rightRowKeyForState) {
                        rightRowKey.set(id, newKey);
                        addMainRightAdd(id, rightRowKeyForState, inputKey, modifiedSlotTracker);
                    }
                } else {
                    // create a duplicate rowset and add the new row to it
                    final WritableRowSet duplicateRows = RowSetFactory.fromKeys(rightRowKeyForState, inputKey);
                    rightRowKey.set(id, duplicates.allocate(duplicateRows));
                    if (duplicateCreationChangesState(duplicateRows, rightRowKeyForState, joinType)) {
                        addMainRightAdd(id, rightRowKeyForState, inputKey, modifiedSlotTracker);
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
    }

    private void removeRight(final LongChunk<OrderedRowKeys> rowKeys, final IntChunk<Values> ids,
            final ByteChunk<Values> statuses, final NaturalJoinModifiedSlotTracker modifiedSlotTracker) {
        for (int ii = 0; ii < ids.size(); ++ii) {
            if (statuses.get(ii) != KeyIdHasher.FOUND) {
                // a right key that is not on the left was never recorded
                continue;
            }
            final int id = ids.get(ii);
            final long inputKey = rowKeys.get(ii);
            final long rightRowKeyForState = rightRowKey.getUnsafe(id);
            if (rightRowKeyForState <= FIRST_DUPLICATE) {
                // remove from the duplicate row set
                final WritableRowSet duplicateRows = duplicates.get(rightRowKeyForState);
                final long duplicateSize = duplicateRows.size();
                final long originalKey = removeRightRowKeyFromDuplicates(duplicateRows, inputKey, joinType);
                Assert.eq(duplicateSize, "duplicateSize", duplicateRows.size() + 1, "duplicates.size() + 1");
                if (originalKey == inputKey) {
                    // we have a new output key for the LHS rows
                    addMain(id, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE,
                            modifiedSlotTracker);
                }
                if (duplicateRows.size() == 1) {
                    rightRowKey.set(id, getRightRowKeyFromDuplicates(duplicateRows, joinType));
                    duplicates.free(rightRowKeyForState);
                }
            } else {
                Assert.eq(rightRowKeyForState, "oldRightRow", inputKey, "inputKey");
                rightRowKey.set(id, RowSet.NULL_ROW_KEY);
                addMain(id, rightRowKeyForState, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_CHANGE,
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
                    continue;
                }
                final int id = ids.get(ii);
                final long oldRightRow = rightRowKey.getUnsafe(id);
                // a key with several right rows redirects its left rows to just one of them, so a modification of any
                // other duplicate row leaves the left rows' values unchanged
                final boolean selectedRightRowModified = !IncrementalNaturalJoinStateManager
                        .isDuplicateRightState(oldRightRow)
                        || getRightRowKeyFromDuplicates(duplicates.get(oldRightRow), joinType) == rowKeys.get(ii);
                if (selectedRightRowModified) {
                    addMain(id, oldRightRow, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_MODIFY_PROBE,
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
                    continue;
                }
                final int id = ids.get(ii);
                final long existingRightRowKey = rightRowKey.getUnsafe(id);
                final long keyToShift = rowKeys.get(ii);
                if (existingRightRowKey == keyToShift - shiftDelta) {
                    rightRowKey.set(id, keyToShift);
                    addMain(id, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT,
                            modifiedSlotTracker);
                } else if (existingRightRowKey <= FIRST_DUPLICATE) {
                    if (shiftDelta < 0) {
                        shiftOneKey(duplicates.get(existingRightRowKey), keyToShift, shiftDelta);
                    } else {
                        jc.addPendingShift(RightDuplicateRowSets.locationFromState(existingRightRowKey), keyToShift);
                    }
                    addMain(id, existingRightRowKey, NaturalJoinModifiedSlotTracker.FLAG_RIGHT_SHIFT,
                            modifiedSlotTracker);
                } else {
                    throw Assert.statementNeverExecuted("Could not find existing index for shifted right row");
                }
            }
        });
        jc.forAllPendingShiftsReversed((location, shiftedRowKey) -> {
            final WritableRowSet duplicateRows = duplicates.getAtLocation(location);
            Assert.neqNull(duplicateRows, "duplicates");
            shiftOneKey(duplicateRows, shiftedRowKey, shiftDelta);
        });
    }

    @Override
    public long getRightRowKey(final int slot) {
        final long key = rightRowKey.getUnsafe(slot);
        if (key <= FIRST_DUPLICATE) {
            return DUPLICATE_RIGHT_VALUE;
        }
        return key;
    }

    @Override
    public RowSet getLeftRowSet(final int slot) {
        return leftRowSet.getUnsafe(slot);
    }

    @Override
    public RowSet getRightRowSet(final int slot) {
        return duplicates.get(rightRowKey.getUnsafe(slot));
    }

    @Override
    public String keyString(final int slot) {
        // every key holds at least one left row, since only left keys are entered into the table
        final long firstLeftRowKey = leftRowSet.getUnsafe(slot).firstRowKey();
        Assert.neq(firstLeftRowKey, "firstLeftRowKey", RowSet.NULL_ROW_KEY);
        return extractKeyStringFromSourceTable(firstLeftRowKey);
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

    @Override
    public WritableRowRedirection buildRowRedirectionFromHashSlot(final QueryTable leftTable,
            final InitialBuildContext ibc, final JoinControl.RedirectionType redirectionType) {
        final int capacity = hasher.idCapacity();
        switch (redirectionType) {
            case Contiguous: {
                if (!leftTable.isFlat() || leftTable.getRowSet().lastRowKey() > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Left table is not flat for contiguous row redirection build!");
                }
                // we can use an array, which is perfect for a small enough flat table
                final long[] innerIndex = new long[leftTable.intSize("contiguous redirection build")];
                for (int id = 0; id < capacity; ++id) {
                    final WritableRowSet leftRows = leftRowSet.getUnsafe(id);
                    final long leftRowKey = leftRows.firstRowKey();
                    final long rightRow = getRightRowKeyFromState(leftRowKey, rightRowKey.getUnsafe(id));
                    checkExactMatch(leftRowKey, rightRow);
                    // Set unconditionally, need to populate the entire array with NULL_ROW_KEY or the RHS key
                    leftRows.forAllRowKeys(pos -> innerIndex[(int) pos] = rightRow);
                }
                return new ContiguousWritableRowRedirection(innerIndex);
            }
            case Sparse: {
                final LongSparseArraySource sparseRedirections = new LongSparseArraySource();
                for (int id = 0; id < capacity; ++id) {
                    final WritableRowSet leftRows = leftRowSet.getUnsafe(id);
                    final long leftRowKey = leftRows.firstRowKey();
                    final long rightRow = getRightRowKeyFromState(leftRowKey, rightRowKey.getUnsafe(id));
                    if (rightRow == RowSet.NULL_ROW_KEY) {
                        checkExactMatch(leftRowKey, rightRow);
                    } else {
                        leftRows.forAllRowKeys(pos -> sparseRedirections.set(pos, rightRow));
                    }
                }
                return new LongColumnSourceWritableRowRedirection(sparseRedirections);
            }
            case Hash: {
                final WritableRowRedirection rowRedirection =
                        WritableRowRedirectionLockFree.FACTORY.createRowRedirection(leftTable.intSize());
                for (int id = 0; id < capacity; ++id) {
                    final WritableRowSet leftRows = leftRowSet.getUnsafe(id);
                    final long leftRowKey = leftRows.firstRowKey();
                    final long rightRow = getRightRowKeyFromState(leftRowKey, rightRowKey.getUnsafe(id));
                    if (rightRow == RowSet.NULL_ROW_KEY) {
                        checkExactMatch(leftRowKey, rightRow);
                    } else {
                        leftRows.forAllRowKeys(pos -> rowRedirection.put(pos, rightRow));
                    }
                }
                return rowRedirection;
            }
        }
        throw new IllegalStateException("Bad redirectionType: " + redirectionType);
    }

    @Override
    public WritableRowRedirection buildRowRedirectionFromHashSlotIndexed(final QueryTable leftTable,
            final ColumnSource<RowSet> rowSetSource, final int groupingSize, final InitialBuildContext ibc,
            final JoinControl.RedirectionType redirectionType) {
        return buildRowRedirectionFromHashSlot(leftTable, ibc, redirectionType);
    }

    @Override
    public Context makeProbeContext(final ColumnSource<?>[] probeSources, final long maxSize) {
        return new KeyIdJoinContext(hasher, probeSources, maxSize);
    }

    @Override
    protected void decorateLeftSide(final RowSet leftRowSet, final ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections) {
        throw new UnsupportedOperationException("Not used with right incremental.");
    }

    @Override
    public InitialBuildContext makeInitialBuildContext(final Table leftTable) {
        // the left rows' ids are not recorded; the redirection is built from each key's left rows
        return null;
    }
}
