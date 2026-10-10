//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.exceptions.DuplicateRightKeyException;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.join.KeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;

import java.util.function.LongUnaryOperator;

/**
 * The natural join state manager when the right table is static, and the left table is static or refreshing.
 * <p>
 * Each distinct key has an id from a {@link KeyIdHasherTypedBase}, and its right state is kept by id: the right row
 * key, {@link #NO_RIGHT_STATE_VALUE} for a key with no right row, or {@link #DUPLICATE_RIGHT_STATE} for a key with
 * several. A build from the left records each left row's id rather than a hash slot, so the table may grow while it is
 * built.
 */
public class StaticKeyIdNaturalJoinStateManager extends StaticHashedNaturalJoinStateManager {

    public static final long NO_RIGHT_STATE_VALUE = RowSet.NULL_ROW_KEY;
    // errorOnDuplicates recognizes duplicate keys by comparing the stored state against DUPLICATE_RIGHT_VALUE
    public static final long DUPLICATE_RIGHT_STATE = DUPLICATE_RIGHT_VALUE;

    private final KeyIdHasherTypedBase hasher;

    // the right state of each key, by id; one flat array, since the probe of each left row reads it
    private final ImmutableLongArraySource rightState = new ImmutableLongArraySource();

    /**
     * @param tableKeySources the key sources the hash table is built from
     * @param keySourcesForErrorMessages the key sources that error messages render keys from
     * @param tableSize the initial number of hash table slots, a power of two
     * @param maximumLoadFactor the fraction of the slots that may be occupied before the table grows
     * @param joinType the join type
     * @param addOnly whether the right table only adds rows
     */
    public StaticKeyIdNaturalJoinStateManager(
            final ColumnSource<?>[] tableKeySources,
            final ColumnSource<?>[] keySourcesForErrorMessages,
            final int tableSize,
            final double maximumLoadFactor,
            final NaturalJoinType joinType,
            final boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);
        hasher = KeyIdHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor);
    }

    private void ensureIdCapacity() {
        final int capacity = hasher.idCapacity();
        final long[] states = rightState.getArray();
        final int length = states == null ? 0 : states.length;
        if (capacity > length) {
            // grow the flat array geometrically, keeping the states of the ids already handed out
            final long[] grown = new long[Math.max(capacity, 2 * length)];
            if (length > 0) {
                System.arraycopy(states, 0, grown, 0, length);
            }
            rightState.setArray(grown);
        }
    }

    @Override
    public void buildFromLeftSide(final Table leftTable, final ColumnSource<?>[] leftSources,
            final IntegerArraySource leftHashSlots) {
        if (leftTable.isEmpty()) {
            return;
        }
        leftHashSlots.ensureCapacity(leftTable.size());
        final long[] offset = {0};
        hasher.build(leftTable.getRowSet(), leftSources, (rows, ids, statuses) -> {
            ensureIdCapacity();
            final long chunkOffset = offset[0];
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int id = ids.get(ii);
                if (statuses.get(ii) == KeyIdHasher.ADDED) {
                    rightState.set(id, NO_RIGHT_STATE_VALUE);
                }
                leftHashSlots.set(chunkOffset + ii, id);
            }
            offset[0] = chunkOffset + size;
        });
    }

    @Override
    public void buildFromRightSide(final Table rightTable, final ColumnSource<?>[] rightSources) {
        if (rightTable.isEmpty()) {
            return;
        }
        hasher.build(rightTable.getRowSet(), rightSources, (rows, ids, statuses) -> {
            ensureIdCapacity();
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int id = ids.get(ii);
                if (statuses.get(ii) == KeyIdHasher.ADDED) {
                    rightState.set(id, rowKeys.get(ii));
                } else if (joinType == NaturalJoinType.LAST_MATCH) {
                    // we are processing sequentially so this is the latest
                    rightState.set(id, rowKeys.get(ii));
                } else if (joinType != NaturalJoinType.FIRST_MATCH) {
                    // a first match join already has its match
                    rightState.set(id, DUPLICATE_RIGHT_STATE);
                }
            }
        });
    }

    @Override
    public void decorateLeftSide(final RowSet leftRowSet, final ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections) {
        // the probed rows are left table rows, which is the keyspace of the error message key sources
        decorateLeftSide(leftRowSet, leftSources, leftRedirections, LongUnaryOperator.identity());
    }

    @Override
    public void decorateLeftSideIndexed(final RowSet indexTableRowSet, final ColumnSource<?>[] indexSources,
            final ColumnSource<RowSet> indexRowSets, final LongArraySource leftRedirections) {
        // the probed rows are data index table rows, so a duplicate key error is rendered from the first left row of
        // the offending group
        decorateLeftSide(indexTableRowSet, indexSources, leftRedirections,
                (long indexRowKey) -> indexRowSets.get(indexRowKey).firstRowKey());
    }

    private void decorateLeftSide(final RowSet probeRowSet, final ColumnSource<?>[] probeSources,
            final LongArraySource leftRedirections, final LongUnaryOperator probedRowKeyToErrorRowKey) {
        if (probeRowSet.isEmpty()) {
            return;
        }
        leftRedirections.ensureCapacity(probeRowSet.size());
        final long[] offset = {0};
        hasher.probe(probeRowSet, probeSources, false, (rows, ids, statuses) -> {
            final long chunkOffset = offset[0];
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int id = ids.get(ii);
                final long rightRowKey;
                if (id != KeyIdHasher.NULL_ID) {
                    rightRowKey = rightState.getUnsafe(id);
                    if (rightRowKey == DUPLICATE_RIGHT_STATE) {
                        throw new DuplicateRightKeyException("Natural Join found duplicate right key for "
                                + extractKeyStringFromSourceTable(
                                        probedRowKeyToErrorRowKey.applyAsLong(rows.asRowKeyChunk().get(ii))));
                    }
                } else {
                    rightRowKey = RowSet.NULL_ROW_KEY;
                }
                leftRedirections.set(chunkOffset + ii, rightRowKey);
            }
            offset[0] = chunkOffset + size;
        });
    }

    @Override
    public void decorateWithRightSide(final Table rightTable, final ColumnSource<?>[] rightSources) {
        if (rightTable.isEmpty()) {
            return;
        }
        hasher.probe(rightTable.getRowSet(), rightSources, false, (rows, ids, statuses) -> {
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                if (statuses.get(ii) != KeyIdHasher.FOUND) {
                    // a right key that is not on the left matches nothing
                    continue;
                }
                final int id = ids.get(ii);
                if (rightState.getUnsafe(id) == NO_RIGHT_STATE_VALUE) {
                    rightState.set(id, rowKeys.get(ii));
                } else if (joinType == NaturalJoinType.LAST_MATCH) {
                    // we are processing sequentially so this is the latest
                    rightState.set(id, rowKeys.get(ii));
                } else if (joinType != NaturalJoinType.FIRST_MATCH) {
                    // a first match join already has its match
                    rightState.set(id, DUPLICATE_RIGHT_STATE);
                    throw new DuplicateRightRowDecorationException(id);
                }
            }
        });
    }

    @Override
    public WritableRowRedirection buildRowRedirectionFromHashSlot(final QueryTable leftTable,
            final IntegerArraySource leftHashSlots, final JoinControl.RedirectionType redirectionType) {
        return buildRowRedirection(leftTable, position -> rightState.getUnsafe(leftHashSlots.getUnsafe(position)),
                redirectionType);
    }

    @Override
    public WritableRowRedirection buildRowRedirectionFromRedirections(final QueryTable leftTable,
            final LongArraySource leftRedirections, final JoinControl.RedirectionType redirectionType) {
        return buildRowRedirection(leftTable, leftRedirections::getUnsafe, redirectionType);
    }

    @Override
    public WritableRowRedirection buildIndexedRowRedirectionFromRedirections(
            final QueryTable leftTable,
            final RowSet indexTableRowSet,
            final LongArraySource leftRedirections,
            final ColumnSource<RowSet> indexRowSets,
            final JoinControl.RedirectionType redirectionType) {
        return buildIndexedRowRedirection(leftTable, indexTableRowSet,
                leftRedirections::getUnsafe, indexRowSets, redirectionType, true);
    }

    @Override
    public WritableRowRedirection buildIndexedRowRedirectionFromHashSlots(
            final QueryTable leftTable,
            final RowSet indexTableRowSet,
            final IntegerArraySource leftHashSlots,
            final ColumnSource<RowSet> indexRowSets,
            final JoinControl.RedirectionType redirectionType) {
        return buildIndexedRowRedirection(leftTable, indexTableRowSet,
                (long groupPosition) -> rightState.getUnsafe(leftHashSlots.getUnsafe(groupPosition)), indexRowSets,
                redirectionType, false);
    }

    @Override
    public void errorOnDuplicatesIndexed(final IntegerArraySource leftHashSlots, final RowSet indexTableRowSet) {
        // the key sources for error messages are columns of the data index table, so the error row key is the index
        // table row key for the offending group
        errorOnDuplicates(indexTableRowSet.size(),
                (long groupPosition) -> rightState.getUnsafe(leftHashSlots.getUnsafe(groupPosition)),
                indexTableRowSet::get);
    }

    @Override
    public void errorOnDuplicatesSingle(final IntegerArraySource leftHashSlots, final long size, final RowSet rowSet) {
        errorOnDuplicates(size, (long position) -> rightState.getUnsafe(leftHashSlots.getUnsafe(position)),
                rowSet::get);
    }
}
