//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.join.KeyIdHasher;
import io.deephaven.engine.table.impl.join.StaticKeyIdHasherTypedBase;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;

/**
 * The natural join state manager when both tables are static and the hash table is built from the left table, or from
 * its data index; the right table then decorates the left keys. A table built from the right is managed by
 * {@link StaticKeyLongValueNaturalJoinStateManager}.
 * <p>
 * Each distinct key has an id from a {@link StaticKeyIdHasherTypedBase}, and its right state is kept by id: the right
 * row key, {@link #NO_RIGHT_STATE_VALUE} for a key with no right row, or {@link #DUPLICATE_RIGHT_STATE} for a key with
 * several. The build records each left row's id rather than a hash slot, so the table may grow while it is built.
 */
public class StaticKeyIdNaturalJoinStateManager extends StaticHashedNaturalJoinStateManager {

    public static final long NO_RIGHT_STATE_VALUE = RowSet.NULL_ROW_KEY;
    // errorOnDuplicates recognizes duplicate keys by comparing the stored state against DUPLICATE_RIGHT_VALUE
    public static final long DUPLICATE_RIGHT_STATE = DUPLICATE_RIGHT_VALUE;

    private final StaticKeyIdHasherTypedBase hasher;

    // the right state of each key, by id; one flat array, since building the redirection reads it for each left row
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
        hasher = StaticKeyIdHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor);
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

    /**
     * Build the hash table from the left table, or from its data index, recording each row's key id by position.
     *
     * @param leftTable the left table, or its data index table
     * @param leftSources the key sources of {@code leftTable}
     * @param leftHashSlots receives the key id of each row of {@code leftTable}, by position
     */
    public void buildFromLeftSide(final Table leftTable, final ColumnSource<?>[] leftSources,
            final IntegerArraySource leftHashSlots) {
        if (leftTable.isEmpty()) {
            hasher.complete();
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
        hasher.complete();
    }

    @Override
    public void decorateLeftSide(final RowSet leftRowSet, final ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections) {
        // the left rows are built into the table rather than probed; a table built from the right probes them
        throw new UnsupportedOperationException("A natural join state manager built from the left is not probed");
    }

    /**
     * Give each left key its right row. A key with several right rows in a join that errors on duplicates throws
     * {@link DuplicateRightRowDecorationException}, after which {@link #errorOnDuplicatesSingle} or
     * {@link #errorOnDuplicatesIndexed} reports the left key.
     *
     * @param rightTable the right table
     * @param rightSources the right key sources
     */
    public void decorateWithRightSide(final Table rightTable, final ColumnSource<?>[] rightSources) {
        if (rightTable.isEmpty()) {
            return;
        }
        hasher.probe(rightTable.getRowSet(), rightSources, false, (rows, ids) -> {
            final LongChunk<OrderedRowKeys> rowKeys = rows.asRowKeyChunk();
            final int size = ids.size();
            for (int ii = 0; ii < size; ++ii) {
                final int id = ids.get(ii);
                if (id == KeyIdHasher.NULL_ID) {
                    // a right key that is not on the left matches nothing
                    continue;
                }
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

    public WritableRowRedirection buildRowRedirectionFromHashSlot(final QueryTable leftTable,
            final IntegerArraySource leftHashSlots, final JoinControl.RedirectionType redirectionType) {
        return buildRowRedirection(leftTable, position -> rightState.getUnsafe(leftHashSlots.getUnsafe(position)),
                redirectionType);
    }

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

    /**
     * Throw the duplicate right key error after a build from a left data index; the error key sources are columns of
     * the data index table.
     *
     * @param leftHashSlots the key id of each data index table row, by position
     * @param indexTableRowSet the data index table's row set
     */
    public void errorOnDuplicatesIndexed(final IntegerArraySource leftHashSlots, final RowSet indexTableRowSet) {
        // the key sources for error messages are columns of the data index table, so the error row key is the index
        // table row key for the offending group
        errorOnDuplicates(indexTableRowSet.size(),
                (long groupPosition) -> rightState.getUnsafe(leftHashSlots.getUnsafe(groupPosition)),
                indexTableRowSet::get);
    }

    /**
     * Throw the duplicate right key error after a build from the left table; the error key sources are columns of the
     * left table.
     *
     * @param leftHashSlots the key id of each left row, by position
     * @param size the number of left rows
     * @param rowSet the left table's row set
     */
    public void errorOnDuplicatesSingle(final IntegerArraySource leftHashSlots, final long size, final RowSet rowSet) {
        errorOnDuplicates(size, (long position) -> rightState.getUnsafe(leftHashSlots.getUnsafe(position)),
                rowSet::get);
    }
}
