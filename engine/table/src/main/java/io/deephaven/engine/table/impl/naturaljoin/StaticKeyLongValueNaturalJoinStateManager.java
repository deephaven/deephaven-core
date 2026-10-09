//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.naturaljoin;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.exceptions.DuplicateRightKeyException;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.JoinControl;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.join.StaticKeyLongValueHasherTypedBase;
import io.deephaven.engine.table.impl.join.StaticKeyLongValueHasherTypedBase.RepeatedKey;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;

import java.util.function.LongUnaryOperator;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;

/**
 * The natural join state manager when the right table is static and the hash table is built from it; the left table is
 * static or refreshing, and each left row (or left data index group) probes the table for its right row.
 * <p>
 * The only state of a key is its right row key: the row key itself, or {@link #DUPLICATE_RIGHT_VALUE} for a key with
 * several right rows in a join that errors on duplicates. That state lives in the key's slot of a
 * {@link StaticKeyLongValueHasherTypedBase}, so a probe reads the redirection from the slot that holds the key. A
 * duplicate is an error only when a left row probes it.
 */
public class StaticKeyLongValueNaturalJoinStateManager extends StaticHashedNaturalJoinStateManager {

    private final StaticKeyLongValueHasherTypedBase hasher;

    /**
     * @param tableKeySources the key sources the hash table is built from
     * @param keySourcesForErrorMessages the key sources that error messages render keys from
     * @param tableSize the initial number of hash table slots, a power of two
     * @param maximumLoadFactor the fraction of the slots that may be occupied before the table grows
     * @param joinType the join type
     * @param addOnly whether the right table only adds rows
     */
    public StaticKeyLongValueNaturalJoinStateManager(
            final ColumnSource<?>[] tableKeySources,
            final ColumnSource<?>[] keySourcesForErrorMessages,
            final int tableSize,
            final double maximumLoadFactor,
            final NaturalJoinType joinType,
            final boolean addOnly) {
        super(keySourcesForErrorMessages, joinType, addOnly);
        hasher = StaticKeyLongValueHasherTypedBase.make(tableKeySources, tableSize, maximumLoadFactor,
                repeatedKey(joinType), DUPLICATE_RIGHT_VALUE);
    }

    private static RepeatedKey repeatedKey(final NaturalJoinType joinType) {
        switch (joinType) {
            case FIRST_MATCH:
                // the right rows are built in order, so the first is the first match
                return RepeatedKey.KEEP_FIRST;
            case LAST_MATCH:
                return RepeatedKey.KEEP_LAST;
            default:
                return RepeatedKey.MARK;
        }
    }

    /**
     * Build the hash table from the right table, mapping each right key to its right row key.
     *
     * @param rightTable the right table
     * @param rightSources the right key sources
     */
    public void buildFromRightSide(final Table rightTable, final ColumnSource<?>[] rightSources) {
        hasher.build(rightTable.getRowSet(), rightSources);
        hasher.complete();
    }

    @Override
    public void decorateLeftSide(final RowSet leftRowSet, final ColumnSource<?>[] leftSources,
            final LongArraySource leftRedirections) {
        // the probed rows are left table rows, which is the keyspace of the error message key sources
        decorateLeftSide(leftRowSet, leftSources, leftRedirections, LongUnaryOperator.identity());
    }

    /**
     * Probe the rows of a left data index table, storing one redirection per group. A duplicate right key error names
     * the key of the offending group's first left row, since {@code keySourcesForErrorMessages} are columns of the left
     * table rather than of the data index table.
     *
     * @param indexTableRowSet the data index table's row set
     * @param indexSources the data index table's key columns
     * @param indexRowSets the data index table's row set column, mapping each group to its left rows
     * @param leftRedirections receives the right row key (or {@link RowSet#NULL_ROW_KEY}) for each group, by position
     */
    public void decorateLeftSideIndexed(final RowSet indexTableRowSet, final ColumnSource<?>[] indexSources,
            final ColumnSource<RowSet> indexRowSets, final LongArraySource leftRedirections) {
        decorateLeftSide(indexTableRowSet, indexSources, leftRedirections,
                (long indexRowKey) -> indexRowSets.get(indexRowKey).firstRowKey());
    }

    private void decorateLeftSide(final RowSet probeRowSet, final ColumnSource<?>[] probeSources,
            final LongArraySource leftRedirections, final LongUnaryOperator probedRowKeyToErrorRowKey) {
        if (probeRowSet.isEmpty()) {
            return;
        }
        leftRedirections.ensureCapacity(probeRowSet.size());
        // only a join that errors on duplicates marks a key, and then only if the right table repeats one
        final boolean checkDuplicates = hasher.anyMarked();
        final long[] offset = {0};
        try (final ChunkSink.FillFromContext fillFromContext =
                leftRedirections.makeFillFromContext((int) Math.min(CHUNK_SIZE, probeRowSet.size()))) {
            hasher.probe(probeRowSet, probeSources, false, NO_RIGHT_ENTRY_VALUE, (rows, rightRowKeys) -> {
                if (checkDuplicates) {
                    checkDuplicates(rows, rightRowKeys, probedRowKeyToErrorRowKey);
                }
                final long chunkOffset = offset[0];
                final int size = rightRowKeys.size();
                // the redirections are stored by position, so each chunk fills one range
                try (final RowSequence positions =
                        RowSequenceFactory.forRange(chunkOffset, chunkOffset + size - 1)) {
                    leftRedirections.fillFromChunk(fillFromContext, rightRowKeys, positions);
                }
                offset[0] = chunkOffset + size;
            });
        }
    }

    private void checkDuplicates(final RowSequence rows, final LongChunk<Values> rightRowKeys,
            final LongUnaryOperator probedRowKeyToErrorRowKey) {
        final int size = rightRowKeys.size();
        for (int ii = 0; ii < size; ++ii) {
            if (rightRowKeys.get(ii) == DUPLICATE_RIGHT_VALUE) {
                throw new DuplicateRightKeyException("Natural Join found duplicate right key for "
                        + extractKeyStringFromSourceTable(
                                probedRowKeyToErrorRowKey.applyAsLong(rows.asRowKeyChunk().get(ii))));
            }
        }
    }

    /**
     * @param leftTable the left table
     * @param leftRedirections the right row key of each left row, by position, from {@link #decorateLeftSide}
     * @param redirectionType the kind of row redirection to build
     * @return the row redirection of the join's result
     */
    public WritableRowRedirection buildRowRedirectionFromRedirections(final QueryTable leftTable,
            final LongArraySource leftRedirections, final JoinControl.RedirectionType redirectionType) {
        return buildRowRedirection(leftTable, leftRedirections::getUnsafe, redirectionType);
    }

    /**
     * @param leftTable the left table
     * @param indexTableRowSet the left data index table's row set
     * @param leftRedirections the right row key of each data index group, by position, from
     *        {@link #decorateLeftSideIndexed}
     * @param indexRowSets the data index table's row set column, mapping each group to its left rows
     * @param redirectionType the kind of row redirection to build
     * @return the row redirection of the join's result
     */
    public WritableRowRedirection buildIndexedRowRedirectionFromRedirections(final QueryTable leftTable,
            final RowSet indexTableRowSet, final LongArraySource leftRedirections,
            final ColumnSource<RowSet> indexRowSets, final JoinControl.RedirectionType redirectionType) {
        return buildIndexedRowRedirection(leftTable, indexTableRowSet, leftRedirections::getUnsafe, indexRowSets,
                redirectionType, true);
    }
}
