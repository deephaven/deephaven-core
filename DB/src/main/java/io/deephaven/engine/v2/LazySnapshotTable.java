/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.v2.utils.UpdatePerformanceTracker;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * This interface represents a table that will not update itself on the run cycle, but instead run itself when the run
 * for snapshot is called.
 */
public interface LazySnapshotTable extends Table {
    void refreshForSnapshot();

    /**
     * Initiate update delivery to this table's listeners. Will notify direct listeners before completing, and enqueue
     * notifications for all other listeners.
     *
     * @param added rowSet values added to the table
     * @param removed rowSet values removed from the table
     * @param modified rowSet values modified in the table.
     */
    default void notifyListeners(RowSet added, RowSet removed, RowSet modified) {
        notifyListeners(new Listener.Update(added, removed, modified, RowSetShiftData.EMPTY,
                modified.isEmpty() ? ModifiedColumnSet.EMPTY : ModifiedColumnSet.ALL));
    }

    /**
     * Initiate update delivery to this table's listeners. Will notify direct listeners before completing, and enqueue
     * notifications for all other listeners.
     *
     * @param update the set of table changes to propagate The caller gives this update object away; the invocation of
     *        {@code notifyListeners} takes ownership, and will call {@code release} on it once it is not used anymore;
     *        callers should pass a {@code copy} for updates they intend to further use.
     */
    void notifyListeners(Listener.Update update);

    /**
     * Initiate failure delivery to this table's listeners. Will notify direct listeners before completing, and enqueue
     * notifications for all other listeners.
     *
     * @param e error
     * @param sourceEntry performance tracking
     */
    void notifyListenersOnError(Throwable e, @Nullable UpdatePerformanceTracker.Entry sourceEntry);

    /**
     * Retrieve the {@link ModifiedColumnSet} that will be used when propagating updates from this table.
     *
     * @param columnNames the columns that should belong to the resulting set.
     * @return the resulting ModifiedColumnSet for the given columnNames
     */
    default ModifiedColumnSet newModifiedColumnSet(String... columnNames) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the table used to construct columnSets. It is an error if {@code columnNames} and {@code columnSets}
     * are not the same length. The transformer will mark {@code columnSets[i]} as dirty if the column represented by
     * {@code columnNames[i]} is dirty.
     *
     * @param columnNames the source columns
     * @param columnSets the destination columns in the convenient ModifiedColumnSet form
     * @return a transformer that knows the dirty details
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(String[] columnNames,
            ModifiedColumnSet[] columnSets) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the provided resultTable.
     *
     * @param resultTable the destination table
     * @param columnNames the columns that map one-to-one with the result table
     * @return a transformer that passes dirty details via an identity mapping
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(Table resultTable,
            String... columnNames) {
        final ModifiedColumnSet[] columnSets = new ModifiedColumnSet[columnNames.length];
        for (int i = 0; i < columnNames.length; ++i) {
            columnSets[i] = resultTable.newModifiedColumnSet(columnNames[i]);
        }
        return newModifiedColumnSetTransformer(columnNames, columnSets);
    }

    /**
     * Create a {@link ModifiedColumnSet.Transformer} that can be used to propagate dirty columns from this table to
     * listeners of the provided resultTable.
     *
     * @param resultTable the destination table
     * @param matchPairs the columns that map one-to-one with the result table
     * @return a transformer that passes dirty details via an identity mapping
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetTransformer(Table resultTable,
            MatchPair... matchPairs) {
        final ModifiedColumnSet[] columnSets = new ModifiedColumnSet[matchPairs.length];
        for (int ii = 0; ii < matchPairs.length; ++ii) {
            columnSets[ii] = resultTable.newModifiedColumnSet(matchPairs[ii].left());
        }
        return newModifiedColumnSetTransformer(MatchPair.getRightColumns(matchPairs), columnSets);
    }

    /**
     * Create a transformer that uses an identity mapping from one ColumnSourceMap to another. The two CSMs must have
     * equivalent column names and column ordering.
     *
     * @param newColumns the column source map for result table
     * @return a simple Transformer that makes a cheap, but CSM compatible copy
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetIdentityTransformer(
            final Map<String, ColumnSource<?>> newColumns) {
        throw new UnsupportedOperationException();
    }

    /**
     * Create a transformer that uses an identity mapping from one Table another. The two tables must have equivalent
     * column names and column ordering.
     *
     * @param other the result table
     * @return a simple Transformer that makes a cheap, but CSM compatible copy
     */
    default ModifiedColumnSet.Transformer newModifiedColumnSetIdentityTransformer(Table other) {
        throw new UnsupportedOperationException();
    }
}
