package io.deephaven.engine.rowset;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.updategraph.LogicalClock;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * <p>
 * {@link RowSet} that internally tracks changes and maintains a consistent snapshot of its previous state, valid during
 * the {@link LogicalClock.State#Updating updating} phase of its associated
 * {@link LogicalClock LogicalClock}.
 * <p>
 * Also adds various methods for interacting with grouping information.
 */
public interface TrackingRowSet extends RowSet {

    long sizePrev();

    WritableRowSet getPrevRowSet();

    long getPrev(long pos);

    long firstRowKeyPrev();

    long lastRowKeyPrev();

    /**
     * Returns the position in [0..(size-1)] where the row key is found in the previous rowSet. If not found, then
     * return {@code (-(position it would be) - 1)}, as in Array.binarySearch.
     *
     * @param rowKey The row key to search for
     * @return A position from [0..(size-1)] if the row key was found. If the row key was not found, then
     *         {@code (-position - 1)} as in Array.binarySearch
     */
    long findPrev(long rowKey);

    /**
     * @return A {@link RowSetIndexer} associated with this {@link RowSet}
     */
    RowSetIndexer indexer(Function<RowSet, RowSetIndexer> indexerFactory);

    default boolean hasGrouping(ColumnSource... keyColumns) {
        return indexer().hasGrouping(keyColumns);
    }

    default Map<Object, RowSet> getGrouping(TupleSource tupleSource) {
        return indexer().getGrouping(tupleSource);
    }

    default Map<Object, RowSet> getPrevGrouping(TupleSource tupleSource) {
        return indexer().getPrevGrouping(tupleSource);
    }

    default void copyImmutableGroupings(TupleSource source, TupleSource dest) {
        indexer().copyImmutableGroupings(source, dest);
    }

    default Map<Object, RowSet> getGroupingForKeySet(Set<Object> keys, TupleSource tupleSource) {
        return getGroupingForKeySet(keys, tupleSource);
    }

    default RowSet getSubSetForKeySet(Set<Object> keys, TupleSource tupleSource) {
        return getSubSetForKeySet(keys, tupleSource);
    }

    @Override
    default TrackingWritableRowSet writableCast() {
        return (TrackingWritableRowSet) this;
    }
}
