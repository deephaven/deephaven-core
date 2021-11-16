package io.deephaven.engine.rowset;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.updategraph.LogicalClock;

import java.util.Map;
import java.util.Set;

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

    boolean hasGrouping(ColumnSource... keyColumns);

    Map<Object, RowSet> getGrouping(TupleSource tupleSource);

    Map<Object, RowSet> getPrevGrouping(TupleSource tupleSource);

    void copyImmutableGroupings(TupleSource source, TupleSource dest);

    /**
     * Return a grouping that contains row keys that match the values in {@code keys}.
     *
     * @param keys A set of values that {@code TupleSource} should match. For a single {@link ColumnSource}, the values
     *        within the set are the values that we would like to find. For compound {@link TupleSource} instances, the
     *        values are SmartKeys.
     * @param tupleSource The tuple factory for singular or compound keys
     * @return A map from keys to {@link RowSet}, for each of the {@code keys} present in this {@link RowSet row set's}
     *         view of {@code tupleSource}
     */
    Map<Object, RowSet> getGroupingForKeySet(Set<Object> keys, TupleSource tupleSource);

    /**
     * Return a subset that contains row keys that match the values in keySet.
     *
     * @param keySet a set of values that keyColumns should match. For a single keyColumns, the values within the set
     *        are the values that we would like to find. For multiple keyColumns, the values are SmartKeys.
     * @param tupleSource the tuple factory for the keyColumn
     * @return an WritableRowSet containing only keys that match keySet.
     */
    /**
     * Return a subset that contains row keys that match the values in {@code keys}.
     *
     * @param keys A set of values that {@code TupleSource} should match. For a single {@link ColumnSource}, the values
     *        within the set are the values that we would like to find. For compound {@link TupleSource} instances, the
     *        values are SmartKeys.
     * @param tupleSource The tuple factory for singular or compound keys
     * @return A {@link WritableRowSet} with all row keys from this RowSet whose value in {@code tupleSource} was present
     *         in {@code keys}
     */
    RowSet getSubSetForKeySet(Set<Object> keys, TupleSource tupleSource);

    @Override
    default TrackingWritableRowSet writableCast() {
        return (TrackingWritableRowSet) this;
    }
}
