package io.deephaven.engine.v2.utils;

import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.tuples.TupleSource;

import java.util.Map;
import java.util.Set;

/**
 * <p>
 * {@link RowSet} that internally tracks changes and maintains a consistent snapshot of its previous state, valid during
 * the {@link io.deephaven.engine.v2.sources.LogicalClock.State#Updating updating} phase of its associated
 * {@link io.deephaven.engine.v2.sources.LogicalClock LogicalClock}.
 * <p>
 * Also adds various methods for interacting with grouping information.
 */
public interface TrackingRowSet extends RowSet {

    long getPrev(long pos);

    long sizePrev();

    TrackingMutableRowSet getPrevIndex();

    long firstKeyPrev();

    long lastKeyPrev();

    /**
     * Returns the position in [0..(size-1)] where the key is found in the previous rowSet. If not found, then return
     * (-(position it would be) - 1), as in Array.binarySearch.
     *
     * @param key the key to search for
     * @return a position from [0..(size-1)] if the key was found. If the key was not found, then (-position - 1) as in
     *         Array.binarySearch.
     */
    long findPrev(long key);

    boolean hasGrouping(ColumnSource... keyColumns);

    Map<Object, TrackingRowSet> getGrouping(TupleSource tupleSource);

    Map<Object, TrackingRowSet> getPrevGrouping(TupleSource tupleSource);

    void copyImmutableGroupings(TupleSource source, TupleSource dest);

    /**
     * Return a grouping that contains keys that match the values in keySet.
     *
     * @param keys a set of values that keyColumns should match. For a single keyColumns, the values within the set are
     *        the values that we would like to find. For multiple keyColumns, the values are SmartKeys.
     * @param tupleSource the tuple factory for the keyColumns
     * @return an Map from keys to Indices, for each of the keys in keySet and this TrackingMutableRowSet.
     */
    Map<Object, TrackingRowSet> getGroupingForKeySet(Set<Object> keys, TupleSource tupleSource);

    /**
     * Return a subIndex that contains indices that match the values in keySet.
     *
     * @param keySet a set of values that keyColumns should match. For a single keyColumns, the values within the set
     *        are the values that we would like to find. For multiple keyColumns, the values are SmartKeys.
     * @param tupleSource the tuple factory for the keyColumn
     * @return an TrackingMutableRowSet containing only keys that match keySet.
     */
    TrackingRowSet getSubIndexForKeySet(Set<Object> keySet, TupleSource tupleSource);
}
