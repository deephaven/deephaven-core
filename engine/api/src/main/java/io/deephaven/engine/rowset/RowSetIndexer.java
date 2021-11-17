package io.deephaven.engine.rowset;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;

import java.util.Map;
import java.util.Set;

/**
 * Indexer that provides "grouping" information based on key values extracted from a {@link TupleSource}, typically
 * linked to a {@link RowSet}.
 */
public interface RowSetIndexer {

    boolean hasGrouping(ColumnSource... keyColumns);

    Map<Object, RowSet> getGrouping(TupleSource tupleSource);

    Map<Object, RowSet> getPrevGrouping(TupleSource tupleSource);

    void copyImmutableGroupings(TupleSource source, TupleSource dest);

    /**
     * Return a grouping that contains row keys that match the values in {@code keys}.
     *
     * @param keys        A set of values that {@code TupleSource} should match. For a single {@link ColumnSource}, the values
     *                    within the set are the values that we would like to find. For compound {@link TupleSource} instances, the
     *                    values are SmartKeys.
     * @param tupleSource The tuple factory for singular or compound keys
     * @return A map from keys to {@link RowSet}, for each of the {@code keys} present in this {@link RowSet row set's}
     * view of {@code tupleSource}
     */
    Map<Object, RowSet> getGroupingForKeySet(Set<Object> keys, TupleSource tupleSource);

    /**
     * Return a subset that contains row keys that match the values in {@code keys}.
     *
     * @param keys        A set of values that {@code TupleSource} should match. For a single {@link ColumnSource}, the values
     *                    within the set are the values that we would like to find. For compound {@link TupleSource} instances, the
     *                    values are SmartKeys.
     * @param tupleSource The tuple factory for singular or compound keys
     * @return A {@link WritableRowSet} with all row keys from this RowSet whose value in {@code tupleSource} was present
     * in {@code keys}
     */
    RowSet getSubSetForKeySet(Set<Object> keys, TupleSource tupleSource);

    RowSetIndexer EMPTY = new RowSetIndexer() {
        @Override
        public boolean hasGrouping(ColumnSource... keyColumns) {
            return false;
        }

        @Override
        public Map<Object, RowSet> getGrouping(TupleSource tupleSource) {

        }

        @Override
        public Map<Object, RowSet> getPrevGrouping(TupleSource tupleSource) {
            return null;
        }

        @Override
        public void copyImmutableGroupings(TupleSource source, TupleSource dest) {

        }

        @Override
        public Map<Object, RowSet> getGroupingForKeySet(Set<Object> keys, TupleSource tupleSource) {
            return null;
        }

        @Override
        public RowSet getSubSetForKeySet(Set<Object> keys, TupleSource tupleSource) {
            return null;
        }
    }
}
