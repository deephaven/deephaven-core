package io.deephaven.engine.testutil.generator;

import io.deephaven.engine.rowset.RowSet;

import java.util.Random;
import java.util.TreeMap;

public interface Generator<T, U> {
    TreeMap<Long, U> populateMap(TreeMap<Long, U> values, RowSet toAdd, Random random);

    /**
     * Called after a key has been removed from the map.
     *
     * @param key the row key that was removed
     * @param removed the value that was removed
     */
    default void onRemove(long key, U removed) {}

    default void onMove(long oldKey, long newKey, U moved) {}

    Class<U> getType();

    Class<T> getColumnType();
}
