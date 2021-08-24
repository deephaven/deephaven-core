/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.Function;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.util.liveness.LivenessNode;
import java.util.Map.Entry;
import java.util.function.BiFunction;

import java.util.Collection;

/**
 * A map of tables.
 */
public interface TableMap extends TransformableTableMap, LivenessNode {

    /**
     * The sentinel key. See {@link #isSentinel(Object)}.
     */
    Object SENTINEL_KEY = new Object();

    /**
     * Returns an instance of the EmptyTableMap.
     */
    static TableMap emptyMap() {
        return EmptyTableMap.INSTANCE;
    }

    /**
     * Check the key passed to the function in {@link #transformTablesWithKey(BiFunction)}, to see
     * if it's the sentinel.
     *
     * @param key the object
     * @return true if the key is the sentinel
     */
    static boolean isSentinel(Object key) {
        return key == SENTINEL_KEY;
    }

    /**
     * Gets a table with a given key. Note that this causes the current
     * {@link io.deephaven.db.util.liveness.LivenessManager} (see
     * {@link io.deephaven.db.util.liveness.LivenessScopeStack}) to manage the result if non-null.
     *
     * @param key key
     * @return table associated with the key, or null if the key is not present.
     */
    Table get(Object key);

    /**
     * Gets a table with a given key, applying the specified transform before returning.
     *
     * @param key key
     * @return table associated with the key, or null if the key is not present.
     */
    Table getWithTransform(Object key, java.util.function.Function<Table, Table> transform);

    /**
     * Gets the keys.
     *
     * @return keys
     */
    Object[] getKeySet();

    /**
     * Gets the entries.
     *
     * @return the entries
     */
    Collection<Entry<Object, Table>> entrySet();

    /**
     * Gets the values.
     *
     * @return values
     */
    Collection<Table> values();

    /**
     * Number of tables in the map.
     *
     * @return number of tables in the map.
     */
    int size();

    /**
     * When creating the table map, some of the keys that we would like to be there eventually may
     * not exist. This call lets you pre-populate keys, so that at initialization time you can
     * perform the appropriate joins, etc., on empty tables that you expect to be populated in the
     * future.
     *
     * @param keys the keys to add to the map
     * @return this TableMap
     */
    TableMap populateKeys(Object... keys);

    /**
     * Add a new listener for changes to the map.
     *
     * @param listener map change listener
     */
    void addListener(Listener listener);

    /**
     * Removes a map change listener.
     * 
     * @param listener map change listener.
     */
    void removeListener(Listener listener);

    /**
     * Listen to changes in the map's keys.
     *
     * @param listener key change listener
     */
    void addKeyListener(KeyListener listener);

    /**
     * Removes a key change listener.
     *
     * @param listener key change listener to remove
     */
    void removeKeyListener(KeyListener listener);

    /**
     * Flattens all of the result tables within the tablemap.
     */
    TableMap flatten();

    /**
     * Applies a function to this tableMap.
     *
     * This is useful if you have a reference to a tableMap and want to run a series of operations
     * against the table map without each individual operation resulting in a remote method
     * invocation.
     *
     * @param function the function to run, its single argument will be this table map.
     * @param <R> the return type of function
     * @return the return value of function
     */
    <R> R apply(Function.Unary<R, TableMap> function);

    /**
     * Applies a transformation function on all tables in the TableMap, producing a new TableMap
     * which will update as new keys are added.
     *
     * @param function the function to apply to each table in this TableMap
     * @return a new TableMap where each table has had function applied
     */
    default TableMap transformTables(java.util.function.Function<Table, Table> function) {
        return transformTablesWithKey(TableMapFunctionAdapter.of(function));
    }

    /**
     * Applies a transformation function on all tables in the TableMap, producing a new TableMap
     * which will update as new keys are added.
     *
     * @param returnDefinition the table definition for the tables the function will return
     * @param function the function to apply to each table in this TableMap
     * @return a new TableMap where each table has had function applied
     */
    default TableMap transformTables(TableDefinition returnDefinition,
        java.util.function.Function<Table, Table> function) {
        return transformTablesWithKey(returnDefinition, TableMapFunctionAdapter.of(function));
    }

    /**
     * Applies a transformation function on all tables in the TableMap, producing a new TableMap
     * which will update as new keys are added.
     * <p>
     * The function may be passed a sentinel key, which can be checked with
     * {@link TableMap#isSentinel(Object)}. On the sentinel key, the function will be passed in an
     * empty table, and is expected to return an empty table of the proper definition. To avoid this
     * sentinel invocation, callers can be explicit and use
     * {@link #transformTablesWithKey(TableDefinition, BiFunction)}.
     *
     * @param function the bifunction to apply to each table in this TableMap
     * @return a new TableMap where each table has had function applied
     */
    TableMap transformTablesWithKey(java.util.function.BiFunction<Object, Table, Table> function);

    /**
     * Applies a transformation function on all tables in the TableMap, producing a new TableMap
     * which will update as new keys are added.
     *
     * @param returnDefinition the table definition for the tables the function will return
     * @param function the bifunction to apply to each table in this TableMap
     * @return a new TableMap where each table has had function applied
     */
    TableMap transformTablesWithKey(TableDefinition returnDefinition,
        java.util.function.BiFunction<Object, Table, Table> function);

    /**
     * Applies a BiFunction function on all tables in this TableMap and otherMap that have matching
     * keys, producing a new TableMap which will update as new keys are added. Only applies the
     * function to tables which exist in both maps.
     *
     * @param otherMap the other TableMap
     * @param function the function to apply to each table in this TableMap, the tables in this map
     *        are the first argument the tables in the other map are the second argument.
     * @return a new TableMap where each table has had function applied
     */
    TableMap transformTablesWithMap(TableMap otherMap,
        java.util.function.BiFunction<Table, Table, Table> function);

    /**
     * Table map change listener.
     */
    interface Listener {
        /**
         * Notification that a table has been added to the map.
         *
         * @param key key
         * @param table table
         */
        void handleTableAdded(Object key, Table table);
    }

    /**
     * Table map key change listener.
     */
    interface KeyListener {
        /**
         * Notification that a new key has been added to the map.
         *
         * @param key key
         */
        void handleKeyAdded(Object key);
    }

    @Override
    default TableMap asTableMap() {
        return this;
    }

}
