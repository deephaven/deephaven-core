//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.input;

import io.deephaven.engine.exceptions.ArgumentException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A minimal interface for mutable shared tables, providing the ability to write to the table instance this is attached
 * to. InputTable instances are set on the table as an attribute.
 * <p>
 * Implementations of this interface will make their own guarantees about how atomically changes will be applied and
 * what operations they support.
 */
public interface InputTableUpdater {

    /**
     * Get the input table updater from the given {@code table} or {@code null} if it is not set.
     *
     * <p>
     * Equivalent to {@code (InputTableUpdater) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE)}.
     * 
     * @param table the table
     * @return the input table updater
     * @see Table#INPUT_TABLE_ATTRIBUTE
     */
    static InputTableUpdater from(Table table) {
        return (InputTableUpdater) table.getAttribute(Table.INPUT_TABLE_ATTRIBUTE);
    }

    /**
     * Gets the names of the key columns.
     *
     * @return a list with the names of the key columns of this input table
     */
    List<String> getKeyNames();

    /**
     * Gets the names of the value columns. By default, any column not marked as a key column is a value column.
     *
     * @return a list with the names of the value columns of this input table
     */
    default List<String> getValueNames() {
        List<String> keyNames = getKeyNames();
        return getTableDefinition().getColumnNames().stream()
                .filter(colName -> !keyNames.contains(colName))
                .collect(Collectors.toList());
    }

    /**
     * If there are client-side defined restrictions on this column; return them as a JSON string to be interpreted by
     * the client for properly displaying the edit field.
     *
     * @param columnName the column name to query
     * @return a string representing the restrictions for this column, or null if no client-side restrictions are
     *         supplied for this column
     */
    @Nullable
    default String getColumnRestrictions(final String columnName) {
        return null;
    };

    /**
     * Get the underlying Table definition (which includes the names and types of all of the columns).
     *
     * @return the TableDefinition for our user-visible table
     */
    TableDefinition getTableDefinition();

    /**
     * Helper to check if a table is compatible with this table, so that it could be added as contents.
     *
     * @param tableToApply the table to check if it can used to add or modify this input table
     * @throws TableDefinition.IncompatibleTableDefinitionException if the definitions are not compatible
     */
    default void validateAddOrModify(final Table tableToApply) {
        getTableDefinition().checkMutualCompatibility(tableToApply.getDefinition());
    }

    /**
     * Validates that the given table definition is suitable to be passed to {@link #delete(Table)}.
     *
     * @param tableToDelete The definition of the table to delete
     * @throws UnsupportedOperationException If this table does not support deletes
     * @throws ArgumentException If the given definition isn't compatible to be used to delete
     */
    default void validateDelete(Table tableToDelete) {
        final TableDefinition keyDefinition = tableToDelete.getDefinition();
        final TableDefinition thisDefinition = getTableDefinition();
        final StringBuilder error = new StringBuilder();
        final List<String> keyNames = getKeyNames();
        for (String keyColumn : keyNames) {
            final ColumnDefinition<?> colDef = keyDefinition.getColumn(keyColumn);
            final ColumnDefinition<?> thisColDef = thisDefinition.getColumn(keyColumn);
            if (colDef == null) {
                error.append("Key Column \"").append(keyColumn).append("\" does not exist.\n");
            } else if (!colDef.isCompatible(thisColDef)) {
                error.append("Key Column \"").append(keyColumn).append("\" is not compatible.\n");
            }
        }
        final List<String> extraKeys = keyDefinition.getColumnNames().stream().filter(kd -> !keyNames.contains(kd))
                .collect(Collectors.toList());
        if (!extraKeys.isEmpty()) {
            error.append("Unknown key columns: ").append(extraKeys);
        }
        if (error.length() > 0) {
            throw new ArgumentException("Invalid Key Table Definition: " + error);
        }
    }

    /**
     * Write {@code newData} to this table. Added rows with keys that match existing rows will instead replace those
     * rows, if supported.
     *
     * <p>
     * This method will block until the add is "completed", where the definition of "completed" is implementation
     * dependenent.
     *
     * <p>
     * For implementations where "completed" means "visible in the next update graph cycle", this method is not suitable
     * for use from a {@link io.deephaven.engine.table.TableListener table listener} or any other
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Notification notification}-dispatched callback
     * dispatched by this InputTable's {@link io.deephaven.engine.updategraph.UpdateGraph update graph}. It may be
     * suitable to delete from another update graph if doing so does not introduce any cycles.
     *
     * @param newData The data to write to this table
     * @throws IOException If there is an error writing the data
     */
    void add(Table newData) throws IOException;

    /**
     * Write {@code newData} to this table. Added rows with keys that match existing rows replace those rows, if
     * supported.
     *
     * <p>
     * The callback to {@code listener} will happen when the add has "completed", where the definition of "completed" is
     * implementation dependenent. It's possible that the callback happens immediately on the same thread.
     *
     * <p>
     * This method will <em>not</em> block, and can be safely used from a {@link io.deephaven.engine.table.TableListener
     * table listener} or any other {@link io.deephaven.engine.updategraph.NotificationQueue.Notification
     * notification}-dispatched callback as long as {@code table} is already
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Dependency#satisfied(long) satisfied} on the current
     * cycle.
     *
     * @param newData The data to write to this table
     * @param listener The listener for asynchronous results
     */
    void addAsync(Table newData, InputTableStatusListener listener);

    /**
     * Delete the keys contained in {@code table} from this input table.
     *
     * <p>
     * This method will block until the delete is "completed", where the definition of "completed" is implementation
     * dependenent.
     *
     * <p>
     * For implementations where "completed" means "visible in the next update graph cycle", this method is not suitable
     * for use from a {@link io.deephaven.engine.table.TableListener table listener} or any other
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Notification notification}-dispatched callback
     * dispatched by this InputTable's {@link io.deephaven.engine.updategraph.UpdateGraph update graph}. It may be
     * suitable to delete from another update graph if doing so does not introduce any cycles.
     *
     * @param table The rows to delete
     * @throws IOException If a problem occurred while deleting the rows.
     * @throws UnsupportedOperationException If this table does not support deletes
     */
    default void delete(Table table) throws IOException {
        throw new UnsupportedOperationException("Table does not support deletes");
    }

    /**
     * Delete the keys contained in table from this input table.
     *
     * <p>
     * The callback to {@code listener} will happen when the delete has "completed", where the definition of "completed"
     * is implementation dependenent. It's possible that the callback happens immediately on the same thread.
     * 
     * <p>
     * This method will <em>not</em> block, and can be safely used from a {@link io.deephaven.engine.table.TableListener
     * table listener} or any other {@link io.deephaven.engine.updategraph.NotificationQueue.Notification
     * notification}-dispatched callback as long as {@code table} is already
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Dependency#satisfied(long) satisfied} on the current
     * cycle.
     *
     * @param table Table containing the rows to delete
     * @throws UnsupportedOperationException If this table does not support deletes
     */
    default void deleteAsync(Table table, InputTableStatusListener listener) {
        throw new UnsupportedOperationException("Table does not support deletes");
    }

    /**
     * Returns true if the specified column is a key.
     *
     * @param columnName the column to interrogate
     * @return true if columnName is a key column, false otherwise
     */
    default boolean isKey(String columnName) {
        return getKeyNames().contains(columnName);
    }

    /**
     * Returns true if the specified column exists in this InputTable.
     *
     * @param columnName the column to interrogate
     * @return true if columnName exists in this InputTable
     */
    default boolean hasColumn(String columnName) {
        return getTableDefinition().getColumnNames().contains(columnName);
    }
}
