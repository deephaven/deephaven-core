/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util.config;

import io.deephaven.engine.exceptions.ArgumentException;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.rowset.TrackingRowSet;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A minimal interface for mutable shared tables, providing the ability to write to the table instance this is attached
 * to. MutableInputTable instances are set on the table as an attribute.
 * <p>
 * Implementations of this interface will make their own guarantees about how atomically changes will be applied and
 * what operations they support.
 */
public interface MutableInputTable extends InputTableRowSetter, InputTableEnumGetter {

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
            throw new ArgumentException("Invalid Key Table Definition: " + error.toString());
        }
    }

    /**
     * Write {@code newData} to this table. Added rows with keys that match existing rows will instead replace those
     * rows, if supported.
     * <p>
     * This method will block until the rows are added. As a result, this method is not suitable for use from a
     * {@link io.deephaven.engine.table.TableListener table listener} or any other
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Notification notification}-dispatched callback
     * dispatched by this MutableInputTable's {@link io.deephaven.engine.updategraph.UpdateGraph update graph}. It may
     * be suitable to delete from another update graph if doing so does not introduce any cycles.
     *
     * @param newData The data to write to this table
     * @throws IOException If there is an error writing the data
     */
    void add(Table newData) throws IOException;

    /**
     * Write {@code newData} to this table. Added rows with keys that match existing rows will instead replace those
     * rows, if supported and {@code allowEdits == true}.
     * <p>
     * This method will <em>not</em> block, and can be safely used from a {@link io.deephaven.engine.table.TableListener
     * table listener} or any other {@link io.deephaven.engine.updategraph.NotificationQueue.Notification
     * notification}-dispatched callback as long as {@code table} is already
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Dependency#satisfied(long) satisfied} on the current
     * cycle.
     *
     * @param newData The data to write to this table
     * @param allowEdits Whether added rows with keys that match existing rows will instead replace those rows, or
     *        result in an error
     * @param listener The listener for asynchronous results
     */
    void addAsync(Table newData, boolean allowEdits, InputTableStatusListener listener);

    /**
     * Delete the keys contained in {@code table} from this input table.
     * <p>
     * This method will block until the rows are deleted. As a result, this method is not suitable for use from a
     * {@link io.deephaven.engine.table.TableListener table listener} or any other
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Notification notification}-dispatched callback
     * dispatched by this MutableInputTable's {@link io.deephaven.engine.updategraph.UpdateGraph update graph}. It may
     * be suitable to delete from another update graph if doing so does not introduce any cycles.
     *
     * @param table The rows to delete
     * @throws IOException If a problem occurred while deleting the rows.
     * @throws UnsupportedOperationException If this table does not support deletes
     */
    default void delete(Table table) throws IOException {
        delete(table, table.getRowSet());
    }

    /**
     * Delete the keys contained in {@code table.subTable(rowSet)} from this input table.
     * <p>
     * This method will block until the rows are deleted. As a result, this method is not suitable for use from a
     * {@link io.deephaven.engine.table.TableListener table listener} or any other
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Notification notification}-dispatched callback
     * dispatched by this MutableInputTable's {@link io.deephaven.engine.updategraph.UpdateGraph update graph}. It may
     * be suitable to delete from another update graph if doing so does not introduce any cycles.
     *
     * @param table Table containing the rows to delete
     * @param rowSet The rows to delete
     * @throws IOException If a problem occurred while deleting the rows
     * @throws UnsupportedOperationException If this table does not support deletes
     */
    default void delete(Table table, TrackingRowSet rowSet) throws IOException {
        throw new UnsupportedOperationException("Table does not support deletes");
    }

    /**
     * Delete the keys contained in {@code table.subTable(rowSet)} from this input table.
     * <p>
     * This method will <em>not</em> block, and can be safely used from a {@link io.deephaven.engine.table.TableListener
     * table listener} or any other {@link io.deephaven.engine.updategraph.NotificationQueue.Notification
     * notification}-dispatched callback as long as {@code table} is already
     * {@link io.deephaven.engine.updategraph.NotificationQueue.Dependency#satisfied(long) satisfied} on the current
     * cycle.
     *
     * @param table Table containing the rows to delete
     * @param rowSet The rows to delete
     * @throws UnsupportedOperationException If this table does not support deletes
     */
    default void deleteAsync(Table table, TrackingRowSet rowSet, InputTableStatusListener listener) {
        throw new UnsupportedOperationException("Table does not support deletes");
    }

    /**
     * Return a user-readable description of this MutableInputTable.
     *
     * @return a description of this input table
     */
    String getDescription();

    /**
     * Returns a Deephaven table that contains the current data for this MutableInputTable.
     *
     * @return the current data in this MutableInputTable.
     */
    Table getTable();

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
     * Returns true if the specified column exists in this MutableInputTable.
     *
     * @param columnName the column to interrogate
     * @return true if columnName exists in this MutableInputTable
     */
    default boolean hasColumn(String columnName) {
        return getTableDefinition().getColumnNames().contains(columnName);
    }

    /**
     * Queries whether this MutableInputTable is editable in the current context.
     *
     * @return true if this MutableInputTable may be edited, false otherwise TODO (deephaven/deephaven-core/issues/255):
     *         Add AuthContext and whatever else is appropriate
     */
    boolean canEdit();
}
