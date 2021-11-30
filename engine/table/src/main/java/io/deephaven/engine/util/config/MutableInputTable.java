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
 *
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
     * @param tableToDelete the definition of the table to delete
     * @throws UnsupportedOperationException if this table does not support deletes
     * @throws ArgumentException if the given definition isn't compatible to be used to delete
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
     * Write newData to this table. This method will block until the rows are added. Added rows with keys that match
     * existing rows will instead replace those rows, if supported.
     *
     * @param newData the data to write to this table
     *
     * @throws IOException if there is an error writing the data
     */
    void add(Table newData) throws IOException;

    /**
     * Delete the keys contained in the parameter table from this input table. This method will block until rows are
     * deleted.
     *
     * @param table The rows to delete.
     * @throws IOException If a problem occurred while deleting the rows.
     * @throws UnsupportedOperationException if this table does not support deletes
     */
    default void delete(Table table) throws IOException {
        delete(table, table.getRowSet());
    }

    /**
     * Delete the keys contained in the parameter table from this input table. This method will block until rows are
     * deleted.
     *
     * @param table The rows to delete.
     * @throws IOException if a problem occurred while deleting the rows
     * @throws UnsupportedOperationException if this table does not support deletes
     */
    default void delete(Table table, TrackingRowSet rowSet) throws IOException {
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
     *
     * @return true if columnName is a key column, false otherwise
     */
    default boolean isKey(String columnName) {
        return getKeyNames().contains(columnName);
    }

    /**
     * Returns true if the specified column exists in this MutableInputTable.
     *
     * @param columnName the column to interrogate
     *
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
