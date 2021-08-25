package io.deephaven.db.util.config;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.web.shared.data.InputTableDefinition;

import java.io.IOException;
import java.util.Arrays;

/**
 * A minimal interface for mutable tables that can be changed over the OpenAPI.
 */
public interface MutableInputTable extends InputTableRowSetter, InputTableEnumGetter {
    /**
     * Get the key and value columns names for this table.
     *
     * @return the InputTableDefinition.
     */
    InputTableDefinition getDefinition();

    /**
     * Get the names of the key columns
     *
     * @return an array with the names of our key columns
     */
    default String[] getKeyNames() {
        return getDefinition().getKeys();
    }

    /**
     * Get the underlying Table definition (which includes the names and types of all of the
     * columns).
     *
     * @return the TableDefinition for our user-visible table
     */
    TableDefinition getTableDefinition();

    /**
     * Write newData to this table.
     *
     * @param newData the data to write to this table
     *
     * @throws IOException if there is an error writing the data
     */
    void add(Table newData) throws IOException;

    /**
     * Delete the keys contained in the parameter table from this input table. This method will
     * block until rows are deleted.
     *
     * @param table The rows to delete.
     * @throws IOException If a problem occurred while deleting the rows.
     */
    default void delete(Table table) throws IOException {
        delete(table, table.getIndex());
    }

    /**
     * Delete the keys contained in the parameter table from this input table. This method will
     * block until rows are deleted.
     *
     * @param table The rows to delete.
     * @throws IOException If a problem occurred while deleting the rows.
     */
    void delete(Table table, Index index) throws IOException;

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
        return Arrays.asList(getDefinition().getKeys()).contains(columnName);
    }

    /**
     * Returns true if the specified column exists in this MutableInputTable.
     *
     * @param columnName the column to interrogate
     *
     * @return true if columnName exists in this MutableInputTable
     */
    default boolean hasColumn(String columnName) {
        return isKey(columnName) || Arrays.asList(getDefinition().getValues()).contains(columnName);
    }

    /**
     * Queries whether this MutableInputTable is editable in the current context.
     *
     * @return true if this MutableInputTable may be edited, false otherwise TODO
     *         (deephaven/deephaven-core/issues/255): Add AuthContext and whatever else is
     *         appropriate
     */
    boolean canEdit();
}
