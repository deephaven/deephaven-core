package io.deephaven.qst;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TimeTable;

import java.util.Collection;

/**
 * Provides methods for building source tables.
 *
 * @param <TABLE> the table type
 */
public interface TableCreator<TABLE> {

    /**
     * "Replay" the {@code table} against the given interfaces.
     *
     * @param creation the table creation
     * @param toOps the table to operations
     * @param toTable the operations to table
     * @param table the table specification
     * @param <TOPS> the table operations type
     * @param <TABLE> the output table type
     * @return the output table
     */
    static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> TABLE create(
        TableCreator<TABLE> creation, TableToOperations<TOPS, TABLE> toOps,
        OperationsToTable<TOPS, TABLE> toTable, TableSpec table) {
        return TableAdapterImpl.toTable(creation, toOps, toTable, table);
    }

    /**
     * Creates a new table.
     *
     * @param newTable the new table specification
     * @return the new table
     */
    TABLE of(NewTable newTable);

    /**
     * Creates an empty table.
     *
     * @param emptyTable the empty table specification
     * @return the empty table
     */
    TABLE of(EmptyTable emptyTable);

    /**
     * Creates a time table.
     *
     * @param timeTable the time table specifications
     * @return the time table
     */
    TABLE of(TimeTable timeTable);

    /**
     * Merges the given {@code tables}.
     *
     * @param tables the tables
     * @return the merged results
     * @see io.deephaven.qst.table.MergeTable
     */
    TABLE merge(Collection<TABLE> tables);

    /**
     * Transform a table operation into a table.
     *
     * @param <TOPS> the table operations type
     * @param <TABLE> the output table type
     * @see #create(TableCreator, TableToOperations, OperationsToTable, TableSpec)
     */
    @FunctionalInterface
    interface OperationsToTable<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

        TABLE of(TOPS tableOperations);
    }

    /**
     * Transform a table into a table operation.
     *
     * @param <TOPS> the table operations type
     * @param <TABLE> the output table type
     * @see #create(TableCreator, TableToOperations, OperationsToTable, TableSpec)
     */
    @FunctionalInterface
    interface TableToOperations<TOPS extends TableOperations<TOPS, TABLE>, TABLE> {

        TOPS of(TABLE table);
    }
}
