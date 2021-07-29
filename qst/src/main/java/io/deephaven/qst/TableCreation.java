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
public interface TableCreation<TABLE> {

    static <TOPS extends TableOperations<TOPS, TABLE>, TABLE> TABLE create(
        TableCreation<TABLE> creation, TableToOperations<TOPS, TABLE> toOps,
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
     */
    TABLE merge(Collection<TABLE> tables);
}
