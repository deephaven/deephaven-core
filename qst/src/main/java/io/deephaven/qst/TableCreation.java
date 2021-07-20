package io.deephaven.qst;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.Table;
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
        OperationsToTable<TOPS, TABLE> toTable, Table table) {
        return TableAdapterImpl.toTable(creation, toOps, toTable, table);
    }

    TABLE of(NewTable newTable);

    TABLE of(EmptyTable emptyTable);

    TABLE of(TimeTable timeTable);

    TABLE of(Collection<TABLE> tables);
}
