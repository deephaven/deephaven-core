package io.deephaven.qst;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.QueryScopeTable;
import io.deephaven.qst.table.SourceTable;

/**
 * Provides methods for building the {@link SourceTable source tables}.
 *
 * @param <TABLE_OPS> the table operations type
 * @param <TABLE> the table type
 */
public interface TableCreation<TABLE_OPS extends TableOperations<TABLE_OPS, TABLE>, TABLE> {

    static <TABLE_OPS extends TableOperations<TABLE_OPS, TABLE>, TABLE> TABLE_OPS create(
        TableCreation<TABLE_OPS, TABLE> creation, io.deephaven.qst.table.Table table) {
        return TableCreationAdapterImpl.of(creation, table);
    }

    TABLE_OPS of(NewTable newTable);

    TABLE_OPS of(EmptyTable emptyTable);

    TABLE_OPS of(QueryScopeTable queryScopeTable);
}
