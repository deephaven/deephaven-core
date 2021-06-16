package io.deephaven.qst;

import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.QueryScopeTable;
import io.deephaven.qst.table.SourceTable;
import io.deephaven.qst.table.Table;

/**
 * Provides methods for building the {@link SourceTable source tables}.
 *
 * @param <OPS_TYPE> the table operations type
 * @param <TABLE> the table type
 */
public interface TableCreation<OPS_TYPE extends TableOperations<OPS_TYPE, TABLE>, TABLE> {

    static <OPS_TYPE extends TableOperations<OPS_TYPE, TABLE>, TABLE> OPS_TYPE create(
        TableCreation<OPS_TYPE, TABLE> creation, Table table) {
        return TableCreationAdapterImpl.of(creation, table);
    }

    OPS_TYPE of(NewTable newTable);

    OPS_TYPE of(EmptyTable emptyTable);

    OPS_TYPE of(QueryScopeTable queryScopeTable);
}
