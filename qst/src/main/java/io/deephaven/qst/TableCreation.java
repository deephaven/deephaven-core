package io.deephaven.qst;

import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.QueryScopeTable;
import io.deephaven.qst.table.SourceTable;
import io.deephaven.qst.table.Table;

/**
 * Provides methods for building the {@link SourceTable source tables}.
 *
 * @param <BUILDER> the table operations type
 * @param <TABLE> the table type
 */
public interface TableCreation<BUILDER extends TableOperations<BUILDER, TABLE>, TABLE> {

    static <BUILDER extends TableOperations<BUILDER, TABLE>, TABLE> BUILDER create(
        TableCreation<BUILDER, TABLE> creation, Table table) {
        return TableCreationAdapterImpl.of(creation, table);
    }

    BUILDER of(NewTable newTable);

    BUILDER of(EmptyTable emptyTable);

    BUILDER of(QueryScopeTable queryScopeTable);
}
