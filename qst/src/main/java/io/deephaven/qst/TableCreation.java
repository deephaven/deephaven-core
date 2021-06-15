package io.deephaven.qst;

import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.Table;

public interface TableCreation<BUILDER extends TableOperations<BUILDER, TABLE>, TABLE> {

    static <BUILDER extends TableOperations<BUILDER, TABLE>, TABLE> BUILDER create(
        TableCreation<BUILDER, TABLE> creation, Table table) {
        return TableCreationAdapterImpl.of(creation, table);
    }

    BUILDER of(NewTable newTable);

    BUILDER of(EmptyTable emptyTable);
}
