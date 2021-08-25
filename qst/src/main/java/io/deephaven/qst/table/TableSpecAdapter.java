package io.deephaven.qst.table;

import io.deephaven.api.TableOperations;
import io.deephaven.api.TableOperationsAdapter;

public abstract class TableSpecAdapter<TOPS extends TableOperations<TOPS, TABLE>, TABLE>
        extends TableOperationsAdapter<TOPS, TABLE, TableSpec, TableSpec> {

    public TableSpecAdapter(TableSpec table) {
        super(table);
    }

    public TableSpec table() {
        return delegate();
    }

}
