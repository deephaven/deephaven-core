package io.deephaven.client.impl;

import io.deephaven.client.ExportedTable;
import io.deephaven.qst.table.Table;
import org.immutables.value.Value.Immutable;

@Immutable
abstract class ExportedTableImpl implements ExportedTable {

    @Override
    public abstract ExportManagerImpl manager();

    @Override
    public abstract Table table();

    public abstract long ticket();
}
