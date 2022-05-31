package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.Lifecycle;
import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.InputTable;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.MergeTable;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.util.Objects;

public abstract class TableHandleManagerBase implements TableHandleManager {

    protected final Session session;
    protected final Lifecycle lifecycle;

    protected TableHandleManagerBase(Session session, Lifecycle lifecycle) {
        this.session = Objects.requireNonNull(session);
        this.lifecycle = lifecycle;
    }

    @Override
    public final LabeledValues<TableHandle> execute(LabeledTables tables)
            throws TableHandleException, InterruptedException {
        return LabeledValues.of(tables.labels(), execute(tables.values()));
    }

    @Override
    public final TableHandle of(NewTable newTable) {
        return handle(newTable);
    }

    @Override
    public final TableHandle of(EmptyTable emptyTable) {
        return handle(emptyTable);
    }

    @Override
    public final TableHandle of(TimeTable timeTable) {
        return handle(timeTable);
    }

    @Override
    public final TableHandle of(TicketTable ticketTable) {
        return handle(ticketTable);
    }

    @Override
    public final TableHandle of(InputTable inputTable) {
        return handle(inputTable);
    }

    @Override
    public final TableHandle merge(Iterable<TableHandle> tableProxies) {
        MergeTable.Builder builder = MergeTable.builder();
        for (TableHandle tableProxy : tableProxies) {
            builder.addTables(tableProxy.table());
        }
        return handle(builder.build());
    }

    private TableHandle handle(TableSpec table) {
        return TableHandle.ofUnchecked(session, table, lifecycle);
    }
}
