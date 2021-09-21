package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreationLabeledLogic;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreationLogic1Input;
import io.deephaven.qst.TableCreationLogic2Inputs;
import io.deephaven.qst.column.Column;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.NewTable;
import io.deephaven.qst.table.TableSpec;
import io.deephaven.qst.table.TicketTable;
import io.deephaven.qst.table.TimeTable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public abstract class TableHandleManagerDelegate implements TableHandleManager {

    protected abstract TableHandleManager delegate();

    @Override
    public final TableHandle execute(TableSpec table)
            throws TableHandleException, InterruptedException {
        return delegate().execute(table);
    }

    @Override
    public final List<TableHandle> execute(Iterable<TableSpec> tables)
            throws TableHandleException, InterruptedException {
        return delegate().execute(tables);
    }

    @Override
    public final LabeledValues<TableHandle> execute(LabeledTables tables)
            throws TableHandleException, InterruptedException {
        return delegate().execute(tables);
    }

    @Override
    public final TableHandle executeLogic(TableCreationLogic logic)
            throws TableHandleException, InterruptedException {
        return delegate().executeLogic(logic);
    }

    @Override
    public final List<TableHandle> executeLogic(Iterable<TableCreationLogic> logics)
            throws TableHandleException, InterruptedException {
        return delegate().executeLogic(logics);
    }

    @Override
    public final LabeledValues<TableHandle> executeLogic(TableCreationLabeledLogic logic)
            throws TableHandleException, InterruptedException {
        return delegate().executeLogic(logic);
    }

    @Override
    public final TableHandle executeInputs(TableCreationLogic1Input logic, TableHandle t1)
            throws TableHandleException, InterruptedException {
        return delegate().executeInputs(logic, t1);
    }

    @Override
    public final TableHandle executeInputs(TableCreationLogic2Inputs logic, TableHandle t1,
            TableHandle t2) throws TableHandleException, InterruptedException {
        return delegate().executeInputs(logic, t1, t2);
    }


    @Override
    public final TableHandle of(NewTable newTable) {
        return delegate().of(newTable);
    }

    @Override
    public final TableHandle of(EmptyTable emptyTable) {
        return delegate().of(emptyTable);
    }

    @Override
    public final TableHandle of(TimeTable timeTable) {
        return delegate().of(timeTable);
    }

    @Override
    public final TableHandle of(TicketTable ticketTable) {
        return delegate().of(ticketTable);
    }

    @Override
    public final TableHandle merge(Iterable<TableHandle> tableHandles) {
        return delegate().merge(tableHandles);
    }

    @Override
    public final TableHandle emptyTable(long size) {
        return delegate().emptyTable(size);
    }

    @Override
    public final TableHandle newTable(Column<?>... columns) {
        return delegate().newTable(columns);
    }

    @Override
    public final TableHandle newTable(Iterable<Column<?>> columns) {
        return delegate().newTable(columns);
    }

    @Override
    public final TableHandle timeTable(Duration interval) {
        return delegate().timeTable(interval);
    }

    @Override
    public final TableHandle timeTable(Duration interval, Instant startTime) {
        return delegate().timeTable(interval, startTime);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2) {
        return delegate().merge(t1, t2);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2, TableHandle t3) {
        return delegate().merge(t1, t2, t3);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2, TableHandle t3, TableHandle t4) {
        return delegate().merge(t1, t2, t3, t4);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2, TableHandle t3, TableHandle t4,
            TableHandle t5) {
        return delegate().merge(t1, t2, t3, t4, t5);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2, TableHandle t3, TableHandle t4,
            TableHandle t5, TableHandle t6) {
        return delegate().merge(t1, t2, t3, t4, t5, t6);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2, TableHandle t3, TableHandle t4,
            TableHandle t5, TableHandle t6, TableHandle t7) {
        return delegate().merge(t1, t2, t3, t4, t5, t6, t7);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2, TableHandle t3, TableHandle t4,
            TableHandle t5, TableHandle t6, TableHandle t7, TableHandle t8) {
        return delegate().merge(t1, t2, t3, t4, t5, t6, t7, t8);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2, TableHandle t3, TableHandle t4,
            TableHandle t5, TableHandle t6, TableHandle t7, TableHandle t8, TableHandle t9) {
        return delegate().merge(t1, t2, t3, t4, t5, t6, t7, t8, t9);
    }

    @Override
    public final TableHandle merge(TableHandle t1, TableHandle t2, TableHandle t3, TableHandle t4,
            TableHandle t5, TableHandle t6, TableHandle t7, TableHandle t8, TableHandle t9,
            TableHandle... remaining) {
        return delegate().merge(t1, t2, t3, t4, t5, t6, t7, t8, t9, remaining);
    }

    @Override
    public final TableHandle merge(TableHandle[] tableHandles) {
        return delegate().merge(tableHandles);
    }
}
