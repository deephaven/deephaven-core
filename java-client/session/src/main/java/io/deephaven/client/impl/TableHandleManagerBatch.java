package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.TableCreationLabeledLogic;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreationLogic1Input;
import io.deephaven.qst.TableCreationLogic2Inputs;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.TableSpec;

import java.util.List;
import java.util.stream.StreamSupport;

/**
 * A table handle manager that executes requests as a single batch.
 *
 * <p>
 * A batch request is executed in a single round-trip.
 *
 * <p>
 * Note: individual {@linkplain io.deephaven.api.TableOperations table operations} executed against
 * a {@linkplain TableHandle table handle} are still executed serially.
 */
class TableHandleManagerBatch extends TableHandleManagerBase {

    public static TableHandleManagerBatch of(Session session) {
        return new TableHandleManagerBatch(session);
    }

    private TableHandleManagerBatch(Session session) {
        super(session, null);
    }

    @Override
    public TableHandle execute(TableSpec table) throws TableHandleException, InterruptedException {
        return TableHandle.of(session, table, lifecycle);
    }

    @Override
    public List<TableHandle> execute(Iterable<TableSpec> tables)
        throws TableHandleException, InterruptedException {
        return TableHandle.of(session, tables, lifecycle);
    }

    @Override
    public TableHandle executeLogic(TableCreationLogic logic)
        throws TableHandleException, InterruptedException {
        return execute(TableSpec.of(logic));
    }

    @Override
    public List<TableHandle> executeLogic(Iterable<TableCreationLogic> logics)
        throws TableHandleException, InterruptedException {
        return execute(
            () -> StreamSupport.stream(logics.spliterator(), false).map(TableSpec::of).iterator());
    }

    @Override
    public LabeledValues<TableHandle> executeLogic(TableCreationLabeledLogic logic)
        throws TableHandleException, InterruptedException {
        return execute(LabeledTables.of(logic));
    }

    @Override
    public TableHandle executeInputs(TableCreationLogic1Input logic, TableHandle t1)
        throws TableHandleException, InterruptedException {
        final TableSpec t1Table = t1.table();
        final TableSpec outTable = logic.create(t1Table);
        return execute(outTable);
    }

    @Override
    public TableHandle executeInputs(TableCreationLogic2Inputs logic, TableHandle t1,
        TableHandle t2) throws TableHandleException, InterruptedException {
        final TableSpec t1Table = t1.table();
        final TableSpec t2Table = t2.table();
        final TableSpec outTable = logic.create(t1Table, t2Table);
        return execute(outTable);
    }
}
