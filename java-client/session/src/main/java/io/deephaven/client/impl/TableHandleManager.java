package io.deephaven.client.impl;

import io.deephaven.client.impl.TableHandle.TableHandleException;
import io.deephaven.qst.LabeledValues;
import io.deephaven.qst.TableCreationLabeledLogic;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreationLogic1Input;
import io.deephaven.qst.TableCreationLogic2Inputs;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.LabeledTables;
import io.deephaven.qst.table.TableSpec;

import java.util.List;

/**
 * A table handle manager is able to execute {@linkplain TableSpec tables}, {@link }
 */
public interface TableHandleManager extends TableCreator<TableHandle> {

    TableHandle execute(TableSpec table) throws TableHandleException, InterruptedException;

    List<TableHandle> execute(Iterable<TableSpec> tables)
            throws TableHandleException, InterruptedException;

    LabeledValues<TableHandle> execute(LabeledTables tables)
            throws TableHandleException, InterruptedException;

    TableHandle executeLogic(TableCreationLogic logic)
            throws TableHandleException, InterruptedException;

    List<TableHandle> executeLogic(Iterable<TableCreationLogic> logics)
            throws TableHandleException, InterruptedException;

    LabeledValues<TableHandle> executeLogic(TableCreationLabeledLogic logic)
            throws TableHandleException, InterruptedException;

    TableHandle executeInputs(TableCreationLogic1Input logic, TableHandle t1)
            throws TableHandleException, InterruptedException;

    TableHandle executeInputs(TableCreationLogic2Inputs logic, TableHandle t1, TableHandle t2)
            throws TableHandleException, InterruptedException;
}
