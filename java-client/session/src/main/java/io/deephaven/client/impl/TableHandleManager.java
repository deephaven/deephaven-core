//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
 * A table handle manager is able to execute commands that produce tables, by accepting {@link TableSpec}s,
 * {@link TableCreationLogic}s, and more.
 */
public interface TableHandleManager extends TableCreator<TableHandle> {

    /**
     * Executes the given {@code table}, waiting for the export to complete successfully before returning. If
     * applicable, the request will build off of the existing exports.
     *
     * @param table the table spec
     * @return the table handle
     * @throws TableHandleException if there was an exception on the exported table creation response or an RPC
     *         exception
     * @throws InterruptedException if the thread was interrupted while waiting
     * @see TableService#executeAsync(TableSpec)
     */
    TableHandle execute(TableSpec table) throws TableHandleException, InterruptedException;

    /**
     * Executes the given {@code tables}, waiting for all of the exports to complete successfully before returning. If
     * applicable, the request will build off of the existing exports.
     *
     * @param tables the table spec
     * @return the table handles
     * @throws TableHandleException if there was an exception in any of the exported table creation response or an RPC
     *         exception
     * @throws InterruptedException if the thread was interrupted while waiting
     * @see TableService#executeAsync(Iterable)
     */
    List<TableHandle> execute(Iterable<TableSpec> tables) throws TableHandleException, InterruptedException;

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
