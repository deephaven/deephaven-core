package io.deephaven.web.client.api.input;

import elemental2.core.JsObject;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb.AddTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb.DeleteTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.BatchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.MergeTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SelectOrUpdateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TableReference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.batchtablerequest.Operation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A js type for operating on input tables.
 */
@JsType(namespace = "dh", name = "InputTable")
public class JsInputTable {

    private final JsTable table;
    private final String[] keys;
    private final String[] values;

    @JsIgnore
    public JsInputTable(JsTable from, String[] keys, String[] values) {
        this.table = from;
        this.keys = JsObject.freeze(keys);
        this.values = JsObject.freeze(values);
    }

    @JsProperty
    public String[] getKeys() {
        return keys;
    }

    @JsProperty
    public Column[] getKeyColumns() {
        return table.findColumns(keys);
    }

    @JsProperty
    public String[] getValues() {
        return values;
    }

    @JsProperty
    public Column[] getValueColumns() {
        return table.findColumns(values);
    }

    public Promise<JsInputTable> addRow(JsPropertyMap<?> row, @JsOptional String userTimeZone) {
        return addRows(new JsPropertyMap[] {row}, userTimeZone);
    }

    public Promise<JsInputTable> addRows(JsPropertyMap<?>[] rows, @JsOptional String userTimeZone) {
        String[] names =
                Arrays.stream(table.lastVisibleState().getColumns()).map(Column::getName).toArray(String[]::new);
        String[] types =
                Arrays.stream(table.lastVisibleState().getColumns()).map(Column::getType).toArray(String[]::new);

        Object[][] data = new Object[names.length][];
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            Object[] columnArray = new Object[rows.length];
            for (int j = 0; j < rows.length; j++) {
                columnArray[j] = rows[j].get(name);
            }
            data[i] = columnArray;
        }

        return table.getConnection().newTable(names, types, data, userTimeZone, null)
                .then(this::addTable);
    }

    public Promise<JsInputTable> addTable(JsTable tableToAdd) {
        return addTables(new JsTable[] {tableToAdd});
    }

    public Promise<JsInputTable> addTables(JsTable[] tablesToAdd) {
        if (tablesToAdd.length == 0) {
            // noinspection unchecked,rawtypes
            return (Promise) Promise.reject("Must provide at least one table");
        }
        final boolean closeIntermediateTable;
        final Promise<JsTable> mergePromise;
        if (tablesToAdd.length == 1) {
            mergePromise = Promise.resolve(tablesToAdd[0]);
            closeIntermediateTable = false;
        } else {
            mergePromise = table.getConnection().mergeTables(tablesToAdd, this.table);
            closeIntermediateTable = true;
        }

        return mergePromise
                .then(merged -> {
                    // noinspection CodeBlock2Expr - easier readability for chained then()
                    return Callbacks.grpcUnaryPromise(c -> {
                        AddTableRequest addTableRequest = new AddTableRequest();
                        addTableRequest.setInputTable(table.getHeadHandle().makeTicket());
                        addTableRequest.setTableToAdd(merged.getHeadHandle().makeTicket());
                        table.getConnection().inputTableServiceClient().addTableToInputTable(addTableRequest,
                                table.getConnection().metadata(), c::apply);
                    }).then(success -> {
                        if (closeIntermediateTable) {
                            // this is an intermediate table for the merge, close it
                            merged.close();
                        }
                        return Promise.resolve(success);
                    }, failure -> {
                        if (closeIntermediateTable) {
                            // this is an intermediate table for the merge, close it
                            merged.close();
                        }
                        return Promise.reject(failure);
                    });
                })
                .then(response -> Promise.resolve(this));
    }

    public Promise<JsInputTable> deleteTable(JsTable tableToDelete) {
        return deleteTables(new JsTable[] {tableToDelete});
    }

    public Promise<JsInputTable> deleteTables(JsTable[] tablesToDelete) {
        if (tablesToDelete.length == 0) {
            return Promise.resolve(this);
        }

        // for each table, make a view on that table of only key columns, then union the tables and drop together
        final List<JsRunnable> cleanups = new ArrayList<>();
        final Ticket ticketToDelete;
        final Promise<?> failureToReport;
        if (tablesToDelete.length == 1) {
            JsTable onlyTable = tablesToDelete[0];
            // don't try too hard to find matching columns, if it looks like we have a match go for it
            if (onlyTable.getColumns().length == keys.length && onlyTable.findColumns(keys).length == keys.length) {
                ticketToDelete = onlyTable.getHandle().makeTicket();
                failureToReport = Promise.resolve((Object) null);
            } else {
                // view the only table
                ticketToDelete = table.getConnection().getConfig().newTicket();
                cleanups.add(() -> table.getConnection().releaseTicket(ticketToDelete));

                SelectOrUpdateRequest view = new SelectOrUpdateRequest();
                view.setSourceId(onlyTable.state().getHandle().makeTableReference());
                view.setResultId(ticketToDelete);
                view.setColumnSpecsList(keys);
                failureToReport = Callbacks.grpcUnaryPromise(c -> table.getConnection().tableServiceClient()
                        .view(view, table.getConnection().metadata(), c::apply));
            }
        } else {
            // there is more than one table here, construct a merge after making a view of each table
            ticketToDelete = table.getConnection().getConfig().newTicket();
            cleanups.add(() -> table.getConnection().releaseTicket(ticketToDelete));

            BatchTableRequest batch = new BatchTableRequest();
            for (int i = 0; i < tablesToDelete.length; i++) {
                JsTable toDelete = tablesToDelete[i];

                SelectOrUpdateRequest view = new SelectOrUpdateRequest();
                view.setSourceId(toDelete.state().getHandle().makeTableReference());
                view.setColumnSpecsList(keys);
                batch.addOps(new Operation()).setView(view);
            }

            MergeTablesRequest mergeRequest = new MergeTablesRequest();
            mergeRequest.setSourceIdsList(IntStream.range(0, tablesToDelete.length).mapToObj(i -> {
                TableReference ref = new TableReference();
                ref.setBatchOffset(i);
                return ref;
            }).toArray(TableReference[]::new));
            mergeRequest.setResultId(ticketToDelete);
            batch.addOps(new Operation()).setMerge(mergeRequest);

            failureToReport = new Promise<>((resolve, reject) -> {
                ResponseStreamWrapper<ExportedTableCreationResponse> wrapper = ResponseStreamWrapper.of(
                        table.getConnection().tableServiceClient().batch(batch, table.getConnection().metadata()));
                wrapper.onData(response -> {
                    // kill the promise on the first failure we see
                    if (!response.getSuccess()) {
                        reject.onInvoke(response.getErrorInfo());
                    }
                });
                wrapper.onEnd(status -> resolve.onInvoke((Object) null));
            });
        }

        // perform the delete on the current input table
        DeleteTableRequest deleteRequest = new DeleteTableRequest();
        deleteRequest.setInputTable(table.getHeadHandle().makeTicket());
        deleteRequest.setTableToRemove(ticketToDelete);
        return Callbacks.grpcUnaryPromise(c -> {
            table.getConnection().inputTableServiceClient().deleteTableFromInputTable(deleteRequest,
                    table.getConnection().metadata(), c::apply);
        }).then(success -> {
            cleanups.forEach(JsRunnable::run);
            return Promise.resolve(this);
        }, err -> {
            cleanups.forEach(JsRunnable::run);
            // first emit any earlier errors, then if there were not, emit whatever we got from the server for the final
            // call
            return (Promise) failureToReport.then(ignore -> Promise.reject(err));
        });
    }

    @JsProperty
    public JsTable getTable() {
        return table;
    }

}
