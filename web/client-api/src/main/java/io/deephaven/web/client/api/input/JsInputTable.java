package io.deephaven.web.client.api.input;

import elemental2.core.Global;
import elemental2.core.JsObject;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb.AddTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb.DeleteTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.batchtablerequest.Operation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.batch.BatchBuilder;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.batch.BatchTableResponse;
import io.deephaven.web.shared.data.ColumnValue;
import io.deephaven.web.shared.data.RowValues;
import io.deephaven.web.shared.fu.JsBiConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsIgnore;
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

    public Promise<JsInputTable> addRow(JsPropertyMap row) {
        return addRows(new JsPropertyMap[] {row});
    }

    public Promise<JsInputTable> addRows(JsPropertyMap[] rows) {
        return Callbacks.<Void, String>promise(table, c -> {
            final RowValues[] rowValues = new RowValues[rows.length];

            for (int i = 0; i < rows.length; i++) {
                final JsPropertyMap row = rows[i];
                // assert that all keys are filled in...
                for (String key : keys) {
                    if (!row.has(key)) {
                        throw new IllegalStateException("Missing key " + key + " in " + Global.JSON.stringify(row));
                    }
                }

                // starts up a request to the server
                final List<ColumnValue> v = new ArrayList<>();
                for (String key : keys) {
                    v.add(newValue(key, row.get(key)));
                }
                for (String key : values) {
                    v.add(newValue(key, row.get(key)));
                }

                rowValues[i] = new RowValues(v.toArray(new ColumnValue[v.size()]));
            }
            // table.getServer().addRowsToInputTable(table.getHeadHandle(), rowValues, c);
            throw new UnsupportedOperationException("addRowsToInputTable");
        }).then(response -> Promise.resolve(this));
    }

    public Promise<JsInputTable> addTable(JsTable tableToAdd) {
        return addTables(new JsTable[] {tableToAdd});
    }

    public Promise<JsInputTable> addTables(JsTable[] tablesToAdd) {
        if (tablesToAdd.length == 0) {
            //noinspection unchecked,rawtypes
            return (Promise) Promise.reject("Must provide at least one table");
        }
        final Promise<JsTable> mergePromise;
        if (tablesToAdd.length == 1) {
            mergePromise = Promise.resolve(tablesToAdd[0]);
        } else {
            mergePromise = table.getConnection().mergeTables(tablesToAdd, this.table);
        }

        return mergePromise
                .then(merged -> {
                    //noinspection CodeBlock2Expr - easier readability for chained then()
                    return Callbacks.grpcUnaryPromise(c -> {
                        AddTableRequest addTableRequest = new AddTableRequest();
                        addTableRequest.setInputTable(table.getHeadHandle().makeTicket());
                        table.getConnection().inputTableServiceClient().addTableToInputTable(addTableRequest, table.getConnection().metadata(), c::apply);
                    }).then(success -> {
                        if (merged != tablesToAdd[0]) {
                            //this is an intermediate table for the merge, close it
                            merged.close();
                        }
                        return Promise.resolve(success);
                    }, failure -> {
                        if (merged != tablesToAdd[0]) {
                            //this is an intermediate table for the merge, close it
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
        if (tablesToDelete.length == 1) {
            JsTable onlyTable = tablesToDelete[0];
            // don't try too hard to find matching columns, if it looks like we have a match go for it
            if (onlyTable.getColumns().length == keys.length && onlyTable.findColumns(keys).length == keys.length) {
                ticketToDelete = onlyTable.getHandle().makeTicket();
            } else {
                // view the only table
                ticketToDelete = table.getConnection().getConfig().newTicket();
                cleanups.add(() -> table.getConnection().releaseTicket(ticketToDelete));

                SelectOrUpdateRequest view = new SelectOrUpdateRequest();
                view.setSourceId(onlyTable.state().getHandle().makeTableReference());
                view.setResultId(ticketToDelete);
                view.setColumnSpecsList(keys);
                table.getConnection().tableServiceClient().view(view, table.getConnection().metadata(), (fail, success) -> {});
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

            table.getConnection().tableServiceClient().batch(batch, table.getConnection().metadata());
        }

        // perform the delete on the current input table
        DeleteTableRequest deleteRequest = new DeleteTableRequest();
        deleteRequest.setInputTable(table.getHeadHandle().makeTicket());
        deleteRequest.setTableToRemove(ticketToDelete);
        return Callbacks.grpcUnaryPromise(c -> {
            table.getConnection().inputTableServiceClient().deleteTableFromInputTable(deleteRequest, table.getConnection().metadata(), c::apply);
        }).then(success -> {
            cleanups.forEach(JsRunnable::run);
            return Promise.resolve(this);
        }, err -> {
            cleanups.forEach(JsRunnable::run);
            return (Promise) Promise.reject(err);
        });
    }

    @JsProperty
    public JsTable getTable() {
        return table;
    }

    @JsIgnore
    private ColumnValue newValue(String key, Object value) {
        final Column column = table.findColumn(key);
        ColumnValue val = new ColumnValue();
        val.setColumnId(column.getIndex());
        final String serialized = ColumnValueDehydrater.serialize(column.getType(), value);
        val.setValue(serialized);
        return val;
    }

}
