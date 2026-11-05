//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.input;

import elemental2.core.JsObject;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.AddTableRequest;
import io.deephaven.proto.backplane.grpc.AddTableResponse;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.DeleteTableRequest;
import io.deephaven.proto.backplane.grpc.DeleteTableResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsLazy;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.JsTableReference;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A js type for operating on input tables.
 *
 * Represents a User Input Table, which can have data added to it from other sources.
 *
 * You may add rows using dictionaries of key-value tuples (representing columns by name), add tables containing all the
 * key/value columns to add, or delete tables containing the keys to delete. Each operation is atomic, and will either
 * succeed completely or fail completely. To guarantee order of operations, apply an operation and wait for the response
 * before sending the next operation.
 *
 * Each table has one or more key columns, where each unique combination of keys will appear at most once in the table.
 *
 * To view the results of the Input Table, you should use standard table operations on the {@link JsInputTable}'s source
 * {@link JsTable} object.
 */
@JsType(namespace = "dh", name = "InputTable")
public class JsInputTable {

    private final JsTable table;
    private final String[] keys;
    private final String[] values;
    private final JsLazy<Column[]> keyColumns;
    private final JsLazy<Column[]> valueColumns;

    @JsIgnore
    public JsInputTable(JsTable from, String[] keys, String[] values) {
        this.table = from;
        this.keys = JsObject.freeze(keys);
        this.values = JsObject.freeze(values);
        this.keyColumns = JsLazy.of(() -> JsObject.freeze(table.findColumns(keys)));
        this.valueColumns = JsLazy.of(() -> JsObject.freeze(table.findColumns(values)));
    }

    /**
     * A list of the key columns by name.
     * 
     * @return String array.
     */
    @JsProperty
    public String[] getKeys() {
        return keys;
    }

    /**
     * A list of the key columns.
     *
     * @return Column array.
     */
    @JsProperty
    public Column[] getKeyColumns() {
        return keyColumns.get();
    }


    /**
     * A list of the value columns by name.
     * 
     * @return String array.
     */
    @JsProperty
    public String[] getValues() {
        return values;
    }

    /**
     * A list of the value {@link Column} objects.
     * 
     * @return {@link Column} array.
     */
    @JsProperty
    public Column[] getValueColumns() {
        return valueColumns.get();
    }

    /**
     * Adds a single row to the table. For each key or value column name in the Input Table, we retrieve that javascript
     * property at that name and validate it can be put into the given column type.
     * 
     * @param row
     * @param userTimeZone
     * @return Promise of dh.InputTable
     */
    public Promise<JsInputTable> addRow(JsPropertyMap<?> row, @JsOptional @JsNullable String userTimeZone) {
        return addRows(new JsPropertyMap[] {row}, userTimeZone);
    }

    /**
     * Add multiple rows to a table.
     * 
     * @param rows
     * @param userTimeZone
     * @return Promise of dh.InputTable
     */
    public Promise<JsInputTable> addRows(JsPropertyMap<?>[] rows, @JsOptional @JsNullable String userTimeZone) {
        // Filter out columns that are not keys or values of the input table
        Column[] filteredColumns = Arrays.stream(table.lastVisibleState().getColumns())
                .filter(column -> column.isInputTableKeyColumn() || column.isInputTableValueColumn())
                .toArray(Column[]::new);

        String[] names = Arrays.stream(filteredColumns)
                .map(Column::getName)
                .toArray(String[]::new);

        String[] types = Arrays.stream(filteredColumns)
                .map(Column::getType)
                .toArray(String[]::new);

        Object[][] data = new Object[names.length][];
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            Object[] columnArray = new Object[rows.length];
            for (int j = 0; j < rows.length; j++) {
                columnArray[j] = rows[j].get(name);
            }
            data[i] = columnArray;
        }

        // TODO deephaven-core#2529 parallelize this
        return table.getConnection().newTable(names, types, data, userTimeZone).asPromise()
                .then(created -> addTable(JsTableReference.of(created)));
    }

    /**
     * Add an entire table to this Input Table. Only column names that match the definition of the input table will be
     * copied, and all key columns must have values filled in. This only copies the current state of the source table;
     * future updates to the source table will not be reflected in the Input Table. The returned promise will be
     * resolved to the same {@link JsInputTable} instance this method was called upon once the server returns.
     *
     * @param tableToAdd
     * @return Promise of dh.InputTable
     */
    public Promise<JsInputTable> addTable(JsTableReference tableToAdd) {
        return addTables(new JsTableReference[] {tableToAdd});
    }

    /**
     * The ticket an input-table operation should act on for a given reference. A {@link JsTable} contributes its head
     * state, as it always has here; anything else contributes the table it refers to.
     */
    private static Ticket ticketOf(JsTableReference reference) {
        if ((Object) reference instanceof JsTable) {
            return reference.asTable().getHeadHandle().makeTicket();
        }
        return reference.typedTicket().getTicket();
    }

    /**
     * Add multiple tables to this Input Table.
     *
     * @param tablesToAdd
     * @return Promise of dh.InputTable
     */
    public Promise<JsInputTable> addTables(JsTableReference[] tablesToAdd) {
        if (tablesToAdd.length == 0) {
            // noinspection unchecked,rawtypes
            return (Promise) Promise.reject("Must provide at least one table");
        }
        if (tablesToAdd.length == 1) {
            return addTicket(ticketOf(tablesToAdd[0]), null);
        }
        // Merge first; the merged table is an intermediate to close once the add has settled
        return table.getConnection().mergeTables(tablesToAdd, this.table).asPromise()
                .then(merged -> addTicket(merged.getHeadHandle().makeTicket(), merged));
    }

    private Promise<JsInputTable> addTicket(Ticket tableToAdd, @Nullable JsTable intermediate) {
        return Callbacks.<AddTableResponse>grpcUnaryPromise(c -> {
            AddTableRequest addTableRequest = AddTableRequest.newBuilder()
                    .setInputTable(table.getHeadHandle().makeTicket())
                    .setTableToAdd(tableToAdd)
                    .build();
            table.getConnection().inputTableServiceClient().addTableToInputTable(addTableRequest, c);
        }).then(success -> {
            if (intermediate != null) {
                intermediate.close();
            }
            return Promise.resolve(this);
        }, failure -> {
            if (intermediate != null) {
                intermediate.close();
            }
            return Promise.reject(failure);
        });
    }

    /**
     * Deletes an entire table from this Input Table. Key columns must match the Input Table.
     * 
     * @param tableToDelete
     * @return Promise of dh.InputTable
     */
    public Promise<JsInputTable> deleteTable(JsTableReference tableToDelete) {
        return deleteTables(new JsTableReference[] {tableToDelete});
    }

    /**
     * Delete multiple tables from this Input Table.
     * 
     * @param tablesToDelete
     * @return
     */
    public Promise<JsInputTable> deleteTables(JsTableReference[] tablesToDelete) {
        if (tablesToDelete.length == 0) {
            return Promise.resolve(this);
        }

        // for each table, make a view on that table of only key columns, then union the tables and drop together
        final List<JsRunnable> cleanups = new ArrayList<>();
        final Ticket ticketToDelete;
        final Promise<?> failureToReport;
        if (tablesToDelete.length == 1) {
            JsTableReference onlyTable = tablesToDelete[0];
            // A Table has columns to inspect: if it looks like it is already just the key columns, use it as is. Any
            // other reference has no columns yet, so it always takes the view.
            if ((Object) onlyTable instanceof JsTable
                    && onlyTable.asTable().getColumns().length == keys.length
                    && onlyTable.asTable().findColumns(keys).length == keys.length) {
                ticketToDelete = onlyTable.asTable().getHandle().makeTicket();
                failureToReport = Promise.resolve((Object) null);
            } else {
                // view the only table
                ticketToDelete = table.getConnection().getTickets().newExportTicket();
                cleanups.add(() -> table.getConnection().releaseTicket(ticketToDelete));

                SelectOrUpdateRequest view = SelectOrUpdateRequest.newBuilder()
                        .setSourceId(TableReference.newBuilder().setTicket(onlyTable.typedTicket().getTicket()))
                        .setResultId(ticketToDelete)
                        .addAllColumnSpecs(Arrays.asList(keys))
                        .build();
                failureToReport = Callbacks
                        .<ExportedTableCreationResponse>grpcUnaryPromise(c -> table.getConnection().tableServiceClient()
                                .view(view, c));
            }
        } else {
            // there is more than one table here, construct a merge after making a view of each table
            ticketToDelete = table.getConnection().getTickets().newExportTicket();
            cleanups.add(() -> table.getConnection().releaseTicket(ticketToDelete));

            BatchTableRequest.Builder batch = BatchTableRequest.newBuilder();
            for (int i = 0; i < tablesToDelete.length; i++) {
                JsTableReference toDelete = tablesToDelete[i];

                SelectOrUpdateRequest view = SelectOrUpdateRequest.newBuilder()
                        .setSourceId(TableReference.newBuilder().setTicket(toDelete.typedTicket().getTicket()))
                        .addAllColumnSpecs(Arrays.asList(keys))
                        .build();
                BatchTableRequest.Operation.Builder op = BatchTableRequest.Operation.newBuilder()
                        .setView(view);
                batch.addOps(op);
            }

            MergeTablesRequest mergeRequest = MergeTablesRequest.newBuilder()
                    .addAllSourceIds(IntStream.range(0, tablesToDelete.length).mapToObj(i -> {
                        return TableReference.newBuilder()
                                .setBatchOffset(i)
                                .build();
                    }).collect(Collectors.toList()))
                    .setResultId(ticketToDelete)
                    .build();
            BatchTableRequest.Operation.Builder op = BatchTableRequest.Operation.newBuilder();
            op.setMerge(mergeRequest);
            batch.addOps(op);

            failureToReport = new Promise<>((resolve, reject) -> {
                ResponseStreamWrapper<ExportedTableCreationResponse> wrapper = ResponseStreamWrapper
                        .of(observer -> table.getConnection().tableServiceClient().batch(batch.build(), observer));
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
        DeleteTableRequest deleteRequest = DeleteTableRequest.newBuilder()
                .setInputTable(table.getHeadHandle().makeTicket())
                .setTableToRemove(ticketToDelete)
                .build();
        return Callbacks.<DeleteTableResponse>grpcUnaryPromise(c -> {
            table.getConnection().inputTableServiceClient().deleteTableFromInputTable(deleteRequest, c);
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

    /**
     * The source table for this Input Table.
     * 
     * @return dh.table
     */
    @JsProperty
    public JsTable getTable() {
        return table;
    }

}
