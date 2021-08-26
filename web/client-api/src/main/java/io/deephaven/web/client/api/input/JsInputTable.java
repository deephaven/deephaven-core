package io.deephaven.web.client.api.input;

import elemental2.core.Global;
import elemental2.core.JsObject;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.batch.BatchBuilder;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.batch.BatchTableRequest;
import io.deephaven.web.shared.batch.BatchTableResponse;
import io.deephaven.web.shared.data.ColumnValue;
import io.deephaven.web.shared.data.RowValues;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        return Callbacks.<Void, String>promise(this.table, c -> {
            // table.getServer().addTablesToInputTable(table.getHeadHandle(), Arrays.stream(tablesToAdd).map(t ->
            // t.getHandle()).toArray(TableHandle[]::new), c);
            throw new UnsupportedOperationException("addTablesToInputTable");
        }).then(response -> Promise.resolve(this));
    }

    public Promise<JsInputTable> deleteTable(JsTable tableToDelete) {
        return deleteTables(new JsTable[] {tableToDelete});
    }

    public Promise<JsInputTable> deleteTables(JsTable[] tablesToDelete) {
        final List<String> keysList = Arrays.asList(keys);
        final List<JsRunnable> cleanups = new ArrayList<>();
        final BatchBuilder builder = new BatchBuilder();

        // Limit the tables to delete to just the key columns
        for (int i = 0; i < tablesToDelete.length; i++) {
            final JsTable tableToDelete = tablesToDelete[i];
            final BatchBuilder.BatchOp op = builder.getOp();
            final WorkerConnection connection = tableToDelete.getConnection();
            final ClientTableState cts = connection.newState(tableToDelete.state(), op);
            cleanups.add(cts.retain(this));

            // final HandleMapping mapping = new HandleMapping(tableToDelete.getHandle(), cts.getHandle());
            // op.fromState(cts);
            // op.setAppendTo(cts.getPrevious());
            // op.setHandles(mapping);
            // builder.setViewColumns(keysList);
            // builder.nextOp(connection, op);
            throw new UnsupportedOperationException("Can't build batch");
        }

        // TODO core#273
        BatchTableRequest req = new BatchTableRequest();
        // req.setOps(builder.serializable());

        return Callbacks.<BatchTableResponse, String>promise(this.table, c -> {
            // table.getServer().batch(req, c);
            throw new UnsupportedOperationException("batch");
        }).then(response -> {
            if (response.getFailureMessages().length > 0) {
                return (Promise) Promise
                        .reject("Unable to delete tables: " + Arrays.toString(response.getFailureMessages()));
            }
            return Callbacks.<Void, String>promise(this.table, c -> {
                // table.getServer().deleteTablesFromInputTable(table.getHeadHandle(), response.getSuccess(), c);
                throw new UnsupportedOperationException("deleteTablesFromInputTable");
            });
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
