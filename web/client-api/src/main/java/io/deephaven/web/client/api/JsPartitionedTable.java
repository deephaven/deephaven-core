//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.JsSet;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.GetTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.MergeRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.PartitionedTableDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SelectOrUpdateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.api.lifecycle.HasLifecycle;
import io.deephaven.web.client.api.subscription.SubscriptionTableData;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.RangeSet;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.*;

/**
 * Represents a set of Tables each corresponding to some key. The keys are available locally, but a call must be made to
 * the server to get each Table. All tables will have the same structure.
 */
@JsType(namespace = "dh", name = "PartitionedTable")
public class JsPartitionedTable extends HasLifecycle implements ServerObject {

    /**
     * Indicates that a new key has been added to the array of keys, which can now be fetched with getTable.
     */
    public static final String EVENT_KEYADDED = "keyadded",
            EVENT_DISCONNECT = JsTable.EVENT_DISCONNECT,
            EVENT_RECONNECT = JsTable.EVENT_RECONNECT,
            /**
             * Indicates that an error has occurred while communicating with the server.
             */
            EVENT_RECONNECTFAILED = JsTable.EVENT_RECONNECTFAILED;

    private final WorkerConnection connection;
    private final JsWidget widget;
    private List<String> keyColumnTypes;
    private PartitionedTableDescriptor descriptor;
    private Promise<JsTable> keys;
    private JsTable baseTable;
    private TableSubscription subscription;

    private final Set<List<Object>> knownKeys = new HashSet<>();

    private Column[] keyColumns;

    private Column[] columns;


    @JsIgnore
    public JsPartitionedTable(WorkerConnection connection, JsWidget widget) {
        this.connection = connection;
        this.widget = widget;
    }

    @JsIgnore
    public Promise<JsPartitionedTable> refetch() {
        closeSubscriptions();

        return widget.refetch().then(w -> {
            descriptor = PartitionedTableDescriptor.deserializeBinary(w.getDataAsU8());

            return w.getExportedObjects()[0].fetch();
        }).then(result -> {
            baseTable = (JsTable) result;
            keyColumnTypes = new ArrayList<>();
            InitialTableDefinition tableDefinition = WebBarrageUtils.readTableDefinition(
                    WebBarrageUtils.readSchemaMessage(descriptor.getConstituentDefinitionSchema_asU8()));
            ColumnDefinition[] columnDefinitions = tableDefinition.getColumns();
            Column[] columns = new Column[0];
            for (int i = 0; i < columnDefinitions.length; i++) {
                ColumnDefinition columnDefinition = columnDefinitions[i];
                Column column =
                        columnDefinition.makeJsColumn(columns.length, tableDefinition.getColumnsByName());
                columns[columns.length] = column;
            }
            Column[] keyColumns = new Column[0];
            JsArray<String> keyColumnNames = descriptor.getKeyColumnNamesList();
            for (int i = 0; i < keyColumnNames.length; i++) {
                String name = keyColumnNames.getAt(i);
                Column keyColumn = baseTable.findColumn(name);
                keyColumnTypes.add(keyColumn.getType());
                keyColumns[keyColumns.length] = keyColumn;
            }
            this.columns = JsObject.freeze(columns);
            this.keyColumns = JsObject.freeze(keyColumns);

            // TODO(deephaven-core#3604) in case of a new session, we should do a full refetch
            baseTable.addEventListener(JsTable.EVENT_DISCONNECT, event -> fireEvent(EVENT_DISCONNECT));
            baseTable.addEventListener(JsTable.EVENT_RECONNECT, event -> {
                subscribeToBaseTable().then(ignore -> {
                    unsuppressEvents();
                    fireEvent(EVENT_RECONNECT);
                    return null;
                }, failure -> {
                    unsuppressEvents();
                    fireEvent(EVENT_RECONNECTFAILED, failure);
                    suppressEvents();
                    return null;
                });
            });
            return subscribeToBaseTable();
        });
    }

    @JsIgnore
    @Override
    public TypedTicket typedTicket() {
        return widget.typedTicket();
    }

    private Promise<JsPartitionedTable> subscribeToBaseTable() {
        subscription = baseTable.subscribe(
                JsArray.asJsArray(baseTable.findColumns(descriptor.getKeyColumnNamesList().asArray(new String[0]))));
        subscription.addEventListener(TableSubscription.EVENT_UPDATED, this::handleKeys);

        LazyPromise<JsPartitionedTable> promise = new LazyPromise<>();
        subscription.addEventListenerOneShot(TableSubscription.EVENT_UPDATED, data -> promise.succeed(this));
        baseTable.addEventListener(JsTable.EVENT_DISCONNECT, e -> promise.fail("Underlying table disconnected"));
        return promise.asPromise();
    }

    private void handleKeys(Event<SubscriptionTableData> update) {

        // We're only interested in added rows, send an event indicating the new keys that are available
        SubscriptionTableData eventData = update.getDetail();
        RangeSet added = eventData.getAdded().getRange();
        added.indexIterator().forEachRemaining((long index) -> {
            // extract the key to use
            JsArray<Object> key = eventData.getColumns().map((c, p1) -> eventData.getData(index, c));
            knownKeys.add(key.asList());
            fireEvent(EVENT_KEYADDED, key);
        });
    }

    /**
     * Fetch the table with the given key. If the key does not exist, returns `null`.
     *
     * @param key The key to fetch. An array of values for each key column, in the same order as the key columns are.
     * @return Promise of dh.Table, or `null` if the key does not exist.
     */
    public Promise<@JsNullable JsTable> getTable(Object key) {
        // Wrap non-arrays in an array so we are consistent with how we track keys
        if (!JsArray.isArray(key)) {
            key = JsArray.of(key);
        }
        final List<Object> keyList = Js.<JsArray<Object>>uncheckedCast(key).asList();
        if (!knownKeys.contains(keyList)) {
            // key doesn't even exist, just hand back a null table
            return Promise.resolve((JsTable) null);
        }
        final String[] columnNames = descriptor.getKeyColumnNamesList().asArray(new String[0]);
        final String[] columnTypes = keyColumnTypes.toArray(new String[0]);
        final Object[][] keysData = keyList.stream().map(item -> new Object[] {item}).toArray(Object[][]::new);
        final ClientTableState entry = connection.newState((c, cts, metadata) -> {
            // TODO deephaven-core#2529 parallelize this
            connection.newTable(
                    columnNames,
                    columnTypes,
                    keysData,
                    null,
                    this)
                    .then(table -> {
                        GetTableRequest getTableRequest = new GetTableRequest();
                        getTableRequest.setPartitionedTable(widget.getTicket());
                        getTableRequest.setKeyTableTicket(table.getHandle().makeTicket());
                        getTableRequest.setResultId(cts.getHandle().makeTicket());
                        connection.partitionedTableServiceClient().getTable(getTableRequest, connection.metadata(),
                                (error, success) -> {
                                    table.close();
                                    c.apply(error, success);
                                });
                        return null;
                    });
        },
                "partitioned table key " + key);

        return entry.refetch(this, connection.metadata())
                .then(cts -> Promise.resolve(new JsTable(cts.getConnection(), cts)));
    }

    /**
     * Open a new table that is the result of merging all constituent tables. See
     * {@link io.deephaven.engine.table.PartitionedTable#merge()} for details.
     *
     * @return A merged representation of the constituent tables.
     */
    public Promise<JsTable> getMergedTable() {
        return connection.newState((c, cts, metadata) -> {
            MergeRequest requestMessage = new MergeRequest();
            requestMessage.setPartitionedTable(widget.getTicket());
            requestMessage.setResultId(cts.getHandle().makeTicket());
            connection.partitionedTableServiceClient().merge(requestMessage, connection.metadata(), c::apply);
        }, "partitioned table merged table")
                .refetch(this, connection.metadata())
                .then(cts -> Promise.resolve(new JsTable(cts.getConnection(), cts)));
    }

    /**
     * The set of all currently known keys. This is kept up to date, so getting the list after adding an event listener
     * for <b>keyadded</b> will ensure no keys are missed.
     *
     * @return Set of Object
     */
    public JsSet<Object> getKeys() {
        if (subscription.getColumns().length == 1) {
            return new JsSet<>(knownKeys.stream().map(list -> list.get(0)).toArray());
        }
        return new JsSet<>(knownKeys.stream().map(List::toArray).toArray());
    }

    /**
     * The count of known keys.
     *
     * @return int
     */
    @JsProperty(name = "size")
    public int size() {
        return knownKeys.size();
    }

    /**
     * An array of all the key columns that the tables are partitioned by.
     *
     * @return Array of Column
     */
    @JsProperty
    public Column[] getKeyColumns() {
        return keyColumns;
    }

    /**
     * An array of the columns in the tables that can be retrieved from this partitioned table, including both key and
     * non-key columns.
     *
     * @return Array of Column
     */
    @JsProperty
    public Column[] getColumns() {
        return columns;
    }

    /**
     * Fetch a table containing all the valid keys of the partitioned table.
     *
     * @return Promise of a Table
     */
    @Deprecated
    public Promise<JsTable> getKeyTable() {
        if (keys == null) {
            keys = connection.newState((c, state, metadata) -> {
                SelectOrUpdateRequest view = new SelectOrUpdateRequest();
                view.setSourceId(baseTable.state().getHandle().makeTableReference());
                view.setResultId(state.getHandle().makeTicket());
                view.setColumnSpecsList(descriptor.getKeyColumnNamesList());
                connection.tableServiceClient().view(view, metadata, c::apply);
            }, "view only key columns")
                    .refetch(this, connection.metadata())
                    .then(state -> Promise.resolve(new JsTable(state.getConnection(), state)));
        }
        return keys.then(JsTable::copy);
    }

    /**
     * Fetch the underlying base table of the partitioned table.
     *
     * @return Promise of a Table
     */
    public Promise<JsTable> getBaseTable() {
        return baseTable.copy();
    }

    /** Close any subscriptions to underlying tables or key tables */
    private void closeSubscriptions() {
        if (baseTable != null) {
            baseTable.close();
        }
        if (keys != null) {
            keys.then(table -> {
                table.close();
                return Promise.resolve(table);
            });
        }
        if (subscription != null) {
            subscription.close();
        }
    }

    /**
     * Indicates that this PartitionedTable will no longer be used, removing subcriptions to updated keys, etc. This
     * will not affect tables in use.
     */
    public void close() {
        closeSubscriptions();

        widget.close();
    }

}
