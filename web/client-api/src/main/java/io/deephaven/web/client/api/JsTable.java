//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.flatbuffers.FlatBufferBuilder;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.JsArray;
import elemental2.promise.IThenable.ThenOnFulfilledCallbackFn;
import elemental2.promise.Promise;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.RollupRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.TreeRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.PartitionByRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.PartitionByResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.AggregateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.AsOfJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.BatchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ColumnStatisticsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.CrossJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.DropColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExactJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Literal;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.NaturalJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.RunChartDownsampleRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SeekRowRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SeekRowResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SelectDistinctRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SelectOrUpdateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SnapshotTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.SnapshotWhenTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.TableReference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.batchtablerequest.Operation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.runchartdownsamplerequest.ZoomRange;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.ticket_pb.TypedTicket;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.web.client.api.barrage.WebBarrageMessage;
import io.deephaven.web.client.api.barrage.WebBarrageMessageReader;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.data.WebBarrageSubscription;
import io.deephaven.web.client.api.barrage.def.TableAttributesDefinition;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.batch.RequestBatcher;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.input.JsInputTable;
import io.deephaven.web.client.api.lifecycle.HasLifecycle;
import io.deephaven.web.client.api.state.StateCache;
import io.deephaven.web.client.api.subscription.AbstractTableSubscription;
import io.deephaven.web.client.api.subscription.DataOptions;
import io.deephaven.web.client.api.subscription.SubscriptionType;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.api.subscription.ViewportData;
import io.deephaven.web.client.api.tree.JsRollupConfig;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.tree.JsTreeTableConfig;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.client.fu.JsItr;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.state.ActiveTableBinding;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.client.state.HasTableBinding;
import io.deephaven.web.shared.data.*;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsProvider;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.fu.RemoverFn;
import javaemul.internal.annotations.DoNotAutobox;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

import static io.deephaven.web.client.api.barrage.WebBarrageUtils.serializeRanges;
import static io.deephaven.web.client.fu.LazyPromise.logError;

/**
 * Provides access to data in a table. Note that several methods present their response through Promises. This allows
 * the client to both avoid actually connecting to the server until necessary, and also will permit some changes not to
 * inform the UI right away that they have taken place.
 */
@TsName(namespace = "dh", name = "Table")
public class JsTable extends HasLifecycle implements HasTableBinding, JoinableTable, ServerObject {
    /**
     * The table size has updated, so live scrollbars and the like can be updated accordingly.
     */
    @JsProperty(namespace = "dh.Table")
    public static final String EVENT_SIZECHANGED = "sizechanged",
            /**
             * {@code event.detail} is the currently visible window, the same as if {@code getViewportData} was called
             * and resolved. Listening to this event removes the need to listen to the finer grained events below for
             * data changes. In contrast, using the finer grained events may enable only updating the specific rows
             * which saw a change.
             */
            EVENT_UPDATED = "updated",
            /**
             * Finer grained visibility into data being added, rather than just seeing the currently visible viewport.
             * Provides the row being added, and the offset it will exist at.
             */
            EVENT_ROWADDED = "rowadded",
            /**
             * Finer grained visibility into data being removed, rather than just seeing the currently visible viewport.
             * Provides the row being removed, and the offset it used to exist at.
             */
            EVENT_ROWREMOVED = "rowremoved",
            /**
             * Finer grained visibility into data being updated, rather than just seeing the currently visible viewport.
             * Provides the row being updated and the offset it exists at.
             */
            EVENT_ROWUPDATED = "rowupdated",
            /**
             * Indicates that a sort has occurred, and that the UI should be replaced with the current viewport.
             */
            EVENT_SORTCHANGED = "sortchanged",
            /**
             * Indicates that a filter has occurred, and that the UI should be replaced with the current viewport.
             */
            EVENT_FILTERCHANGED = "filterchanged",
            /**
             * Indicates that columns for this table have changed, and column headers should be updated.
             */
            EVENT_CUSTOMCOLUMNSCHANGED = "customcolumnschanged",
            EVENT_DISCONNECT = "disconnect",
            EVENT_RECONNECT = "reconnect",
            EVENT_RECONNECTFAILED = "reconnectfailed",
            /**
             * Indicates that an error occurred on this table on the server or while communicating with it. The message
             * will provide more insight, but recent operations were likely unsuccessful and may need to be reapplied.
             */
            EVENT_REQUEST_FAILED = "requestfailed",
            EVENT_REQUEST_SUCCEEDED = "requestsucceeded";

    /**
     * The size the table will have if it is uncoalesced.
     */
    @JsProperty(namespace = "dh.Table")
    public static final double SIZE_UNCOALESCED = -2;

    // indicates that the CTS has changed, "downstream" tables should take note
    public static final String INTERNAL_EVENT_STATECHANGED = "statechanged-internal",
            // indicates that the "size listener" has gone off, thought possibly without a change in size, indicating a
            // change in some table data
            INTERNAL_EVENT_SIZELISTENER = "sizelistener-internal";

    public static final int MAX_BATCH_TIME = 600_000;

    private final WorkerConnection workerConnection;

    @Deprecated
    private final Map<TableTicket, TableViewportSubscription> subscriptions = new HashMap<>();

    private ClientTableState lastVisibleState;

    private ClientTableState currentState;

    private int batchDepth;

    private boolean hasInputTable;

    private boolean isBlinkTable;

    private final List<JsRunnable> onClosed;

    private double size = ClientTableState.SIZE_UNINITIALIZED;

    private final int subscriptionId;
    private static int nextSubscriptionId;

    /**
     * Creates a new {@code Table} directly from an existing {@code ClientTableState} (CTS). The CTS manages all fetch
     * operations, so this is just a simple constructor to get a table that points to the given state.
     */
    public JsTable(
            WorkerConnection workerConnection,
            ClientTableState state) {
        this.subscriptionId = nextSubscriptionId++;
        this.workerConnection = workerConnection;
        onClosed = new ArrayList<>();
        setState(state);
        setSize(state.getSize());
    }

    /**
     * Copy-constructor, used to build a new table instance based on the current handle/state of the current one,
     * allowing not only sharing state, but also actual handle and viewport subscriptions.
     *
     * @param table the original table to copy settings from
     */
    private JsTable(JsTable table) {
        this.subscriptionId = nextSubscriptionId++;
        this.workerConnection = table.workerConnection;
        this.hasInputTable = table.hasInputTable;
        this.isBlinkTable = table.isBlinkTable;
        this.currentState = table.currentState;
        this.lastVisibleState = table.lastVisibleState;
        this.size = table.size;
        onClosed = new ArrayList<>();
        table.getBinding().copyBinding(this);
    }

    /**
     * a {@code Sort} than can be used to reverse a table. This can be passed into n array in applySort. Note that Tree
     * Tables do not support {@code reverse}.
     * 
     * @return {@link Sort}
     */
    @JsMethod(namespace = "dh.Table")
    public static Sort reverse() {
        return Sort.reverse();
    }

    @Override
    public Promise<JsTable> refetch() {
        // TODO(deephaven-core#3604) consider supporting this method when new session reconnects are supported
        return Promise.reject("Cannot reconnect a Table with refetch(), see deephaven-core#3604");
    }

    @Override
    public TypedTicket typedTicket() {
        TypedTicket typedTicket = new TypedTicket();
        typedTicket.setTicket(state().getHandle().makeTicket());
        typedTicket.setType(JsVariableType.TABLE);
        return typedTicket;
    }

    @JsMethod
    public Promise<JsTable> batch(JsConsumer<RequestBatcher> userCode) {
        boolean rootBatch = batchDepth++ == 0;
        RequestBatcher batcher = workerConnection.getBatcher(this);

        if (!rootBatch) {
            batcher.finishOp();
        }
        userCode.apply(batcher);
        if (--batchDepth == 0) {
            return batcher.sendRequest()
                    .then(ignored -> Promise.resolve(JsTable.this));
        } else {
            return batcher.nestedPromise(this);
        }
    }

    /**
     * Retrieve a column by the given name. You should prefer to always retrieve a new {@code Column} instance instead
     * of caching a returned value.
     *
     * @param key
     * @return {@link Column}
     */
    @JsMethod
    public Column findColumn(String key) {
        return lastVisibleState().findColumn(key);
    }

    /**
     * Retrieve multiple columns specified by the given names.
     *
     * @param keys
     * @return {@link Column} array
     */
    @JsMethod
    public Column[] findColumns(String[] keys) {
        Column[] result = new Column[keys.length];
        for (int i = 0; i < keys.length; i++) {
            result[i] = findColumn(keys[i]);
        }
        return result;
    }

    /**
     * Returns the current state if active, or the last state which was active which represents the state we will return
     * to if an error occurs with the state we're presently waiting on. This lets user code access the last known table
     * size and columns.
     */
    public ClientTableState lastVisibleState() {
        // Try and get the running state first
        ActiveTableBinding binding = state().getActiveBinding(this);
        while (binding != null) {
            if (binding.getState().isRunning()) {
                return binding.getState();
            }
            binding = binding.getRollback();
        }

        // If we've disconnected and are reconnecting, we may not have an actively running state
        // Instead use the last state that we know was running
        assert lastVisibleState != null : "Table used before running";
        return lastVisibleState;
    }

    @Override
    public boolean isAlive() {
        return !isClosed();
    }

    @Override
    public ClientTableState state() {
        if (currentState == null) {
            throw new IllegalStateException("Table already closed, cannot be used again");
        }
        return currentState;
    }

    /**
     * {@code true} if this table represents a user Input Table (created by {@code InputTable.newInputTable}). When
     * {@code true}, you may call {@code .inputTable()} to add or remove data from the underlying table.
     * 
     * @return boolean
     */
    @JsProperty(name = "hasInputTable")
    public boolean hasInputTable() {
        return hasInputTable;
    }

    @JsMethod
    public boolean isBlinkTable() {
        return isBlinkTable;
    }

    /**
     * If {@code .hasInputTable} is {@code true}, you may call this method to gain access to an {@code InputTable}
     * object which can be used to mutate the data within the table. If the table is not an Input Table, the promise
     * will be immediately rejected.
     *
     * @return Promise of {@code dh.InputTable}
     */
    @JsMethod
    public Promise<JsInputTable> inputTable() {
        if (!hasInputTable) {
            return Js.uncheckedCast(Promise.reject("Table is not an InputTable"));
        }
        String[] keyCols = new String[0];
        String[] valueCols = new String[0];
        for (int i = 0; i < getColumns().length; i++) {
            final Column column = getColumns().getAt(i);
            if (column.isInputTableKeyColumn()) {
                keyCols[keyCols.length] = column.getName();
            } else if (column.isInputTableValueColumn()) {
                valueCols[valueCols.length] = column.getName();
            }
        }
        return Promise.resolve(new JsInputTable(this, keyCols, valueCols));
    }

    /**
     * Indicates that this {@code Table} instance will no longer be used, and its connection to the server can be
     * cleaned up.
     */
    @JsMethod
    public void close() {
        if (currentState == null) {
            // deliberately avoiding JsLog so that it shows up (with stack trace) in developer's console
            JsLog.warn("Table.close() called twice, second call being ignored", this);
            return;
        }
        onClosed.forEach(JsRunnable::run);
        onClosed.clear();

        currentState.pause(this);
        for (ClientTableState s : currentState.reversed()) {
            s.releaseTable(this);
        }
        // make this table unusable.
        currentState = null;
        // LATER: add more cleanup / assertions to aggressively enable GC

        subscriptions.values().forEach(TableViewportSubscription::internalClose);
        subscriptions.clear();
    }

    @JsMethod
    public String[] getAttributes() {
        TableAttributesDefinition attrs = lastVisibleState().getTableDef().getAttributes();
        return Stream.concat(
                Arrays.stream(attrs.getKeys()),
                attrs.getRemainingAttributeKeys().stream()).toArray(String[]::new);
    }

    /**
     * {@code null} if no property exists, a string if it is an easily serializable property, or a {@code Promise
     * &lt;Table&gt;} that will either resolve with a table or error out if the object can't be passed to JS.
     * 
     * @param attributeName
     * @return Object
     */
    @JsMethod
    @JsNullable
    public Object getAttribute(String attributeName) {
        TableAttributesDefinition attrs = lastVisibleState().getTableDef().getAttributes();
        // If the value was present as something easy to serialize, return it.
        String value = attrs.getValue(attributeName);
        if (value != null) {
            return value;
        }

        // Else check to see if it was present in the remaining keys (things that werent serialized).
        // This shouldn't be used to detect the absence of an attribute, use getAttributes() for that
        if (!attrs.getRemainingAttributeKeys().contains(attributeName)) {
            return null;
        }

        // Finally, assume that this is a table value, since we won't have any other way to serialze
        // some other type. If this isn't correct, the server will fail and we'll fail the promise.
        return workerConnection.newState((c, cts, metadata) -> {
            // workerConnection.getServer().fetchTableAttributeAsTable(
            // state().getHandle(),
            // cts.getHandle(),
            // attributeName,
            // c
            // );
            throw new UnsupportedOperationException("getAttribute");
        },
                "reading table from attribute with name " + attributeName)
                .refetch(this, workerConnection.metadata())
                .then(cts -> Promise.resolve(new JsTable(workerConnection, cts)));
    }

    // TODO: make these use Promise, so that if the tables list is only partially resolved,
    // we can force the calling client to wait appropriately (that or we throw errors / log warnings
    // when attempting to read columns / size / etc before the tables list is fully resolved)

    /**
     * The columns that are present on this table. This is always all possible columns. If you specify fewer columns in
     * {@code .setViewport()}, you will get only those columns in your {@code ViewportData}. {@code size} is the total
     * count of rows in the table. The size can and will change; see the {@code sizechanged} event for details. Size
     * will be negative in exceptional cases (eg. the table is uncoalesced, see the {@code isUncoalesced} property for
     * details).
     * 
     * @return {@link Column} array
     */
    @JsProperty
    public JsArray<Column> getColumns() {
        return Js.uncheckedCast(lastVisibleState().getColumns());
    }

    @JsProperty
    @JsNullable
    public JsLayoutHints getLayoutHints() {
        return lastVisibleState().getLayoutHints();
    }

    /**
     * The total count of rows in the table. If there is a viewport subscription active, this size will be updated when
     * the subscription updates. If not, and {@link #isUncoalesced()} is true, the size will be
     * {@link #SIZE_UNCOALESCED}. Otherwise, the size will be updated when the server's update graph processes changes.
     * <p>
     * When the size changes, the {@link #EVENT_SIZECHANGED} event will be fired.
     *
     * @return the size of the table, or {@link #SIZE_UNCOALESCED} if there is no subscription and the table is
     *         uncoalesced.
     */
    @JsProperty
    public double getSize() {
        TableViewportSubscription subscription = subscriptions.get(getHandle());
        if (subscription != null && subscription.hasValidSize()) {
            // only ask the viewport for the size if it is alive and ticking
            return subscription.size();
        }
        if (isUncoalesced()) {
            return JsTable.SIZE_UNCOALESCED;
        }
        // Only return the size from ETUM if we have no other choice
        return size;
    }

    @JsProperty
    @JsNullable
    public String getDescription() {
        return lastVisibleState().getTableDef().getAttributes().getDescription();
    }

    /**
     * The total count of the rows in the table, excluding any filters. Unlike {@link #getSize()}, changes to this value
     * will not result in any event. If the table is unfiltered, this will return the same size as {@link #getSize()}.
     * If this table was uncoalesced before it was filtered, this will return {@link #SIZE_UNCOALESCED}.
     * 
     * @return the size of the table before filters, or {@link #SIZE_UNCOALESCED}
     */
    @JsProperty
    public double getTotalSize() {
        if (state().getFilters().isEmpty()) {
            // If there are no filters, use the subscription size (if any)
            return getSize();
        }
        return getHeadState().getSize();
    }

    /**
     * An ordered list of {@link Sort}s to apply to the table. To update, call {@link #applySort(Sort[])}. Note that
     * this getter will return the new value immediately, even though it may take a little time to update on the server.
     * You may listen for the <b>sortchanged</b> event to know when to update the UI.
     * 
     * @return {@link Sort} array
     */
    @JsProperty
    public JsArray<Sort> getSort() {
        return JsItr.slice(state().getSorts());
    }

    /**
     * An ordered list of filter conditions to apply to the table. To update, call
     * {@link #applyFilter(FilterCondition[])}. Note that this getter will return the new value immediately, even though
     * it may take a little time to update on the server. You may listen for the {@code filterchanged} event to know
     * when to update the UI.
     * 
     * @return {@link FilterCondition} array
     */
    @JsProperty
    public JsArray<FilterCondition> getFilter() {
        return JsItr.slice(state().getFilters());
    }

    /**
     * Replace the currently set sort on this table. Returns the previously set value. Note that the sort property will
     * immediately return the new value, but you may receive update events using the old sort before the new sort is
     * applied, and the {@code sortchanged} event fires. Reusing existing, applied sorts may enable this to perform
     * better on the server. The {@code updated} event will also fire, but {@code rowadded} and {@code rowremoved} will
     * not.
     *
     * @param sort
     * @return {@link Sort} array
     */
    @JsMethod
    @SuppressWarnings("unusable-by-js")
    public JsArray<Sort> applySort(Sort[] sort) {
        List<Sort> newSort = Arrays.asList(sort);
        // when replacing sorts, just blindly remove all sorts,
        // condensing any orphaned filter / custom columns, and adding the given sorts.

        // take a look at the current sort so we can return it
        final ClientTableState current = state();
        List<Sort> currentSort = current.getSorts();
        // try to skip work (TODO refactor this into a private method)
        if (!currentSort.equals(newSort)) {
            if (batchDepth > 0) {
                // when explicitly in batch mode, just record what the user requested,
                // but don't compute anything until the batch is complete.
                workerConnection.getBatcher(this).sort(newSort);
            } else {
                // we use the batch mechanism to handle unwinding / building table states
                batch(batcher -> {
                    batcher.customColumns(current.getCustomColumns());
                    batcher.filter(current.getFilters());
                    batcher.sort(newSort);
                }).catch_(logError(() -> "Failed to apply sorts: " + Arrays.toString(sort)));
            }
        }

        return JsItr.slice(currentSort);
    }

    /**
     * Replace the currently set filters on the table. Returns the previously set value. Note that the filter property
     * will immediately return the new value, but you may receive update events using the old filter before the new one
     * is applied, and the {@code filterchanged} event fires. Reusing existing, applied filters may enable this to
     * perform better on the server. The {@code updated} event will also fire, but {@code rowadded} and
     * {@code rowremoved} will not.
     *
     * @param filter
     * @return {@link FilterCondition} array
     */
    @JsMethod
    @SuppressWarnings("unusable-by-js")
    public JsArray<FilterCondition> applyFilter(FilterCondition[] filter) {

        final List<FilterCondition> newFilter = Arrays.asList(filter);
        // take a look at the current filter so we can return it
        final ClientTableState current = state();
        List<FilterCondition> currentFilter = current.getFilters();
        // try to skip work (TODO refactor this into a private method)
        if (!currentFilter.equals(newFilter)) {
            if (batchDepth > 0) {
                // when batching, just record what the user requested, but don't compute anything
                // until the batch is complete.
                workerConnection.getBatcher(this).filter(newFilter);
            } else {
                // we use the batch mechanism to handle unwinding / building table states
                batch(batcher -> {
                    batcher.customColumns(current.getCustomColumns());
                    batcher.filter(newFilter);
                    batcher.sort(current.getSorts());
                }).catch_(logError(() -> "Failed to apply filters: " + Arrays.toString(filter)));
            }
        }

        return JsItr.slice(currentFilter);
    }

    @TsUnion
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    public interface CustomColumnArgUnionType {
        @JsOverlay
        static CustomColumnArgUnionType of(@DoNotAutobox Object value) {
            return Js.cast(value);
        }

        @JsOverlay
        default boolean isString() {
            return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isCustomColumn() {
            return (Object) this instanceof CustomColumn;
        }

        @JsOverlay
        @TsUnionMember
        default String asString() {
            return Js.asString(this);
        }

        @JsOverlay
        @TsUnionMember
        default CustomColumn asCustomColumn() {
            return Js.cast(this);
        }
    }

    /**
     * Used when adding new filter and sort operations to the table, as long as they are present.
     *
     * @param customColumns
     * @return {@link CustomColumn} array
     */
    @JsMethod
    public JsArray<CustomColumn> applyCustomColumns(JsArray<CustomColumnArgUnionType> customColumns) {
        String[] customColumnStrings = customColumns.map((item, index) -> {
            if (item.isString() || item.isCustomColumn()) {
                return item.toString();
            }

            return (new CustomColumn((JsPropertyMap<Object>) item)).toString();
        }).asArray(new String[0]);

        final List<CustomColumnDescriptor> newCustomColumns = CustomColumnDescriptor.from(customColumnStrings);

        // take a look at the current custom columns so we can return it
        final ClientTableState current = state();
        List<CustomColumnDescriptor> currentCustomColumns = current.getCustomColumns();
        final List<CustomColumn> returnMe = current.getCustomColumnsObject();
        if (!currentCustomColumns.equals(newCustomColumns)) {
            if (batchDepth > 0) {
                // when batching, just record what the user requested, but don't compute anything
                // until the batch is complete.
                workerConnection.getBatcher(this).customColumns(newCustomColumns);
            } else {
                // we use the batch mechanism to handle unwinding / building table states
                batch(batcher -> {
                    batcher.customColumns(newCustomColumns);
                    batcher.filter(current.getFilters());
                    batcher.sort(current.getSorts());
                }).catch_(logError(() -> "Failed to apply custom columns: " + customColumns));

            }
        }

        return JsItr.slice(returnMe);
    }

    /**
     * An ordered list of custom column formulas to add to the table, either adding new columns or replacing existing
     * ones. To update, call {@link #applyCustomColumns(JsArray)}.
     * 
     * @return {@link CustomColumn} array
     *
     */
    @JsProperty
    public JsArray<CustomColumn> getCustomColumns() {
        return Js.cast(JsItr.slice(state().getCustomColumnsObject()));
    }

    /**
     * Overload for Java (since JS just omits the optional params).
     */
    public TableViewportSubscription setViewport(double firstRow, double lastRow) {
        return setViewport(firstRow, lastRow, null, null, null);
    }

    /**
     * Overload for Java (since JS just omits the optional param).
     */
    public TableViewportSubscription setViewport(double firstRow, double lastRow, JsArray<Column> columns) {
        return setViewport(firstRow, lastRow, columns, null, null);
    }

    /**
     * If the columns parameter is not provided, all columns will be used. If the {@code updateIntervalMs} parameter is
     * not provided, a default of one second will be used. Until this is called, no data will be available. Invoking
     * this will result in events to be fired once data becomes available, starting with an {@code updated} event and a
     * {@code rowadded} event per row in that range. The returned object allows the viewport to be closed when no longer
     * needed.
     *
     * @param firstRow
     * @param lastRow
     * @param columns
     * @param updateIntervalMs
     * @return {@link TableViewportSubscription}
     * @deprecated Use {@link #createViewportSubscription(Object)} instead.
     */
    @JsMethod
    @Deprecated
    public TableViewportSubscription setViewport(double firstRow, double lastRow,
            @JsOptional @JsNullable JsArray<Column> columns,
            @JsOptional @JsNullable Double updateIntervalMs,
            @JsOptional @JsNullable Boolean isReverseViewport) {
        ClientTableState currentState = state();
        Column[] columnsCopy = columns != null ? Js.uncheckedCast(columns.slice()) : currentState.getColumns();
        TableViewportSubscription activeSubscription = subscriptions.get(getHandle());
        if (activeSubscription != null && !activeSubscription.isClosed()) {
            // hasn't finished, lets reuse it
            activeSubscription.setInternalViewport(RangeSet.ofRange((long) firstRow, (long) lastRow), columnsCopy,
                    updateIntervalMs, isReverseViewport);
            return activeSubscription;
        } else {
            // In the past, we left the old sub going until the new one was ready, then started the new one. But now,
            // we want to reference the old or the new as appropriate - until the new state is running, we keep pumping
            // the old one, then cross over once we're able.

            // We're not responsible here for shutting down the old one here - setState will do that after the new one
            // is running.

            // rewrap current state in a new one, when ready the viewport will be applied
            DataOptions.ViewportSubscriptionOptions options = new DataOptions.ViewportSubscriptionOptions();
            options.previewOptions = new DataOptions.PreviewOptions();
            options.previewOptions.convertArrayToString = true;
            options.rows = Js.uncheckedCast(new JsRangeSet(RangeSet.ofRange((long) firstRow, (long) lastRow)));
            options.columns = Js.uncheckedCast(columnsCopy);
            options.updateIntervalMs = updateIntervalMs;
            options.isReverseViewport = isReverseViewport;
            TableViewportSubscription replacement = TableViewportSubscription.make(options, this);

            subscriptions.put(currentState.getHandle(), replacement);
            return replacement;
        }
    }

    /**
     * Gets the currently visible viewport. If the current set of operations has not yet resulted in data, it will not
     * resolve until that data is ready. If this table is closed before the promise resolves, it will be rejected - to
     * separate the lifespan of this promise from the table itself, call
     * {@link TableViewportSubscription#getViewportData()} on the result from {@link #setViewport(double, double)}.
     *
     * @return Promise of {@link TableData}
     * @deprecated use {@link TableViewportSubscription#getViewportData()} on the result from
     *             {@link #createViewportSubscription(Object)} instead.
     */
    @JsMethod
    @Deprecated
    public Promise<AbstractTableSubscription.@TsTypeRef(ViewportData.class) UpdateEventData> getViewportData() {
        TableViewportSubscription subscription = subscriptions.get(getHandle());
        if (subscription == null) {
            return Promise.reject("No viewport currently set");
        }
        return subscription.getInternalViewportData();
    }

    /**
     * Overload for java (since js just omits the optional var)
     */
    public TableSubscription subscribe(JsArray<Column> columns) {
        return subscribe(columns, null);
    }

    /**
     * Creates a subscription to the specified columns, across all rows in the table. The optional parameter
     * {@code updateIntervalMs} may be specified to indicate how often the server should send updates, defaulting to one
     * second if omitted. Useful for charts or taking a snapshot of the table atomically. The initial snapshot will
     * arrive in a single event, but later changes will be sent as updates. However, this may still be very expensive to
     * run from a browser for very large tables. Each call to subscribe creates a new subscription, which must have
     * {@link TableSubscription#close()} called on it to stop it, and all events are fired from the
     * {@link TableSubscription} instance.
     *
     * @param columns
     * @param updateIntervalMs
     * @return {@link TableSubscription}
     * @deprecated Use {@link #createSubscription(Object)} with a {@link DataOptions.SubscriptionOptions} instead.
     */
    @JsMethod
    @Deprecated
    public TableSubscription subscribe(JsArray<Column> columns, @JsOptional Double updateIntervalMs) {
        DataOptions.SubscriptionOptions options = new DataOptions.SubscriptionOptions();
        options.previewOptions = new DataOptions.PreviewOptions();
        options.previewOptions.convertArrayToString = true;
        options.columns = columns;
        options.updateIntervalMs = updateIntervalMs;
        return TableSubscription.createTableSubscription(options, this);
    }

    /**
     * Creates a subscription to the specified columns, across all rows in the table. Useful for charts or taking a
     * snapshot of the table atomically. The initial snapshot will arrive in a single event, but later changes will be
     * sent as updates. However, this may still be very expensive to run from a browser for very large tables. Each call
     * to {@code createSubscription} creates a new subscription, which must have {@link TableSubscription#close()}
     * called on it to stop it and release its resources, and all events are fired from the {@link TableSubscription}
     * instance.
     * 
     * @param options options for the subscription; see {@link DataOptions.SubscriptionOptions} for details
     * @return a new {@link TableSubscription}
     */
    @JsMethod
    public TableSubscription createSubscription(@TsTypeRef(DataOptions.SubscriptionOptions.class) Object options) {
        return TableSubscription.createTableSubscription(DataOptions.SubscriptionOptions.of(options), this);
    }

    /**
     * Creates a viewport subscription to the specified columns, across the specified rows in the table. The returned
     * {@link TableViewportSubscription} instance allows the viewport to be changed over time, and events are fired from
     * it when the data changes or when a viewport change has been applied. Each call to
     * {@code createViewportSubscription} creates a new subscription, which must have
     * {@link TableViewportSubscription#close()} called on it to stop it and release its resources
     *
     * @param options options for the viewport subscription; see {@link DataOptions.ViewportSubscriptionOptions} for
     *        details
     * @return a new {@link TableViewportSubscription}
     */
    @JsMethod
    public TableViewportSubscription createViewportSubscription(
            @TsTypeRef(DataOptions.ViewportSubscriptionOptions.class) Object options) {
        DataOptions.ViewportSubscriptionOptions copy = DataOptions.ViewportSubscriptionOptions.of(options);
        if (copy.columns == null) {
            throw new IllegalArgumentException("Missing 'columns' property in viewport subscription options");
        }
        return TableViewportSubscription.make(copy, this);
    }


    /**
     * Returns a promise that will resolve to a {@link TableData} instance containing a snapshot of the current state of
     * the table, within the bounds of the specified rows and columns.
     *
     * @param options options for the snapshot; see {@link DataOptions.SnapshotOptions} for details
     * @return Promise of {@link TableData}
     */
    @JsMethod
    public Promise<TableData> createSnapshot(@TsTypeRef(DataOptions.SnapshotOptions.class) Object options) {
        DataOptions.SnapshotOptions snapshotOptions = DataOptions.SnapshotOptions.of(options);
        JsArray<Column> columns = snapshotOptions.columns;
        RangeSet rows = snapshotOptions.rows.asRangeSet().getRange();

        // TODO #1039 slice rows and drop columns
        int previewListLengthLimit = AbstractTableSubscription.getPreviewListLengthLimit(snapshotOptions);
        BarrageSnapshotOptions barrageSnapshotOptions = BarrageSnapshotOptions.builder()
                .batchSize(WebBarrageSubscription.BATCH_SIZE)
                .maxMessageSize(WebBarrageSubscription.MAX_MESSAGE_SIZE)
                .useDeephavenNulls(true)
                .previewListLengthLimit(previewListLengthLimit)
                .build();

        ClientTableState previewed =
                AbstractTableSubscription.createPreview(workerConnection, state(), snapshotOptions.previewOptions);

        LazyPromise<TableData> promise = new LazyPromise<>();
        previewed.onRunning(cts -> {
            int rowStyleColumn = cts.getRowFormatColumn() == null ? TableData.NO_ROW_FORMAT_COLUMN
                    : cts.getRowFormatColumn().getIndex();

            WebBarrageSubscription snapshot = WebBarrageSubscription.subscribe(
                    SubscriptionType.SNAPSHOT, cts,
                    (serverViewport1, serverColumns, serverReverseViewport) -> {
                    },
                    (rowsAdded, rowsRemoved, totalMods, shifted, modifiedColumnSet) -> {
                    });

            WebBarrageMessageReader reader = new WebBarrageMessageReader();

            BiDiStream<FlightData, FlightData> doExchange =
                    workerConnection.<FlightData, FlightData>streamFactory().create(
                            headers -> workerConnection.flightServiceClient().doExchange(headers),
                            (first, headers) -> workerConnection.browserFlightServiceClient().openDoExchange(first,
                                    headers),
                            (next, headers, c) -> workerConnection.browserFlightServiceClient().nextDoExchange(next,
                                    headers,
                                    c::apply),
                            new FlightData());
            MutableLong rowsReceived = new MutableLong(0);
            doExchange.onData(data -> {
                WebBarrageMessage message;
                try {
                    message = reader.parseFrom(barrageSnapshotOptions, cts.columnTypes(),
                            cts.componentTypes(), data);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if (message != null) {
                    // Replace rowsets with flat versions
                    long resultSize = message.rowsIncluded.size();
                    if (resultSize != 0) {
                        message.rowsAdded = RangeSet.ofRange(rowsReceived.get(), rowsReceived.get() + resultSize - 1);
                        message.rowsIncluded = message.rowsAdded;
                        message.snapshotRowSet = null;
                        rowsReceived.add(resultSize);
                    }

                    // Update our table data with the complete message
                    snapshot.applyUpdates(message);
                }
            });
            FlightData payload = new FlightData();
            final FlatBufferBuilder metadata = new FlatBufferBuilder();

            int colOffset = BarrageSnapshotRequest.createColumnsVector(metadata,
                    cts.makeBitset(Js.uncheckedCast(columns)).toByteArray());
            int vpOffset =
                    BarrageSnapshotRequest.createViewportVector(metadata, serializeRanges(Collections.singleton(rows)));
            int optOffset = barrageSnapshotOptions.appendTo(metadata);

            final int ticOffset = BarrageSnapshotRequest.createTicketVector(metadata,
                    Js.<byte[]>uncheckedCast(cts.getHandle().getTicket()));
            BarrageSnapshotRequest.startBarrageSnapshotRequest(metadata);
            BarrageSnapshotRequest.addColumns(metadata, colOffset);
            BarrageSnapshotRequest.addViewport(metadata, vpOffset);
            BarrageSnapshotRequest.addSnapshotOptions(metadata, optOffset);
            BarrageSnapshotRequest.addTicket(metadata, ticOffset);
            BarrageSnapshotRequest.addReverseViewport(metadata, false);
            metadata.finish(BarrageSnapshotRequest.endBarrageSnapshotRequest(metadata));

            payload.setAppMetadata(WebBarrageUtils.wrapMessage(metadata, BarrageMessageType.BarrageSnapshotRequest));
            doExchange.onEnd(status -> {
                if (status.isOk()) {
                    // notify the caller that the snapshot is finished
                    RangeSet result;
                    if (rowsReceived.get() != 0) {
                        result = RangeSet.ofRange(0, rowsReceived.get() - 1);
                    } else {
                        result = RangeSet.empty();
                    }

                    promise.succeed(new AbstractTableSubscription.SubscriptionEventData(snapshot, rowStyleColumn,
                            Js.uncheckedCast(columns),
                            result,
                            RangeSet.empty(),
                            RangeSet.empty(),
                            null));
                } else {
                    promise.fail(status);
                }
            });

            doExchange.send(payload);
            doExchange.end();

        }, promise::fail, () -> promise.fail("Table was closed"));
        return promise.asPromise();
    }

    /**
     * A new table containing the distinct tuples of values from the given columns that are present in the original
     * table. This table can be manipulated as any other table. Sorting is often desired as the default sort is the
     * order of appearance of values from the original table.
     * 
     * @param columns
     * @return Promise of dh.Table
     */
    @JsMethod
    public Promise<JsTable> selectDistinct(Column[] columns) {
        final ClientTableState state = state();
        // We are going to forget all configuration for the current state
        // by just creating a new, fresh state. This should be an optional flatten()/copy() step instead.
        String[] columnNames = Arrays.stream(columns).map(Column::getName).toArray(String[]::new);
        final ClientTableState distinct = workerConnection.newState((c, cts, metadata) -> {
            SelectDistinctRequest request = new SelectDistinctRequest();
            request.setSourceId(state.getHandle().makeTableReference());
            request.setResultId(cts.getHandle().makeTicket());
            request.setColumnNamesList(columnNames);
            workerConnection.tableServiceClient().selectDistinct(request, metadata, c::apply);
        },
                "selectDistinct " + Arrays.toString(columnNames));
        return distinct.refetch(this, workerConnection.metadata())
                .then(cts -> Promise.resolve(new JsTable(workerConnection, cts)));
    }

    /**
     * Creates a new copy of this table, so it can be sorted and filtered separately, and maintain a different viewport.
     * 
     * @return Promise of dh.Table
     *
     */
    @JsMethod
    public Promise<JsTable> copy() {
        return Promise.resolve(new JsTable(this));
    }

    public Promise<JsTable> copy(boolean resolved) {
        if (resolved) {
            LazyPromise<ClientTableState> promise = new LazyPromise<>();
            final ClientTableState unresolved = state();
            unresolved.onRunning(promise::succeed, promise::fail,
                    () -> promise.fail("Table failed or closed, copy could not complete"));
            return promise.asPromise(MAX_BATCH_TIME)
                    .then(s -> Promise.resolve(new JsTable(this)));
        }
        return copy();
    }

    /**
     * A promise that will resolve to a Totals Table of this table. This table will obey the configurations provided as
     * a parameter, or will use the table's default if no parameter is provided, and be updated once per second as
     * necessary. Note that multiple calls to this method will each produce a new {@code TotalsTable} which must have
     * {@code close} called on it when not in use.
     * 
     * @param config
     * @return Promise of dh.TotalsTable
     */
    @JsMethod
    public Promise<JsTotalsTable> getTotalsTable(
            @JsOptional @JsNullable @TsTypeRef(JsTotalsTableConfig.class) Object config) {
        // fetch the handle and wrap it in a new jstable. listen for changes
        // on the parent table, and re-fetch each time.

        return fetchTotals(config, this::lastVisibleState);
    }

    /**
     * The default configuration to be used when building a {@code TotalsTable} for this table.
     * 
     * @return dh.TotalsTableConfig
     */
    @JsProperty
    public JsTotalsTableConfig getTotalsTableConfig() {
        // we want to communicate to the JS dev that there is no default config, so we allow
        // returning null here, rather than a default config. They can then easily build a
        // default config, but without this ability, there is no way to indicate that the
        // config omitted a totals table
        return lastVisibleState().getTotalsTableConfig();
    }

    private Promise<JsTotalsTable> fetchTotals(Object config, JsProvider<ClientTableState> state) {
        JsTotalsTableConfig directive = getTotalsDirectiveFromOptionalConfig(config);
        ClientTableState[] lastGood = {null};
        final JsTableFetch totalsFactory = (callback, newState, metadata) -> {
            final ClientTableState target;
            // we know this will get called at least once, immediately, so lastGood will never be null
            if (isClosed()) {
                // source table was closed, we have to rely on lastGood...
                target = lastGood[0];
            } else {
                target = state.valueOf();
                // make sure we are only retained by one state at a time
                // TODO: refactor subscription system to handle non-JsTable subscriptions w/ same one:one semantics,
                target.retain(directive);
                if (lastGood[0] != null && lastGood[0] != target) {
                    lastGood[0].unretain(directive);
                }
                lastGood[0] = target;
            }
            JsLog.debug("Sending totals table fetch ", directive, " for ", target,
                    "(", LazyString.of(target::getHandle), "), into ", LazyString.of(newState::getHandle), "(",
                    newState, ")");

            AggregateRequest requestMessage = directive.buildRequest(getColumns());
            JsArray<String> updateViewExprs = directive.getCustomColumns();
            JsArray<String> dropColumns = directive.getDropColumns();
            requestMessage.setSourceId(target.getHandle().makeTableReference());
            requestMessage.setResultId(newState.getHandle().makeTicket());
            if (updateViewExprs.length != 0) {
                SelectOrUpdateRequest columnExpr = new SelectOrUpdateRequest();
                columnExpr.setResultId(requestMessage.getResultId());
                requestMessage.setResultId();
                columnExpr.setColumnSpecsList(updateViewExprs);
                columnExpr.setSourceId(new TableReference());
                columnExpr.getSourceId().setBatchOffset(0);
                BatchTableRequest batch = new BatchTableRequest();
                Operation aggOp = new Operation();
                aggOp.setAggregate(requestMessage);
                Operation colsOp = new Operation();
                colsOp.setUpdateView(columnExpr);
                batch.addOps(aggOp);
                batch.addOps(colsOp);
                if (dropColumns.length != 0) {
                    DropColumnsRequest drop = new DropColumnsRequest();
                    drop.setColumnNamesList(dropColumns);
                    drop.setResultId(columnExpr.getResultId());
                    columnExpr.setResultId();
                    drop.setSourceId(new TableReference());
                    drop.getSourceId().setBatchOffset(1);

                    Operation dropOp = new Operation();
                    dropOp.setDropColumns(drop);
                    batch.addOps(dropOp);
                }
                ResponseStreamWrapper<ExportedTableCreationResponse> stream = ResponseStreamWrapper
                        .of(workerConnection.tableServiceClient().batch(batch, workerConnection.metadata()));
                stream.onData(creationResponse -> {
                    if (creationResponse.getResultId().hasTicket()) {
                        // represents the final output
                        callback.apply(null, creationResponse);
                    }
                });
                stream.onEnd(status -> {
                    if (!status.isOk()) {
                        callback.apply(status, null);
                    }
                });
            } else {
                workerConnection.tableServiceClient().aggregate(requestMessage, workerConnection.metadata(),
                        callback::apply);
            }
        };
        String summary = "totals table " + directive + ", " + directive.groupBy.join(",");
        final ClientTableState totals = workerConnection.newState(totalsFactory, summary);
        final LazyPromise<JsTotalsTable> result = new LazyPromise<>();
        boolean[] downsample = {true};
        return totals.refetch(this, workerConnection.metadata()) // lastGood will always be non-null after this
                .then(ready -> {
                    JsTable wrapped = new JsTable(workerConnection, ready);
                    // technically this is overkill, but it is more future-proofed than only listening for column
                    // changes
                    final RemoverFn remover = addEventListener(
                            INTERNAL_EVENT_STATECHANGED,
                            e -> {
                                // eat superfluous changes (wait until event loop settles before firing requests).
                                // IDS-2684 If you disable downsampling, you can lock up the entire websocket with some
                                // rapid
                                // table-state-changes that trigger downstream totals table changes.
                                // It probably makes more sense to move this downsampling to the internal event,
                                // or expose a public event that is already downsampled by a more sophisticated latch.
                                // (for example, a batch that can outlive a single event loop by using an internal table
                                // copy()
                                // which simply accrues state until the user decides to commit the modification).
                                if (downsample[0]) {
                                    downsample[0] = false;
                                    LazyPromise.runLater(() -> {
                                        if (wrapped.isClosed()) {
                                            return;
                                        }
                                        downsample[0] = true;
                                        // IDS-2684 - comment out the four lines above to reproduce
                                        // when ever the main table changes its state, reload the totals table from the
                                        // new state
                                        final ClientTableState existing = wrapped.state();
                                        final ClientTableState nextState =
                                                workerConnection.newState(totalsFactory, summary);
                                        JsLog.debug("Rebasing totals table", existing, " -> ", nextState, " for ",
                                                wrapped);
                                        wrapped.setState(nextState);
                                        // If the wrapped table's state has changed (any filter / sort / columns
                                        // applied),
                                        // then we'll want to re-apply these conditions on top of the newly set state.
                                        final boolean needsMutation = !existing.isEqual(ready);

                                        final ThenOnFulfilledCallbackFn restoreVp = running -> {
                                            // now that we've (possibly) updated selection conditions, put back in any
                                            // viewport.
                                            result.onSuccess(JsTotalsTable::refreshViewport);
                                            return null;
                                        };
                                        final Promise<ClientTableState> promise =
                                                nextState.refetch(this, workerConnection.metadata());
                                        if (needsMutation) { // nextState will be empty, so we might want to test for
                                                             // isEmpty() instead
                                            wrapped.batch(b -> b.setConfig(existing)).then(restoreVp);
                                        } else {
                                            promise.then(restoreVp);
                                        }
                                        // IDS-2684 - Comment out the two lines below to reproduce
                                    });
                                }
                            });
                    wrapped.onClosed.add(remover::remove);
                    wrapped.onClosed.add(() -> lastGood[0].unretain(directive));
                    onClosed.add(remover::remove);
                    final JsTotalsTable totalsTable =
                            new JsTotalsTable(wrapped, directive.serialize(), directive.groupBy);
                    result.succeed(totalsTable);
                    return result.asPromise();
                });
    }

    private JsTotalsTableConfig getTotalsDirectiveFromOptionalConfig(Object config) {
        if (config == null) {
            return JsTotalsTableConfig.parse(lastVisibleState().getTableDef().getAttributes().getTotalsTableConfig());
        } else {
            if (config instanceof JsTotalsTableConfig) {
                return (JsTotalsTableConfig) config;
            } else if (config instanceof String) {
                return JsTotalsTableConfig.parse((String) config);
            } else {
                return new JsTotalsTableConfig((JsPropertyMap<Object>) config);
            }
        }
    }

    /**
     * A promise that will resolve to a Totals Table of this table, ignoring any filters. See {@code getTotalsTable}
     * above for more specifics.
     * 
     * @param config
     * @return promise of dh.TotalsTable
     */
    @JsMethod
    public Promise<JsTotalsTable> getGrandTotalsTable(
            @JsOptional @JsNullable @TsTypeRef(JsTotalsTableConfig.class) Object config) {
        // As in getTotalsTable, but this time we want to skip any filters - this could mean use the
        // most-derived table which has no filter, or the least-derived table which has all custom columns.
        // Currently, these two mean the same thing.
        return fetchTotals(config, () -> {
            ClientTableState unfiltered = state();
            while (!unfiltered.getFilters().isEmpty()) {
                unfiltered = unfiltered.getPrevious();
                assert unfiltered != null : "no table is unfiltered, even base table!";
            }
            return unfiltered;
        });
    }

    /**
     * a promise that will resolve to a new roll-up {@code TreeTable} of this table. Multiple calls to this method will
     * each produce a new {@code TreeTable} which must have {@code close} called on it when not in use.
     * 
     * @param configObject
     * @return Promise of dh.TreeTable
     */
    @JsMethod
    public Promise<JsTreeTable> rollup(@TsTypeRef(JsRollupConfig.class) Object configObject) {
        Objects.requireNonNull(configObject, "Table.rollup configuration");
        final JsRollupConfig config;
        if (configObject instanceof JsRollupConfig) {
            config = (JsRollupConfig) configObject;
        } else {
            config = new JsRollupConfig(Js.cast(configObject));
        }

        Ticket rollupTicket = workerConnection.getTickets().newExportTicket();

        Promise<Object> rollupPromise = Callbacks.grpcUnaryPromise(c -> {
            RollupRequest request = config.buildRequest(getColumns());
            request.setSourceTableId(state().getHandle().makeTicket());
            request.setResultRollupTableId(rollupTicket);
            workerConnection.hierarchicalTableServiceClient().rollup(request, workerConnection.metadata(), c::apply);
        });

        TypedTicket typedTicket = new TypedTicket();
        typedTicket.setType(JsVariableType.HIERARCHICALTABLE);
        typedTicket.setTicket(rollupTicket);

        JsWidget widget = new JsWidget(workerConnection, typedTicket);

        return Promise.all(widget.refetch(), rollupPromise)
                .then(ignore -> Promise.resolve(new JsTreeTable(workerConnection, widget)));
    }

    /**
     * A promise that will resolve to a new {@code TreeTable} of this table. Multiple calls to this method will each
     * produce a new {@code TreeTable} which must have {@code close} called on it when not in use.
     * 
     * @param configObject
     * @return Promise dh.TreeTable
     */
    @JsMethod
    public Promise<JsTreeTable> treeTable(@TsTypeRef(JsTreeTableConfig.class) Object configObject) {
        Objects.requireNonNull(configObject, "Table.treeTable configuration");
        final JsTreeTableConfig config;
        if (configObject instanceof JsTreeTableConfig) {
            config = (JsTreeTableConfig) configObject;
        } else {
            config = new JsTreeTableConfig(Js.cast(configObject));
        }

        Ticket treeTicket = workerConnection.getTickets().newExportTicket();

        Promise<Object> treePromise = Callbacks.grpcUnaryPromise(c -> {
            TreeRequest requestMessage = new TreeRequest();
            requestMessage.setSourceTableId(state().getHandle().makeTicket());
            requestMessage.setResultTreeTableId(treeTicket);
            requestMessage.setIdentifierColumn(config.idColumn);
            requestMessage.setParentIdentifierColumn(config.parentColumn);
            requestMessage.setPromoteOrphans(config.promoteOrphansToRoot);

            workerConnection.hierarchicalTableServiceClient().tree(requestMessage, workerConnection.metadata(),
                    c::apply);
        });

        TypedTicket typedTicket = new TypedTicket();
        typedTicket.setType(JsVariableType.HIERARCHICALTABLE);
        typedTicket.setTicket(treeTicket);

        JsWidget widget = new JsWidget(workerConnection, typedTicket);

        return Promise.all(widget.refetch(), treePromise)
                .then(ignore -> Promise.resolve(new JsTreeTable(workerConnection, widget)));
    }

    /**
     * A "frozen" version of this table (a server-side snapshot of the entire source table). Viewports on the frozen
     * table will not update. This does not change the original table, and the new table will not have any of the client
     * side sorts/filters/columns. New client side sorts/filters/columns can be added to the frozen copy.
     *
     * @return Promise of dh.Table
     */
    @JsMethod
    public Promise<JsTable> freeze() {
        return workerConnection.newState((c, state, metadata) -> {
            SnapshotTableRequest request = new SnapshotTableRequest();
            request.setSourceId(state().getHandle().makeTableReference());
            request.setResultId(state.getHandle().makeTicket());
            workerConnection.tableServiceClient().snapshot(request, metadata, c::apply);
        }, "freeze").refetch(this, workerConnection.metadata())
                .then(state -> Promise.resolve(new JsTable(workerConnection, state)));
    }

    @Override
    @JsMethod
    public Promise<JsTable> snapshot(JsTable baseTable, @JsOptional Boolean doInitialSnapshot,
            @JsOptional String[] stampColumns) {
        Objects.requireNonNull(baseTable, "Snapshot base table");
        final boolean realDoInitialSnapshot;
        if (doInitialSnapshot != null) {
            realDoInitialSnapshot = doInitialSnapshot;
        } else {
            realDoInitialSnapshot = true;
        }
        final String[] realStampColumns;
        if (stampColumns == null) {
            realStampColumns = new String[0]; // server doesn't like null
        } else {
            // make sure we pass an actual string array
            realStampColumns = Arrays.stream(stampColumns).toArray(String[]::new);
        }
        final String fetchSummary =
                "snapshot(" + baseTable + ", " + doInitialSnapshot + ", " + Arrays.toString(stampColumns) + ")";
        return workerConnection.newState((c, state, metadata) -> {
            SnapshotWhenTableRequest request = new SnapshotWhenTableRequest();
            request.setBaseId(baseTable.state().getHandle().makeTableReference());
            request.setTriggerId(state().getHandle().makeTableReference());
            request.setResultId(state.getHandle().makeTicket());
            request.setInitial(realDoInitialSnapshot);
            request.setStampColumnsList(realStampColumns);

            workerConnection.tableServiceClient().snapshotWhen(request, metadata, c::apply);
        }, fetchSummary).refetch(this, workerConnection.metadata())
                .then(state -> Promise.resolve(new JsTable(workerConnection, state)));
    }

    // inheritDoc lets us implement the inherited method, but still keep docs for TS
    /**
     * @inheritDoc
     */
    @Override
    @JsMethod
    @Deprecated
    public Promise<JsTable> join(String joinType, JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd, @JsOptional @JsNullable String asOfMatchRule) {
        if (joinType.equals("AJ") || joinType.equals("RAJ") || joinType.equals("ReverseAJ")) {
            return asOfJoin(rightTable, columnsToMatch, columnsToAdd, asOfMatchRule);
        } else if (joinType.equals("CROSS_JOIN") || joinType.equals("Join")) {
            return crossJoin(rightTable, columnsToMatch, columnsToAdd, null);
        } else if (joinType.equals("EXACT_JOIN") || joinType.equals("ExactJoin")) {
            return exactJoin(rightTable, columnsToMatch, columnsToAdd);
        } else if (joinType.equals("NATURAL_JOIN") || joinType.equals("Natural")) {
            return naturalJoin(rightTable, columnsToMatch, columnsToAdd);
        } else {
            throw new IllegalArgumentException("Unsupported join type " + joinType);
        }
    }

    @Override
    @JsMethod
    public Promise<JsTable> asOfJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd, @JsOptional @JsNullable String asOfMatchRule) {
        if (rightTable.state().getConnection() != workerConnection) {
            throw new IllegalStateException(
                    "Table argument passed to join is not from the same worker as current table");
        }
        return workerConnection.newState((c, state, metadata) -> {
            AsOfJoinTablesRequest request = new AsOfJoinTablesRequest();
            request.setLeftId(state().getHandle().makeTableReference());
            request.setRightId(rightTable.state().getHandle().makeTableReference());
            request.setResultId(state.getHandle().makeTicket());
            request.setColumnsToMatchList(columnsToMatch);
            request.setColumnsToAddList(columnsToAdd);
            if (asOfMatchRule != null) {
                request.setAsOfMatchRule(
                        Js.asPropertyMap(AsOfJoinTablesRequest.MatchRule).getAsAny(asOfMatchRule).asDouble());
            }
            workerConnection.tableServiceClient().asOfJoinTables(request, metadata, c::apply);
        }, "asOfJoin(" + rightTable + ", " + columnsToMatch + ", " + columnsToAdd + "," + asOfMatchRule + ")")
                .refetch(this, workerConnection.metadata())
                .then(state -> Promise.resolve(new JsTable(workerConnection, state)));
    }

    @Override
    @JsMethod
    public Promise<JsTable> crossJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd, @JsOptional @JsNullable Double reserveBits) {
        if (rightTable.state().getConnection() != workerConnection) {
            throw new IllegalStateException(
                    "Table argument passed to join is not from the same worker as current table");
        }
        return workerConnection.newState((c, state, metadata) -> {
            CrossJoinTablesRequest request = new CrossJoinTablesRequest();
            request.setLeftId(state().getHandle().makeTableReference());
            request.setRightId(rightTable.state().getHandle().makeTableReference());
            request.setResultId(state.getHandle().makeTicket());
            request.setColumnsToMatchList(columnsToMatch);
            request.setColumnsToAddList(columnsToAdd);
            if (reserveBits != null) {
                request.setReserveBits(reserveBits);
            }
            workerConnection.tableServiceClient().crossJoinTables(request, metadata, c::apply);
        }, "join(" + rightTable + ", " + columnsToMatch + ", " + columnsToAdd + "," + reserveBits + ")")
                .refetch(this, workerConnection.metadata())
                .then(state -> Promise.resolve(new JsTable(workerConnection, state)));
    }

    @Override
    @JsMethod
    public Promise<JsTable> exactJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional JsArray<String> columnsToAdd) {
        if (rightTable.state().getConnection() != workerConnection) {
            throw new IllegalStateException(
                    "Table argument passed to join is not from the same worker as current table");
        }
        return workerConnection.newState((c, state, metadata) -> {
            ExactJoinTablesRequest request = new ExactJoinTablesRequest();
            request.setLeftId(state().getHandle().makeTableReference());
            request.setRightId(rightTable.state().getHandle().makeTableReference());
            request.setResultId(state.getHandle().makeTicket());
            request.setColumnsToMatchList(columnsToMatch);
            request.setColumnsToAddList(columnsToAdd);
            workerConnection.tableServiceClient().exactJoinTables(request, metadata, c::apply);
        }, "exactJoin(" + rightTable + ", " + columnsToMatch + ", " + columnsToAdd + ")")
                .refetch(this, workerConnection.metadata())
                .then(state -> Promise.resolve(new JsTable(workerConnection, state)));
    }

    @Override
    @JsMethod
    public Promise<JsTable> naturalJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional JsArray<String> columnsToAdd) {
        if (rightTable.state().getConnection() != workerConnection) {
            throw new IllegalStateException(
                    "Table argument passed to join is not from the same worker as current table");
        }
        return workerConnection.newState((c, state, metadata) -> {
            NaturalJoinTablesRequest request = new NaturalJoinTablesRequest();
            request.setLeftId(state().getHandle().makeTableReference());
            request.setRightId(rightTable.state().getHandle().makeTableReference());
            request.setResultId(state.getHandle().makeTicket());
            request.setColumnsToMatchList(columnsToMatch);
            request.setColumnsToAddList(columnsToAdd);
            workerConnection.tableServiceClient().naturalJoinTables(request, metadata, c::apply);
        }, "naturalJoin(" + rightTable + ", " + columnsToMatch + ", " + columnsToAdd + ")")
                .refetch(this, workerConnection.metadata())
                .then(state -> Promise.resolve(new JsTable(workerConnection, state)));
    }

    @JsMethod
    public Promise<JsPartitionedTable> byExternal(Object keys, @JsOptional Boolean dropKeys) {
        return partitionBy(keys, dropKeys);
    }

    /**
     * Creates a new PartitionedTable from the contents of the current table, partitioning data based on the specified
     * keys.
     *
     * @param keys
     * @param dropKeys
     *
     * @return Promise dh.PartitionedTable
     */
    @JsMethod
    public Promise<JsPartitionedTable> partitionBy(Object keys, @JsOptional Boolean dropKeys) {
        final String[] actualKeys;
        if (keys instanceof String) {
            actualKeys = new String[] {(String) keys};
        } else if (JsArray.isArray(keys)) {
            actualKeys = Js.asArrayLike(keys).asList().toArray(new String[0]);
        } else {
            throw new IllegalArgumentException("Can't use keys argument as either a string or array of strings");
        }
        // We don't validate that the keys are non-empty, since that is allowed, but ensure they are all columns
        findColumns(actualKeys);

        // Start the partitionBy on the server - we want to get the error from here, but we'll race the fetch against
        // this to avoid an extra round-trip
        Ticket partitionedTableTicket = workerConnection.getTickets().newExportTicket();
        Promise<PartitionByResponse> partitionByPromise = Callbacks.<PartitionByResponse, Object>grpcUnaryPromise(c -> {
            PartitionByRequest partitionBy = new PartitionByRequest();
            partitionBy.setTableId(state().getHandle().makeTicket());
            partitionBy.setResultId(partitionedTableTicket);
            partitionBy.setKeyColumnNamesList(actualKeys);
            if (dropKeys != null) {
                partitionBy.setDropKeys(dropKeys);
            }
            workerConnection.partitionedTableServiceClient().partitionBy(partitionBy, workerConnection.metadata(),
                    c::apply);
        });
        // construct the partitioned table around the ticket created above
        TypedTicket typedTicket = new TypedTicket();
        typedTicket.setType(JsVariableType.PARTITIONEDTABLE);
        typedTicket.setTicket(partitionedTableTicket);
        Promise<JsPartitionedTable> fetchPromise = new JsWidget(workerConnection, typedTicket).refetch().then(
                widget -> Promise.resolve(new JsPartitionedTable(workerConnection, widget)));

        // Ensure that the partition failure propagates first, but the result of the fetch will be returned - both
        // are running concurrently.
        return partitionByPromise.then(ignore -> fetchPromise);
    }

    /**
     * a promise that will resolve to ColumnStatistics for the column of this table.
     *
     * @param column
     * @return Promise of dh.ColumnStatistics
     */
    @JsMethod
    public Promise<JsColumnStatistics> getColumnStatistics(Column column) {
        List<Runnable> toRelease = new ArrayList<>();
        return workerConnection.newState((c, state, metadata) -> {
            ColumnStatisticsRequest req = new ColumnStatisticsRequest();
            req.setColumnName(column.getName());
            req.setSourceId(state().getHandle().makeTableReference());
            req.setResultId(state.getHandle().makeTicket());
            workerConnection.tableServiceClient().computeColumnStatistics(req, metadata, c::apply);
        }, "get column statistics")
                .refetch(this, workerConnection.metadata())
                .then(state -> {
                    JsTable table = new JsTable(workerConnection, state);
                    toRelease.add(table::close);
                    DataOptions.SnapshotOptions options = new DataOptions.SnapshotOptions();
                    options.rows = Js.uncheckedCast(JsRangeSet.ofRange(0, 0));
                    options.columns = table.getColumns();
                    return table.createSnapshot(options);
                })
                .then(tableData -> Promise.resolve(new JsColumnStatistics(tableData)))
                .finally_(() -> {
                    toRelease.forEach(Runnable::run);
                });
    }

    private Literal objectToLiteral(String valueType, Object value) {
        Literal literal = new Literal();
        if (value instanceof DateWrapper) {
            literal.setNanoTimeValue(((DateWrapper) value).valueOf());
        } else if (value instanceof LongWrapper) {
            literal.setLongValue(((LongWrapper) value).valueOf());
        } else if (Js.typeof(value).equals("number")) {
            literal.setDoubleValue(Js.asDouble(value));
        } else if (Js.typeof(value).equals("boolean")) {
            literal.setBoolValue((Boolean) value);
        } else {
            switch (valueType) {
                case ValueType.STRING:
                    literal.setStringValue(value.toString());
                    break;
                case ValueType.NUMBER:
                    literal.setDoubleValue(Double.parseDouble(value.toString()));
                    break;
                case ValueType.LONG:
                    literal.setLongValue(value.toString());
                    break;
                case ValueType.DATETIME:
                    literal.setNanoTimeValue(value.toString());
                    break;
                case ValueType.BOOLEAN:
                    literal.setBoolValue(Boolean.parseBoolean(value.toString()));
                    break;
                default:
                    throw new UnsupportedOperationException("Invalid value type for seekRow: " + valueType);
            }
        }
        return literal;
    }

    /**
     * Seek the row matching the data provided
     *
     * @param startingRow Row to start the seek from
     * @param column Column to seek for value on
     * @param valueType Type of value provided
     * @param seekValue Value to seek
     * @param insensitive Optional value to flag a search as case-insensitive. Defaults to {@code false}.
     * @param contains Optional value to have the seek value do a contains search instead of exact equality. Defaults to
     *        {@code false}.
     * @param isBackwards Optional value to seek backwards through the table instead of forwards. Defaults to
     *        {@code false}.
     * @return A promise that resolves to the row value found.
     */
    @JsMethod
    public Promise<Double> seekRow(
            double startingRow,
            Column column,
            @TsTypeRef(ValueType.class) String valueType,
            Any seekValue,
            @JsOptional @JsNullable Boolean insensitive,
            @JsOptional @JsNullable Boolean contains,
            @JsOptional @JsNullable Boolean isBackwards) {
        SeekRowRequest seekRowRequest = new SeekRowRequest();
        seekRowRequest.setSourceId(state().getHandle().makeTicket());
        seekRowRequest.setStartingRow(String.valueOf(startingRow));
        seekRowRequest.setColumnName(column.getName());
        seekRowRequest.setSeekValue(objectToLiteral(valueType, seekValue));
        if (insensitive != null) {
            seekRowRequest.setInsensitive(insensitive);
        }
        if (contains != null) {
            seekRowRequest.setContains(contains);
        }
        if (isBackwards != null) {
            seekRowRequest.setIsBackward(isBackwards);
        }

        return Callbacks
                .<SeekRowResponse, Object>grpcUnaryPromise(c -> workerConnection.tableServiceClient()
                        .seekRow(seekRowRequest, workerConnection.metadata(), c::apply))
                .then(seekRowResponse -> Promise.resolve((double) Long.parseLong(seekRowResponse.getResultRow())));
    }

    public void maybeRevive(ClientTableState state) {
        if (isSuppress()) {
            revive(state);
        }
    }

    public void revive(ClientTableState state) {
        JsLog.debug("Revive!", (state == state()), this);
        if (state == state()) {
            unsuppressEvents();
            LazyPromise.runLater(() -> {
                fireEvent(EVENT_RECONNECT);
            });
        }
    }

    /**
     * Get a downsampled version of the table. Currently only supports downsampling with an Instant or long
     * {@code xCol}.
     *
     * @param zoomRange The visible range as {@code [start, end]} or {@code null} to always use all data.
     * @param pixelCount The width of the visible area in pixels.
     * @param xCol The name of the X column to downsample. Must be an {@code Instant} or {@code long}.
     * @param yCols The names of the Y columns to downsample.
     * @return A promise that resolves to the downsampled table.
     */
    public Promise<JsTable> downsample(LongWrapper[] zoomRange, int pixelCount, String xCol,
            String[] yCols) {
        JsLog.info("downsample", zoomRange, pixelCount, xCol, yCols);
        final String fetchSummary = "downsample(" + Arrays.toString(zoomRange) + ", " + pixelCount + ", " + xCol + ", "
                + Arrays.toString(yCols) + ")";
        return workerConnection.newState((c, state, metadata) -> {
            RunChartDownsampleRequest downsampleRequest = new RunChartDownsampleRequest();
            downsampleRequest.setPixelCount(pixelCount);
            if (zoomRange != null) {
                ZoomRange zoom = new ZoomRange();
                zoom.setMinDateNanos(Long.toString(zoomRange[0].getWrapped()));
                zoom.setMaxDateNanos(Long.toString(zoomRange[1].getWrapped()));
                downsampleRequest.setZoomRange(zoom);
            }
            downsampleRequest.setXColumnName(xCol);
            downsampleRequest.setYColumnNamesList(yCols);
            downsampleRequest.setSourceId(state().getHandle().makeTableReference());
            downsampleRequest.setResultId(state.getHandle().makeTicket());
            workerConnection.tableServiceClient().runChartDownsample(downsampleRequest, workerConnection.metadata(),
                    c::apply);
        }, fetchSummary).refetch(this, workerConnection.metadata())
                .then(state -> Promise.resolve(new JsTable(workerConnection, state)));
    }

    /**
     * {@code true} if this table has been closed.
     * 
     * @return boolean
     */
    @JsProperty(name = "isClosed")
    public boolean isClosed() {
        return currentState == null;
    }

    /**
     * {@code true} if this table may receive updates from the server, including size changed events, updated events
     * after initial snapshot.
     *
     * @return boolean
     */
    @JsProperty(name = "isRefreshing")
    public boolean isRefreshing() {
        return !state().isStatic();
    }

    /**
     * Read-only. {@code true} if this table is uncoalesced, indicating that work must be done before the table can be
     * used.
     * <p>
     * Uncoalesced tables are expensive to operate on - filter to a single partition or range of partitions before
     * subscribing to access only the desired data efficiently. A subscription can be specified without a filter, but
     * this can be very expensive. To see which partitions are available, check each column on the table to see which
     * have {@link Column#getIsPartitionColumn()} as {@code true}, and filter those columns. To read the possible values
     * for those columns, use {@link #selectDistinct(Column[])}.
     *
     * @return {@code true} if the table is uncoaleced and should be filtered before operating on it, otherwise
     *         {@code false}.
     */
    @JsProperty(name = "isUncoalesced")
    public boolean isUncoalesced() {
        return size == Long.MIN_VALUE;
    }

    @JsProperty
    @JsNullable
    public String getPluginName() {
        return lastVisibleState().getTableDef().getAttributes().getPluginName();
    }

    @Override
    public TableTicket getHandle() {
        return state().getHandle();
    }

    public TableTicket getHeadHandle() {
        return getHeadState().getHandle();
    }

    private ClientTableState getHeadState() {
        // TODO: stash a pointer to head state for us to return, and instead make it an
        // assertion that head is an ancestor of the current state. (Although, reconnection
        // will _want_ to change the HEAD state, so we may want to just delete this comment)
        ClientTableState head = currentState;
        for (ClientTableState s : currentState.ancestors()) {
            head = s;
        }
        return head;
    }

    @JsMethod
    @Override
    public String toString() {
        if (isAlive()) {
            return "Table { id=" + subscriptionId + " filters=[" + getFilter() + "], sort=[" + getSort()
                    + "], customColumns=[" + getCustomColumns() + "] }";
        }
        return "Table { id=" + subscriptionId + " CLOSED }";
    }

    public WorkerConnection getConnection() {
        return workerConnection;
    }

    public boolean isActive(ClientTableState state) {
        return currentState == state;
    }

    @Override
    public void setState(final ClientTableState state) {
        state.onRunning(s -> {
            if (state == currentState) {
                lastVisibleState = state;
                hasInputTable = s.getTableDef().getAttributes().isInputTable();
                isBlinkTable = s.getTableDef().getAttributes().isBlinkTable();

                // defer the size change so that is there is a viewport sub also waiting for onRunning, it gets it first
                LazyPromise.runLater(() -> {
                    if (state == state()) {
                        setSize(state.getSize());
                    }
                });
            }
        }, JsRunnable.doNothing());
        final ClientTableState was = currentState;
        if (was != state) {
            state.onRunning(s -> {
                // Double check it didn't switch back while we weren't looking
                // If already closed, we can ignore this, since we already cleaned those up
                if (!isClosed() && was != null && was != state()) {
                    // if we held a subscription
                    TableViewportSubscription existingSubscription = subscriptions.remove(was.getHandle());
                    if (existingSubscription != null && !existingSubscription.isClosed()) {
                        JsLog.debug("closing old viewport", state(), existingSubscription.state());
                        // with the replacement state successfully running, we can shut down the old viewport (unless
                        // something external retained it)
                        existingSubscription.internalClose();
                    }
                }
            }, JsRunnable.doNothing());

            boolean historyChanged = false;
            if (was != null) {
                // check if the new state is derived from the current state
                historyChanged = !state.isAncestor(was);
                was.pause(this);
                JsLog.debug("Table state change (new history? ", historyChanged, ") " +
                        "from ", was.getHandle().toString(), was,
                        " to ", state.getHandle().toString(), state);
            }
            currentState = state;
            ActiveTableBinding active = state.getActiveBinding(this);
            if (active == null) {
                state.createBinding(this);
            } else {
                active.changeState(state);
            }

            if (historyChanged) {
                // when the new state is not derived from the current state,
                // then, when the new state succeeds, we will totally releaseTable the previous table,
                // allowing it to be automatically released (if nobody else needs it).
                state.onRunning(success -> {
                    if (isClosed()) {
                        // if already closed, we should have already released that handle too
                        return;
                    }
                    if (currentState != state) {
                        // we've already moved on from this state, cleanup() should manage it, don't release the
                        // ancestor
                        return;
                    }
                    final boolean shouldRelease = !state().isAncestor(was);
                    JsLog.debug("History changing state update complete; release? ", shouldRelease, " state: ", was,
                            LazyString.of(was::toStringMinimal));
                    if (shouldRelease) {
                        was.releaseTable(this);
                    }
                }, () -> {
                    LazyPromise.runLater(() -> {
                        if (isClosed()) {
                            // if already closed, we should have already released that handle too
                            return;
                        }
                        if (currentState != state) {
                            // we've already moved on from this state, cleanup() should manage it, don't release the
                            // ancestor
                            return;
                        }
                        final boolean shouldRelease = !currentState.isAncestor(was);
                        JsLog.debug("History changing state update failed; release? ", shouldRelease, " state: ", was,
                                LazyString.of(was::toStringMinimal));
                        if (shouldRelease) {
                            was.releaseTable(this);
                        }
                    });
                });
            }
            fireEvent(INTERNAL_EVENT_STATECHANGED, state);
        }
    }

    public ActiveTableBinding getBinding() {
        return currentState.getActiveBinding(this);
    }

    public StateCache getCache() {
        return workerConnection.getCache();
    }

    @Override
    public boolean hasHandle(TableTicket tableHandle) {
        for (ClientTableState state : state().reversed()) {
            if (state.getHandle().equals(tableHandle)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasRollbackHandle(TableTicket tableHandle) {
        for (ClientTableState state : state().reversed()) {
            ActiveTableBinding binding = state.getActiveBinding(this);
            if (binding == null) {
                continue;
            }

            ActiveTableBinding rollback = binding.getRollback();
            if (rollback == null) {
                continue;
            }

            if (rollback.getState().getHandle().equals(tableHandle)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void setRollback(ActiveTableBinding rollbackTo) {
        if (rollbackTo.getState().isRunning()) {
            getBinding().setRollback(rollbackTo.getPaused());
        } else {
            // recurse until we find the active one that is running, which we can rollback to
            assert rollbackTo.getRollback() != null;
            setRollback(rollbackTo.getRollback());
        }
    }

    @Override
    public void rollback() {
        getBinding().rollback();
    }

    public void setSize(double s) {
        boolean changed = this.size != s;
        if (changed) {
            JsLog.debug("Table ", this, " size changed from ", this.size, " to ", s);
        }
        this.size = s;

        TableViewportSubscription subscription = subscriptions.get(getHandle());
        if (changed && (subscription == null || !subscription.hasValidSize())) {
            // If the size changed, and we have no subscription active, fire. Otherwise, we want to let the
            // subscription itself manage this, so that the size changes are synchronized with data changes,
            // and consumers won't be confused by the table size not matching data.
            fireEvent(JsTable.EVENT_SIZECHANGED, s);
        }
        fireEvent(JsTable.INTERNAL_EVENT_SIZELISTENER);
    }

    public int getSubscriptionId() {
        return subscriptionId;
    }

    @Override
    public void maybeReviveSubscription() {
        TableViewportSubscription viewportSubscription = subscriptions.get(getHandle());
        if (viewportSubscription != null) {
            viewportSubscription.revive();
        }
    }

}
