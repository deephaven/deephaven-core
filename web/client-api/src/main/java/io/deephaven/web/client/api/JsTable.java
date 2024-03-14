//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.JsArray;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable.ThenOnFulfilledCallbackFn;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.RollupRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.TreeRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.partitionedtable_pb.PartitionByRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.partitionedtable_pb.PartitionByResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.AggregateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.AsOfJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.BatchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ColumnStatisticsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.CrossJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.DropColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExactJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Literal;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.NaturalJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.RunChartDownsampleRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SeekRowRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SeekRowResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SelectDistinctRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SelectOrUpdateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SnapshotTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SnapshotWhenTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TableReference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.batchtablerequest.Operation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.runchartdownsamplerequest.ZoomRange;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.TableAttributesDefinition;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.batch.RequestBatcher;
import io.deephaven.web.client.api.batch.TableConfig;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.filter.FilterValue;
import io.deephaven.web.client.api.input.JsInputTable;
import io.deephaven.web.client.api.lifecycle.HasLifecycle;
import io.deephaven.web.client.api.state.StateCache;
import io.deephaven.web.client.api.subscription.TableSubscription;
import io.deephaven.web.client.api.subscription.TableViewportSubscription;
import io.deephaven.web.client.api.subscription.ViewportData;
import io.deephaven.web.client.api.subscription.ViewportData.MergeResults;
import io.deephaven.web.client.api.subscription.ViewportRow;
import io.deephaven.web.client.api.tree.JsRollupConfig;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.tree.JsTreeTableConfig;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.client.fu.JsData;
import io.deephaven.web.client.fu.JsItr;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.state.ActiveTableBinding;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.client.state.HasTableBinding;
import io.deephaven.web.shared.data.*;
import io.deephaven.web.shared.data.TableSnapshot.SnapshotType;
import io.deephaven.web.shared.data.columns.ColumnData;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsProvider;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.fu.RemoverFn;
import javaemul.internal.annotations.DoNotAutobox;
import jsinterop.annotations.JsIgnore;
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

import java.util.*;
import java.util.stream.Stream;

import static io.deephaven.web.client.api.subscription.ViewportData.NO_ROW_FORMAT_COLUMN;
import static io.deephaven.web.client.fu.LazyPromise.logError;

/**
 * Provides access to data in a table. Note that several methods present their response through Promises. This allows
 * the client to both avoid actually connecting to the server until necessary, and also will permit some changes not to
 * inform the UI right away that they have taken place.
 */
@TsName(namespace = "dh", name = "Table")
public class JsTable extends HasLifecycle implements HasTableBinding, JoinableTable, ServerObject {
    @JsProperty(namespace = "dh.Table")
    /**
     * The table size has updated, so live scrollbars and the like can be updated accordingly.
     */
    public static final String EVENT_SIZECHANGED = "sizechanged",
            /**
             * event.detail is the currently visible window, the same as if getViewportData() was called and resolved.
             * Listening to this event removes the need to listen to the finer grained events below for data changes. In
             * contrast, using the finer grained events may enable only updating the specific rows which saw a change.
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

    @JsProperty(namespace = "dh.Table")
    public static final double SIZE_UNCOALESCED = -2;

    // indicates that the CTS has changed, "downstream" tables should take note
    public static final String INTERNAL_EVENT_STATECHANGED = "statechanged-internal",
            // indicates that the "size listener" has gone off, thought possibly without a change in size, indicating a
            // change in some table data
            INTERNAL_EVENT_SIZELISTENER = "sizelistener-internal";

    // Amount of debounce to use when eating snapshot events.
    public static final int DEBOUNCE_TIME = 20;
    public static final int MAX_BATCH_TIME = 600_000;

    private final WorkerConnection workerConnection;

    private Map<TableTicket, TableViewportSubscription> subscriptions = new HashMap<>();
    @Deprecated // TODO refactor this inside of the viewportSubscription type
    private ViewportData currentViewportData;

    private ClientTableState lastVisibleState;

    private ClientTableState currentState;

    private int batchDepth;

    private boolean hasInputTable;

    private boolean isBlinkTable;

    private final List<JsRunnable> onClosed;

    private double size = ClientTableState.SIZE_UNINITIALIZED;

    private final int subscriptionId;
    private static int nextSubscriptionId;
    private TableSubscription nonViewportSub;

    /**
     * Creates a new Table directly from an existing ClientTableState. The CTS manages all fetch operations, so this is
     * just a simple constructor to get a table that points to the given state.
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
     * a Sort than can be used to reverse a table. This can be passed into n array in applySort. Note that Tree Tables
     * do not support reverse.
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
     * Retrieve a column by the given name. You should prefer to always retrieve a new Column instance instead of
     * caching a returned value.
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
        assert currentState != null : "Table already closed, cannot be used again";
        return currentState;
    }

    /**
     * True if this table represents a user Input Table (created by InputTable.newInputTable). When true, you may call
     * .inputTable() to add or remove data from the underlying table.
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
     * If .hasInputTable is true, you may call this method to gain access to an InputTable object which can be used to
     * mutate the data within the table. If the table is not an Input Table, the promise will be immediately rejected.
     *
     * @return Promise of dh.InputTable
     */
    @JsMethod
    public Promise<JsInputTable> inputTable() {
        if (!hasInputTable) {
            return Js.uncheckedCast(Promise.reject("Table is not an InputTable"));
        }
        String[] keyCols = new String[0];
        String[] valueCols = new String[0];
        for (int i = 0; i < getColumns().length; i++) {
            if (getColumns().getAt(i).isInputTableKeyColumn()) {
                keyCols[keyCols.length] = getColumns().getAt(i).getName();
            } else {
                valueCols[valueCols.length] = getColumns().getAt(i).getName();
            }
        }
        return Promise.resolve(new JsInputTable(this, keyCols, valueCols));
    }

    /**
     * Indicates that this Table instance will no longer be used, and its connection to the server can be cleaned up.
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
     * null if no property exists, a string if it is an easily serializable property, or a {@code Promise
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
     * .setViewport(), you will get only those columns in your ViewportData. <b>Number size</b> The total count of rows
     * in the table. The size can and will change; see the <b>sizechanged</b> event for details. Size will be negative
     * in exceptional cases (eg. the table is uncoalesced, see the <b>isUncoalesced</b> property for details).
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
     * The total count of rows in the table. The size can and will change; see the <b>sizechanged</b> event for details.
     * Size will be negative in exceptional cases (e.g., the table is uncoalesced; see the <b>isUncoalesced</b>
     * property). for details).
     * 
     * @return double
     */
    @JsProperty
    public double getSize() {
        TableViewportSubscription subscription = subscriptions.get(getHandle());
        if (subscription != null && subscription.getStatus() == TableViewportSubscription.Status.ACTIVE) {
            // only ask the viewport for the size if it is alive and ticking
            return subscription.size();
        }
        if (isUncoalesced()) {
            return JsTable.SIZE_UNCOALESCED;
        }
        return size;
    }

    @JsProperty
    @JsNullable
    public String getDescription() {
        return lastVisibleState().getTableDef().getAttributes().getDescription();
    }

    /**
     * The total count of the rows in the table, excluding any filters. Unlike <b>size</b>, changes to this value will
     * not result in any event. <b>Sort[] sort</b> an ordered list of Sorts to apply to the table. To update, call
     * applySort(). Note that this getter will return the new value immediately, even though it may take a little time
     * to update on the server. You may listen for the <b>sortchanged</b> event to know when to update the UI.
     * 
     * @return double
     */
    @JsProperty
    public double getTotalSize() {
        TableViewportSubscription subscription = subscriptions.get(getHandle());
        if (subscription != null && subscription.getStatus() == TableViewportSubscription.Status.ACTIVE) {
            // only ask the viewport for the size if it is alive and ticking
            return subscription.totalSize();
        }
        return getHeadState().getSize();
    }

    /**
     * An ordered list of Sorts to apply to the table. To update, call <b>applySort()</b>. Note that this getter will
     * return the new value immediately, even though it may take a little time to update on the server. You may listen
     * for the <b>sortchanged</b> event to know when to update the UI.
     * 
     * @return {@link Sort} array
     */
    @JsProperty
    public JsArray<Sort> getSort() {
        return JsItr.slice(state().getSorts());
    }

    /**
     * An ordered list of Filters to apply to the table. To update, call applyFilter(). Note that this getter will
     * return the new value immediately, even though it may take a little time to update on the server. You may listen
     * for the <b>filterchanged</b> event to know when to update the UI.
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
     * applied, and the <b>sortchanged</b> event fires. Reusing existing, applied sorts may enable this to perform
     * better on the server. The <b>updated</b> event will also fire, but <b>rowadded</b> and <b>rowremoved</b> will
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
     * is applied, and the <b>filterchanged</b> event fires. Reusing existing, applied filters may enable this to
     * perform better on the server. The <b>updated</b> event will also fire, but <b>rowadded</b> and <b>rowremoved</b>
     * will not.
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
     * used when adding new filter and sort operations to the table, as long as they are present.
     *
     * @param customColumns
     * @return {@link CustomColumn} array
     */
    @JsMethod
    @SuppressWarnings("unusable-by-js")
    public JsArray<CustomColumn> applyCustomColumns(JsArray<CustomColumnArgUnionType> customColumns) {
        String[] customColumnStrings = customColumns.map((item, index, array) -> {
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
     * ones. To update, call <b>applyCustomColumns()</b>.
     * 
     * @return {@link CustomColumn} array
     *
     */
    @JsProperty
    public JsArray<CustomColumn> getCustomColumns() {
        return Js.cast(JsItr.slice(state().getCustomColumnsObject()));
    }

    /**
     * Overload for Java (since JS just omits the optional params)
     */
    public TableViewportSubscription setViewport(double firstRow, double lastRow) {
        return setViewport(firstRow, lastRow, null, null);
    }

    /**
     * Overload for Java (since JS just omits the optional param)
     */
    public TableViewportSubscription setViewport(double firstRow, double lastRow, JsArray<Column> columns) {
        return setViewport(firstRow, lastRow, columns, null);
    }

    /**
     * If the columns parameter is not provided, all columns will be used. If the updateIntervalMs parameter is not
     * provided, a default of one second will be used. Until this is called, no data will be available. Invoking this
     * will result in events to be fired once data becomes available, starting with an `updated` event and a
     * <b>rowadded</b> event per row in that range. The returned object allows the viewport to be closed when no longer
     * needed.
     *
     * @param firstRow
     * @param lastRow
     * @param columns
     * @param updateIntervalMs
     * @return {@link TableViewportSubscription}
     */
    @JsMethod
    public TableViewportSubscription setViewport(double firstRow, double lastRow,
            @JsOptional @JsNullable JsArray<Column> columns,
            @JsOptional @JsNullable Double updateIntervalMs) {
        Column[] columnsCopy = columns != null ? Js.uncheckedCast(columns.slice()) : null;
        ClientTableState currentState = state();
        TableViewportSubscription activeSubscription = subscriptions.get(getHandle());
        if (activeSubscription != null && activeSubscription.getStatus() != TableViewportSubscription.Status.DONE) {
            // hasn't finished, lets reuse it
            activeSubscription.setInternalViewport(firstRow, lastRow, columnsCopy, updateIntervalMs);
            return activeSubscription;
        } else {
            // In the past, we left the old sub going until the new one was ready, then started the new one. But now,
            // we want to reference the old or the new as appropriate - until the new state is running, we keep pumping
            // the old one, then cross over once we're able.

            // We're not responsible here for shutting down the old one here - setState will do that after the new one
            // is running.

            // rewrap current state in a new one, when ready the viewport will be applied
            TableViewportSubscription replacement =
                    new TableViewportSubscription(firstRow, lastRow, columnsCopy, updateIntervalMs, this);

            subscriptions.put(currentState.getHandle(), replacement);
            return replacement;
        }
    }

    public void setInternalViewport(double firstRow, double lastRow, Column[] columns) {
        if (firstRow > lastRow) {
            throw new IllegalArgumentException(firstRow + " > " + lastRow);
        }
        if (firstRow < 0) {
            throw new IllegalArgumentException(firstRow + " < " + 0);
        }
        currentViewportData = null;
        // we must wait for the latest stack entry that can add columns (so we get an appropriate BitSet)
        state().setDesiredViewport(this, (long) firstRow, (long) lastRow, columns);
    }

    /**
     * Gets the currently visible viewport. If the current set of operations has not yet resulted in data, it will not
     * resolve until that data is ready. If this table is closed before the promise resolves, it will be rejected - to
     * separate the lifespan of this promise from the table itself, call
     * {@link TableViewportSubscription#getViewportData()} on the result from {@link #setViewport(double, double)}.
     * 
     * @return Promise of {@link TableData}
     */
    @JsMethod
    public Promise<TableData> getViewportData() {
        TableViewportSubscription subscription = subscriptions.get(getHandle());
        if (subscription == null) {
            return Promise.reject("No viewport currently set");
        }
        return subscription.getInternalViewportData();
    }

    public Promise<TableData> getInternalViewportData() {
        final LazyPromise<TableData> promise = new LazyPromise<>();
        final ClientTableState active = state();
        active.onRunning(state -> {
            if (currentViewportData == null) {
                // no viewport data received yet; let's set up a one-shot UPDATED event listener
                addEventListenerOneShot(EVENT_UPDATED, ignored -> promise.succeed(currentViewportData));
            } else {
                promise.succeed(currentViewportData);
            }
        }, promise::fail, () -> promise.fail("Table closed before viewport data was read"));
        return promise.asPromise(MAX_BATCH_TIME);
    }

    /**
     * Overload for java (since js just omits the optional var)
     */
    public TableSubscription subscribe(JsArray<Column> columns) {
        return subscribe(columns, null);
    }

    /**
     * Creates a subscription to the specified columns, across all rows in the table. The optional parameter
     * updateIntervalMs may be specified to indicate how often the server should send updates, defaulting to one second
     * if omitted. Useful for charts or taking a snapshot of the table atomically. The initial snapshot will arrive in a
     * single event, but later changes will be sent as updates. However, this may still be very expensive to run from a
     * browser for very large tables. Each call to subscribe creates a new subscription, which must have <b>close()</b>
     * called on it to stop it, and all events are fired from the TableSubscription instance.
     *
     * @param columns
     * @param updateIntervalMs
     * @return {@link TableSubscription}
     */
    @JsMethod
    public TableSubscription subscribe(JsArray<Column> columns, @JsOptional Double updateIntervalMs) {
        assert nonViewportSub == null : "Can't directly subscribe to the 'private' table instance";
        // make a new table with a pUT call, listen to the subscription there
        return new TableSubscription(columns, this, updateIntervalMs);
    }

    public void internalSubscribe(JsArray<Column> columns, TableSubscription sub) {
        if (columns == null) {
            columns = getColumns();
        }
        this.nonViewportSub = sub;

        state().subscribe(this, Js.uncheckedCast(columns));
    }

    /**
     * a new table containing the distinct tuples of values from the given columns that are present in the original
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
     * a promise that will resolve to a Totals Table of this table. This table will obey the configurations provided as
     * a parameter, or will use the table's default if no parameter is provided, and be updated once per second as
     * necessary. Note that multiple calls to this method will each produce a new TotalsTable which must have close()
     * called on it when not in use.
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
     * The default configuration to be used when building a <b>TotalsTable</b> for this table.
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
     * a promise that will resolve to a Totals Table of this table, ignoring any filters. See <b>getTotalsTable()</b>
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
     * a promise that will resolve to a new roll-up <b>TreeTable</b> of this table. Multiple calls to this method will
     * each produce a new <b>TreeTable</b> which must have close() called on it when not in use.
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

        Ticket rollupTicket = workerConnection.getConfig().newTicket();

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
     * a promise that will resolve to a new `TreeTable` of this table. Multiple calls to this method will each produce a
     * new `TreeTable` which must have close() called on it when not in use.
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

        Ticket treeTicket = workerConnection.getConfig().newTicket();

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
     * a "frozen" version of this table (a server-side snapshot of the entire source table). Viewports on the frozen
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

    /**
     * @deprecated a promise that will be resolved with a newly created table holding the results of the join operation.
     *             The last parameter is optional, and if not specified or empty, all columns from the right table will
     *             be added to the output. Callers are responsible for ensuring that there are no duplicates - a match
     *             pair can be passed instead of a name to specify the new name for the column. Supported `joinType`
     *             values (consult Deephaven's "Joining Data from Multiple Tables for more detail): "Join" <a href=
     *             'https://docs.deephaven.io/latest/Content/writeQueries/tableOperations/joins.htm#Joining_Data_from_Multiple_Tables'>Joining_Data_from_Multiple_Tables</a>
     *             "Natural" "AJ" "ReverseAJ" "ExactJoin" "LeftJoin"
     * @param joinType
     * @param rightTable
     * @param columnsToMatch
     * @param columnsToAdd
     * @param asOfMatchRule
     * @return Promise of dh.Table
     */
    @Override
    @JsMethod
    @Deprecated
    public Promise<JsTable> join(Object joinType, JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional @JsNullable JsArray<String> columnsToAdd, @JsOptional @JsNullable Object asOfMatchRule) {
        if (joinType.equals("AJ") || joinType.equals("RAJ")) {
            return asOfJoin(rightTable, columnsToMatch, columnsToAdd, (String) asOfMatchRule);
        } else if (joinType.equals("CROSS_JOIN")) {
            return crossJoin(rightTable, columnsToMatch, columnsToAdd, null);
        } else if (joinType.equals("EXACT_JOIN")) {
            return exactJoin(rightTable, columnsToMatch, columnsToAdd);
        } else if (joinType.equals("NATURAL_JOIN")) {
            return naturalJoin(rightTable, columnsToMatch, columnsToAdd);
        } else {
            throw new IllegalArgumentException("Unsupported join type " + joinType);
        }
    }

    /**
     * a promise that will be resolved with the newly created table holding the results of the specified as-of join
     * operation. The <b>columnsToAdd</b> parameter is optional, not specifying it will result in all columns from the
     * right table being added to the output. The <b>asOfMatchRule</b> is optional, defaults to <b>LESS_THAN_EQUAL</b>
     *
     * <p>
     * the allowed values are:
     * </p>
     *
     * <ul>
     * <li>LESS_THAN_EQUAL</li>
     * <li>LESS_THAN</li>
     * <li>GREATER_THAN_EQUAL</li>
     * <li>GREATER_THAN</li>
     * </ul>
     *
     * @param rightTable
     * @param columnsToMatch
     * @param columnsToAdd
     * @param asOfMatchRule
     * @return Promise og dh.Table
     */
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

    /**
     * a promise that will be resolved with the newly created table holding the results of the specified cross join
     * operation. The <b>columnsToAdd</b> parameter is optional, not specifying it will result in all columns from the
     * right table being added to the output. The <b>reserveBits</b> optional parameter lets the client control how the
     * key space is distributed between the rows in the two tables, see the Java <b>Table</b> class for details.
     *
     * @param rightTable
     * @param columnsToMatch
     * @param columnsToAdd
     * @param reserve_bits
     *
     * @return Promise of dh.Table
     */
    @Override
    @JsMethod
    public Promise<JsTable> crossJoin(JoinableTable rightTable, JsArray<String> columnsToMatch,
            @JsOptional JsArray<String> columnsToAdd, @JsOptional Double reserve_bits) {
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
            if (reserve_bits != null) {
                request.setReserveBits(reserve_bits);
            }
            workerConnection.tableServiceClient().crossJoinTables(request, metadata, c::apply);
        }, "join(" + rightTable + ", " + columnsToMatch + ", " + columnsToAdd + "," + reserve_bits + ")")
                .refetch(this, workerConnection.metadata())
                .then(state -> Promise.resolve(new JsTable(workerConnection, state)));
    }

    /**
     * a promise that will be resolved with the newly created table holding the results of the specified exact join
     * operation. The `columnsToAdd` parameter is optional, not specifying it will result in all columns from the right
     * table being added to the output.
     *
     * @param rightTable
     * @param columnsToMatch
     * @param columnsToAdd
     *
     * @return Promise of dh.Table
     */
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

    /**
     * a promise that will be resolved with the newly created table holding the results of the specified natural join
     * operation. The <b>columnsToAdd</b> parameter is optional, not specifying it will result in all columns from the
     * right table being added to the output.
     *
     * @param rightTable
     * @param columnsToMatch
     * @param columnsToAdd
     *
     * @return Promise of dh.Table
     */
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
        Ticket partitionedTableTicket = workerConnection.getConfig().newTicket();
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
        if (column.getDescription() != null && column.getDescription().startsWith("Preview of type")) {
            // TODO (deephaven-core#188) Remove this workaround when we don't preview columns until just before
            // subscription
            return Promise.reject("Can't produce column statistics for preview column");
        }
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
                    // TODO (deephaven-core#188) don't drop these columns once we can decode them
                    JsArray<String> dropCols = new JsArray<>();
                    if (Arrays.stream(state.getColumns()).anyMatch(c -> c.getName().equals("UNIQUE_KEYS"))) {
                        dropCols.push("UNIQUE_KEYS");
                    }
                    if (Arrays.stream(state.getColumns()).anyMatch(c -> c.getName().equals("UNIQUE_COUNTS"))) {
                        dropCols.push("UNIQUE_COUNTS");
                    }

                    if (dropCols.length > 0) {
                        toRelease.add(() -> workerConnection.releaseHandle(state.getHandle()));
                        return workerConnection.newState((c2, state2, metadata2) -> {
                            DropColumnsRequest drop = new DropColumnsRequest();
                            drop.setColumnNamesList(dropCols);
                            drop.setSourceId(state.getHandle().makeTableReference());
                            drop.setResultId(state2.getHandle().makeTicket());
                            workerConnection.tableServiceClient().dropColumns(drop, metadata2, c2::apply);
                        }, "drop unreadable stats columns")
                                .refetch(this, workerConnection.metadata())
                                .then(state2 -> {
                                    JsTable table = new JsTable(workerConnection, state2);
                                    toRelease.add(table::close);
                                    table.setViewport(0, 0);
                                    return table.getViewportData();
                                });
                    }
                    JsTable table = new JsTable(workerConnection, state);
                    toRelease.add(table::close);
                    table.setViewport(0, 0);
                    return table.getViewportData();
                })
                .then(tableData -> Promise.resolve(new JsColumnStatistics(tableData)));
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
     * @param insensitive Optional value to flag a search as case-insensitive. Defaults to `false`.
     * @param contains Optional value to have the seek value do a contains search instead of exact equality. Defaults to
     *        `false`.
     * @param isBackwards Optional value to seek backwards through the table instead of forwards. Defaults to `false`.
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
                getBinding().maybeReviveSubscription();
            });
        }
    }

    public Promise<JsTable> downsample(LongWrapper[] zoomRange, int pixelCount, String xCol, String[] yCols) {
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

    private final class Debounce {
        private final ClientTableState state;
        private final TableTicket handle;
        private final SnapshotType type;
        private final RangeSet includedRows;
        private final BitSet columns;
        private final Object[] dataColumns;
        private final double timestamp;
        private final long maxRows;

        public Debounce(
                TableTicket table,
                SnapshotType snapshotType,
                RangeSet includedRows,
                BitSet columns,
                Object[] dataColumns,
                long maxRows) {
            this.handle = table;
            this.type = snapshotType;
            this.includedRows = includedRows;
            this.columns = columns;
            this.dataColumns = dataColumns;
            this.state = currentState;
            this.maxRows = maxRows;
            timestamp = System.currentTimeMillis();
        }

        public boolean isEqual(Debounce o) {
            if (type == o.type) {
                // this is intentionally weird. We only want to debounce when one instance is column snapshot and the
                // other is row snapshot,
                // so we consider two events of the same type to be incompatible with debouncing.
                return false;
            }
            if (handle != o.handle) {
                assert !handle.equals(o.handle);
                return false;
            }
            if (state != o.state) {
                assert state.getHandle() != o.state.getHandle();
                return false;
            }
            if (!includedRows.equals(o.includedRows)) {
                return false;
            }
            if (!columns.equals(o.columns)) {
                return false;
            }
            if (maxRows != o.maxRows) {
                return false;
            }
            assert Arrays.deepEquals(dataColumns, o.dataColumns) : "Debounce is broken, remove it.";
            return true;
        }
    }

    private Debounce debounce;

    private void handleSnapshot(TableTicket table, SnapshotType snapshotType, RangeSet includedRows,
            Object[] dataColumns, BitSet columns, long maxRows) {
        assert table.equals(state().getHandle()) : "Table received incorrect snapshot";
        // if the type is initial_snapshot, we've already recorded the size, so only watch for the other two updates.
        // note that this will sometimes result in multiple updates on startup, so we do this ugly debounce-dance.
        // When IDS-2113 is fixed, we can likely remove this code.
        JsLog.debug("Received snapshot for ", table, snapshotType, includedRows, dataColumns, columns);
        Debounce operation = new Debounce(table, snapshotType, includedRows, columns, dataColumns, maxRows);
        if (debounce == null) {
            debounce = operation;
            DomGlobal.setTimeout(ignored -> processSnapshot(), DEBOUNCE_TIME);
        } else if (debounce.isEqual(operation)) {
            // If we think the problem is fixed, we can put `assert false` here for a while before deleting Debounce
            // class
            JsLog.debug("Eating duplicated operation", debounce, operation);
        } else {
            processSnapshot();
            debounce = operation;
            DomGlobal.setTimeout(ignored -> processSnapshot(), DEBOUNCE_TIME);
        }
    }

    public void handleSnapshot(TableTicket handle, TableSnapshot snapshot) {
        if (!handle.equals(state().getHandle())) {
            return;
        }
        Viewport viewport = getBinding().getSubscription();
        if (viewport == null || viewport.getRows() == null || viewport.getRows().size() == 0) {
            // check out if we have a non-viewport sub attached
            if (nonViewportSub != null) {
                nonViewportSub.handleSnapshot(snapshot);
            }
            return;
        }

        RangeSet viewportRows = viewport.getRows();
        JsLog.debug("handleSnapshot on " + viewportRows, handle, snapshot, viewport);

        RangeSet includedRows = snapshot.getIncludedRows();
        ColumnData[] dataColumns = snapshot.getDataColumns();
        JsArray[] remappedData = new JsArray[dataColumns.length];
        // remap dataColumns to the expected range for that table's viewport
        long lastRow = -1;
        for (int col = viewport.getColumns().nextSetBit(0); col >= 0; col = viewport.getColumns().nextSetBit(col + 1)) {
            ColumnData dataColumn = dataColumns[col];
            if (dataColumn == null) {
                // skip this, at least one column requested by that table isn't present, waiting on a later update
                // TODO when IDS-2138 is fixed stop throwing this data away
                return;
            }
            Object columnData = dataColumn.getData();

            final ColumnDefinition def = state().getTableDef().getColumns()[col];
            remappedData[col] = JsData.newArray(def.getType());

            PrimitiveIterator.OfLong viewportIterator = viewportRows.indexIterator();
            PrimitiveIterator.OfLong includedRowsIterator = includedRows.indexIterator();
            int dataIndex = 0;
            while (viewportIterator.hasNext()) {
                long viewportIndex = viewportIterator.nextLong();
                if (viewportIndex >= snapshot.getTableSize()) {
                    // reached or passed the end of the table, we'll still make a snapshot
                    break;
                }
                if (!includedRowsIterator.hasNext()) {
                    // we've reached the end, the viewport apparently goes past the end of what the server sent,
                    // so there is another snapshot on its way
                    // TODO when IDS-2138 is fixed stop throwing this data away
                    return;
                }

                long possibleMatch = includedRowsIterator.nextLong();
                while (includedRowsIterator.hasNext() && possibleMatch < viewportIndex) {
                    dataIndex++;// skip, still seeking to the next item

                    possibleMatch = includedRowsIterator.nextLong();
                }
                if (!includedRowsIterator.hasNext() && possibleMatch < viewportIndex) {
                    // we didn't find any items which match, just give up
                    return;
                }

                if (possibleMatch > viewportIndex) {
                    // if we hit a gap (more data coming, doesn't match viewport), skip the
                    // rest of this table entirely, a later update will get us caught up
                    return;
                }
                Object data = Js.<JsArray<Object>>uncheckedCast(columnData).getAt(dataIndex);
                remappedData[col].push(data);
                dataIndex++;// increment for the next row

                // Track how many rows were actually present, allowing the snapshot to stop before the viewport's end
                lastRow = Math.max(lastRow, possibleMatch);
            }
        }

        // TODO correct this - assumes max one range per table viewport, and nothing skipped
        RangeSet actualViewport =
                lastRow == -1 ? RangeSet.empty() : RangeSet.ofRange(viewportRows.indexIterator().nextLong(), lastRow);

        handleSnapshot(handle, snapshot.getSnapshotType(), actualViewport, remappedData, viewport.getColumns(),
                viewportRows.size());
    }

    @JsIgnore
    public void processSnapshot() {
        try {
            if (debounce == null) {
                JsLog.debug("Skipping snapshot b/c debounce is null");
                return;
            }
            if (debounce.state != currentState) {
                JsLog.debug("Skipping snapshot because state has changed ", debounce.state, " != ", currentState);
                return;
            }
            if (isClosed()) {
                JsLog.debug("Skipping snapshot because table is closed", this);
                return;
            }
            JsArray<Column> viewportColumns =
                    getColumns().filter((item, index, all) -> debounce.columns.get(item.getIndex()));
            ViewportData data = new ViewportData(debounce.includedRows, debounce.dataColumns, viewportColumns,
                    currentState.getRowFormatColumn() == null ? NO_ROW_FORMAT_COLUMN
                            : currentState.getRowFormatColumn().getIndex(),
                    debounce.maxRows);
            this.currentViewportData = data;
            CustomEventInit updatedEvent = CustomEventInit.create();
            updatedEvent.setDetail(data);
            fireEvent(EVENT_UPDATED, updatedEvent);

            // also fire rowadded events - TODO also fire some kind of remove event for now-missing rows?
            for (int i = 0; i < data.getRows().length; i++) {
                CustomEventInit addedEvent = CustomEventInit.create();
                addedEvent.setDetail(wrap(data.getRows().getAt(i), i));
                fireEvent(EVENT_ROWADDED, addedEvent);
            }
        } finally {
            debounce = null;
        }
    }

    /**
     * True if this table has been closed.
     * 
     * @return boolean
     */
    @JsProperty(name = "isClosed")
    public boolean isClosed() {
        return currentState == null;
    }

    /**
     * True if this table may receive updates from the server, including size changed events, updated events after
     * initial snapshot.
     *
     * @return boolean
     */
    @JsProperty(name = "isRefreshing")
    public boolean isRefreshing() {
        return !state().isStatic();
    }

    /**
     * Read-only. True if this table is uncoalesced. Set a viewport or filter on the partition columns to coalesce the
     * table. Check the <b>isPartitionColumn</b> property on the table columns to retrieve the partition columns. Size
     * will be unavailable until table is coalesced.
     * 
     * @return boolean
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

    // Factored out so that we always apply the same format
    private Object wrap(ViewportRow at, int index) {
        return JsPropertyMap.of("row", at, "index", (double) index);
    }

    public void handleDelta(ClientTableState current, DeltaUpdates updates) {
        current.onRunning(s -> {
            if (current != state()) {
                return;
            }
            if (nonViewportSub != null) {
                nonViewportSub.handleDelta(updates);
                return;
            }
            final ViewportData vpd = currentViewportData;
            if (vpd == null) {
                // if the current viewport data is null, we're waiting on an initial snapshot to arrive for a different
                // part of the viewport
                JsLog.debug("Received delta while waiting for reinitialization");
                return;
            }
            MergeResults mergeResults = vpd.merge(updates);
            if (mergeResults.added.size() == 0 && mergeResults.modified.size() == 0
                    && mergeResults.removed.size() == 0) {
                return;
            }
            CustomEventInit event = CustomEventInit.create();
            event.setDetail(vpd);
            // user might call setViewport, and wind up nulling our currentViewportData
            fireEvent(EVENT_UPDATED, event);

            // fire rowadded/rowupdated/rowremoved
            // TODO when we keep more rows loaded than the user is aware of, check if a given row is actually in the
            // viewport
            // here
            for (Integer index : mergeResults.added) {
                CustomEventInit addedEvent = CustomEventInit.create();
                addedEvent.setDetail(wrap(vpd.getRows().getAt(index), index));
                fireEvent(EVENT_ROWADDED, addedEvent);
            }
            for (Integer index : mergeResults.modified) {
                CustomEventInit addedEvent = CustomEventInit.create();
                addedEvent.setDetail(wrap(vpd.getRows().getAt(index), index));
                fireEvent(EVENT_ROWUPDATED, addedEvent);
            }
            for (Integer index : mergeResults.removed) {
                CustomEventInit addedEvent = CustomEventInit.create();
                addedEvent.setDetail(wrap(vpd.getRows().getAt(index), index));
                fireEvent(EVENT_ROWREMOVED, addedEvent);
            }
        }, JsRunnable.doNothing());
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

    public void refreshViewport(ClientTableState state, Viewport vp) {
        assert state() == state : "Called refreshViewport with wrong state (" + state + " instead of " + state() + ")";
        assert state.getResolution() == ClientTableState.ResolutionState.RUNNING
                : "Do not call refreshViewport for a state that is not running! (" + state + ")";

        currentViewportData = null; // ignore any deltas for past viewports
        workerConnection.scheduleCheck(state);
        // now that we've made sure the server knows, if we already know that the viewport is beyond what exists, we
        // can go ahead and fire an update event. We're in the onResolved call, so we know the handle has resolved
        // and if size is not -1, then we've already at least gotten the initial snapshot (otherwise, that snapshot
        // will be here soon, and will fire its own event)
        if (state.getSize() != ClientTableState.SIZE_UNINITIALIZED && state.getSize() <= vp.getRows().getFirstRow()) {
            JsLog.debug("Preparing to send a 'fake' update event since " + state.getSize() + "<="
                    + vp.getRows().getFirstRow(), state);
            LazyPromise.runLater(() -> {
                if (state != state()) {
                    return;
                }

                // get the column expected to be in the snapshot
                JsArray<Column> columns = Js.uncheckedCast(getBinding().getColumns());
                Column[] allColumns = state.getColumns();
                if (columns == null) {
                    columns = Js.uncheckedCast(allColumns);
                }
                // build an array of empty column data for this snapshot
                Object[] dataColumns = new Object[allColumns.length];

                for (int i = 0; i < columns.length; i++) {
                    Column c = columns.getAt(i);
                    dataColumns[c.getIndex()] = JsData.newArray(c.getType());
                    if (c.getFormatStringColumnIndex() != null) {
                        dataColumns[c.getFormatStringColumnIndex()] = JsData.newArray("java.lang.String");
                    }
                    if (c.getStyleColumnIndex() != null) {
                        dataColumns[c.getStyleColumnIndex()] = JsData.newArray("long");
                    }
                }
                if (currentState.getRowFormatColumn() != null) {
                    dataColumns[currentState.getRowFormatColumn().getIndex()] = JsData.newArray("long");
                }

                ViewportData data = new ViewportData(RangeSet.empty(), dataColumns, columns,
                        currentState.getRowFormatColumn() == null ? NO_ROW_FORMAT_COLUMN
                                : currentState.getRowFormatColumn().getIndex(),
                        0);
                this.currentViewportData = data;
                CustomEventInit updatedEvent = CustomEventInit.create();
                updatedEvent.setDetail(data);
                JsLog.debug("Sending 'fake' update event since " + state.getSize() + "<=" + vp.getRows().getFirstRow(),
                        vp, state);
                fireEvent(EVENT_UPDATED, updatedEvent);
            });
        }
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
                    if (existingSubscription != null
                            && existingSubscription.getStatus() != TableViewportSubscription.Status.DONE) {
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
            final CustomEventInit init = CustomEventInit.create();
            init.setDetail(state);
            fireEvent(INTERNAL_EVENT_STATECHANGED, init);
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
        if (changed && (subscription == null || subscription.getStatus() == TableViewportSubscription.Status.DONE)) {
            // If the size changed, and we have no subscription active, fire. Otherwise, we want to let the
            // subscription itself manage this, so that the size changes are synchronized with data changes,
            // and consumers won't be confused by the table size not matching data.
            CustomEventInit event = CustomEventInit.create();
            event.setDetail(s);
            fireEvent(JsTable.EVENT_SIZECHANGED, event);
        }
        fireEvent(JsTable.INTERNAL_EVENT_SIZELISTENER);
    }

    public int getSubscriptionId() {
        return subscriptionId;
    }

    @Override
    public void maybeReviveSubscription() {
        getBinding().maybeReviveSubscription();
    }

}
