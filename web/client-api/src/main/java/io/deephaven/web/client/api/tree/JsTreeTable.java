//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.tree;

import com.vertispan.tsdefs.annotations.TsIgnore;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.Uint8Array;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableApplyRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableSourceExportRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableViewKeyTableDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableViewRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Condition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TableReference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.data.WebBarrageSubscription;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.impl.TicketAndPromise;
import io.deephaven.web.client.api.lifecycle.HasLifecycle;
import io.deephaven.web.client.api.subscription.AbstractTableSubscription;
import io.deephaven.web.client.api.subscription.SubscriptionType;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.client.fu.JsItr;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.*;
import javaemul.internal.annotations.DoNotAutobox;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Behaves like a {@link JsTable} externally, but data, state, and viewports are managed by an entirely different
 * mechanism, and so reimplemented here.
 * <p>
 * Any time a change is made, we build a new request and send it to the server, and wait for the updated state.
 * <p>
 * Semantics around getting updates from the server are slightly different - we don't "unset" the viewport here after
 * operations are performed, but encourage the client code to re-set them to the desired position.
 * <p>
 * The table size will be -1 until a viewport has been fetched.
 * <p>
 * Similar to a table, a Tree Table provides access to subscribed viewport data on the current hierarchy. A different
 * Row type is used within that viewport, showing the depth of that node within the tree and indicating details about
 * whether it has children or is expanded. The Tree Table itself then provides the ability to change if a row is
 * expanded or not. Methods used to control or check if a row should be expanded or not can be invoked on a TreeRow
 * instance, or on the number of the row (thus allowing for expanding/collapsing rows which are not currently visible in
 * the viewport).
 * <p>
 * Events and viewports are somewhat different from tables, due to the expense of computing the expanded/collapsed rows
 * and count of children at each level of the hierarchy, and differences in the data that is available.
 * <p>
 * <ul>
 * <li>There is no {@link JsTable#getTotalSize() totalSize} property.</li>
 * <li>The viewport is not un-set when changes are made to filter or sort, but changes will continue to be streamed in.
 * It is suggested that the viewport be changed to the desired position (usually the first N rows) after any filter/sort
 * change is made. Likewise, {@link #getViewportData()} will always return the most recent data, and will not wait if a
 * new operation is pending.</li>
 * <li>Custom columns are not directly supported. If the TreeTable was created client-side, the original Table can have
 * custom columns applied, and the TreeTable can be recreated.</li>
 * <li>Whereas Table has a {@link JsTable#getTotalsTableConfig()} property, it is defined here as a method,
 * {@link #getTotalsTableConfig()}. This returns a promise so the config can be fetched asynchronously.</li>
 * <li>Totals Tables for trees vary in behavior between tree tables and roll-up tables. This behavior is based on the
 * original flat table used to produce the Tree Table - for a hierarchical table (i.e. Table.treeTable in the query
 * config), the totals will include non-leaf nodes (since they are themselves actual rows in the table), but in a
 * roll-up table, the totals only include leaf nodes (as non-leaf nodes are generated through grouping the contents of
 * the original table). Roll-ups also have the {@link JsRollupConfig#includeConstituents} property, indicating that a
 * {@link Column} in the tree may have a {@link Column#getConstituentType()} property reflecting that the type of cells
 * where {@link TreeSubscription.TreeRowImpl#hasChildren()} is false will be different from usual.</li>
 * </ul>
 */
@JsType(namespace = "dh", name = "TreeTable")
public class JsTreeTable extends HasLifecycle implements ServerObject {
    /**
     * event.detail is the currently visible viewport data based on the active viewport configuration.
     */
    public static final String EVENT_UPDATED = "updated",
            EVENT_DISCONNECT = "disconnect",
            EVENT_RECONNECT = "reconnect",
            EVENT_RECONNECTFAILED = "reconnectfailed",
            EVENT_REQUEST_FAILED = "requestfailed";

    private static final double ACTION_EXPAND = 0b001;
    private static final double ACTION_EXPAND_WITH_DESCENDENTS = 0b011;
    private static final double ACTION_COLLAPSE = 0b100;

    /**
     * Ordered series of steps that must be performed when changes are made to the table. When any change is applied,
     * all subsequent steps must be performed as well.
     */
    private enum RebuildStep {
        FILTER, SORT, HIERARCHICAL_TABLE_VIEW, SUBSCRIPTION;
    }

    private final WorkerConnection connection;

    // This group of fields represent the underlying state of the original HierarchicalTable
    private final JsWidget widget;
    private final boolean isRefreshing;
    private final InitialTableDefinition tableDefinition;
    private final Column[] visibleColumns;
    private final Map<String, Column> columnsByName = new HashMap<>();
    private final Map<String, Column> sourceColumns;
    private final JsArray<Column> keyColumns = new JsArray<>();
    private Column rowDepthCol;
    private Column rowExpandedCol;
    private final Column actionCol;
    private final JsArray<Column> groupedColumns;
    private JsLayoutHints layoutHints;

    // The source JsTable behind the original HierarchicalTable, lazily built at this time
    private final JsLazy<Promise<JsTable>> sourceTable;

    // The current filter and sort state
    private List<FilterCondition> filters = new ArrayList<>();
    private List<Sort> sorts = new ArrayList<>();
    private TicketAndPromise<?> filteredTable;
    private TicketAndPromise<?> sortedTable;

    // Tracking for the current/next key table contents. Note that the key table doesn't necessarily
    // only include key columns, but all HierarchicalTable.isExpandByColumn columns.
    private Object[][] keyTableData;
    private Promise<JsTable> keyTable;

    private TicketAndPromise<?> viewTicket;
    private Promise<TreeSubscription> stream;

    // the "next" set of filters/sorts that we'll use. these either are "==" to the above fields, or are scheduled
    // to replace them soon.
    private List<FilterCondition> nextFilters = new ArrayList<>();
    private List<Sort> nextSort = new ArrayList<>();

    // viewport information
    private Double firstRow;
    private Double lastRow;
    private Column[] columns;
    private int updateInterval = 1000;

    private TreeSubscription.TreeViewportDataImpl currentViewportData;

    private boolean alwaysFireNextEvent = false;

    private boolean closed = false;

    @JsIgnore
    public JsTreeTable(WorkerConnection workerConnection, JsWidget widget) {
        this.connection = workerConnection;
        this.widget = widget;

        // register for same-session disconnect/reconnect callbacks
        this.connection.registerSimpleReconnectable(this);

        // TODO(deephaven-core#3604) factor most of the rest of this out for a refetch, in case of new session
        HierarchicalTableDescriptor treeDescriptor =
                HierarchicalTableDescriptor.deserializeBinary(widget.getDataAsU8());

        Uint8Array flightSchemaMessage = treeDescriptor.getSnapshotSchema_asU8();

        this.isRefreshing = !treeDescriptor.getIsStatic();
        this.tableDefinition = WebBarrageUtils.readTableDefinition(flightSchemaMessage);
        Column[] columns = new Column[0];
        Map<Boolean, Map<String, ColumnDefinition>> columnDefsByName = tableDefinition.getColumnsByName();
        int rowFormatColumn = -1;

        // This is handy to avoid certain lookups that we'll only do anyway if this is a rollup. This is naturally
        // always false if this is a tree.
        boolean hasConstituentColumns = !columnDefsByName.get(true).isEmpty();

        Map<String, Column> constituentColumns = new HashMap<>();
        JsArray<Column> groupedColumns = new JsArray<>();
        for (ColumnDefinition definition : tableDefinition.getColumns()) {
            Column column = definition.makeJsColumn(columns.length, columnDefsByName);
            if (definition.isForRow()) {
                assert rowFormatColumn == -1;
                rowFormatColumn = definition.getColumnIndex();
                continue;
            }

            if (definition.isHierarchicalRowDepthColumn()) {
                rowDepthCol = column;
            } else if (definition.isHierarchicalRowExpandedColumn()) {
                rowExpandedCol = column;
            } else if (definition.isHierarchicalExpandByColumn()) {
                // technically this may be set for the above two cases, but at this time we send them regardless
                keyColumns.push(column);
            }
            if (definition.isRollupConstituentNodeColumn()) {
                constituentColumns.put(column.getName(), column);
            }
            if (definition.isVisible()) {
                columns[columns.length] = column;
            }
            if (definition.isRollupGroupByColumn() && !definition.isRollupConstituentNodeColumn()) {
                groupedColumns.push(column);

                if (hasConstituentColumns) {
                    column.setConstituentType(columnDefsByName.get(true).get(definition.getName()).getType());
                }
            }
            if (hasConstituentColumns && definition.getRollupAggregationInputColumn() != null
                    && !definition.getRollupAggregationInputColumn().isEmpty()) {
                column.setConstituentType(
                        columnDefsByName.get(true).get(definition.getRollupAggregationInputColumn()).getType());
            }
        }
        this.groupedColumns = JsObject.freeze(groupedColumns);

        sourceColumns = columnDefsByName.get(false).values().stream()
                .map(c -> {
                    if (c.getRollupAggregationInputColumn() != null && !c.getRollupAggregationInputColumn().isEmpty()) {
                        // Use the specified input column
                        return constituentColumns.remove(c.getRollupAggregationInputColumn());
                    }
                    if (c.isRollupGroupByColumn()) {
                        // use the groupby column's own name
                        return constituentColumns.remove(c.getName());
                    }
                    // filter out the rest
                    return null;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(
                        Column::getName,
                        Function.identity()));
        // add the rest of the constituent columns as themselves, they will only show up in constituent rows
        sourceColumns.putAll(constituentColumns);
        // TODO #3303 offer those as plain columns too

        // visit each column with a source column but no format/style column - if the source column as a format column,
        // we will reference the source column's format column data instead
        for (int i = 0; i < columns.length; i++) {
            Column visibleColumn = columns[i];
            Column sourceColumn = sourceColumns.get(visibleColumn.getName());
            if (sourceColumn == null) {
                continue;
            }

            if (visibleColumn.getFormatStringColumnIndex() == null
                    && sourceColumn.getFormatStringColumnIndex() != null) {
                columns[i] = visibleColumn.withFormatStringColumnIndex(sourceColumn.getFormatStringColumnIndex());
            }
            if (visibleColumn.getStyleColumnIndex() == null && sourceColumn.getStyleColumnIndex() != null) {
                columns[i] = visibleColumn.withStyleColumnIndex(sourceColumn.getStyleColumnIndex());
            }
        }

        // track columns by name and freeze the array to avoid defensive copies
        this.visibleColumns = JsObject.freeze(columns);
        for (int i = 0; i < visibleColumns.length; i++) {
            Column column = visibleColumns[i];
            columnsByName.put(column.getName(), column);
        }

        keyTableData = new Object[keyColumns.length + 2][0];
        actionCol = new Column(-1, -1, null, null, "byte", "__action__", false, null, null, false, false);

        sourceTable = JsLazy.of(() -> workerConnection
                .newState(this, (c, newState, metadata) -> {
                    HierarchicalTableSourceExportRequest exportRequest = new HierarchicalTableSourceExportRequest();
                    exportRequest.setResultTableId(newState.getHandle().makeTicket());
                    exportRequest.setHierarchicalTableId(widget.getTicket());
                    connection.hierarchicalTableServiceClient().exportSource(exportRequest, connection.metadata(),
                            c::apply);
                }, "source for hierarchical table")
                .then(cts -> Promise.resolve(new JsTable(connection, cts))));
    }

    private TicketAndPromise<?> prepareFilter() {
        if (filteredTable != null) {
            return filteredTable;
        }
        if (nextFilters.isEmpty()) {
            return new TicketAndPromise<>(widget.getTicket(), connection);
        }
        Ticket ticket = connection.getConfig().newTicket();
        filteredTable = new TicketAndPromise<>(ticket, Callbacks.grpcUnaryPromise(c -> {

            HierarchicalTableApplyRequest applyFilter = new HierarchicalTableApplyRequest();
            applyFilter.setFiltersList(
                    nextFilters.stream().map(FilterCondition::makeDescriptor).toArray(Condition[]::new));
            applyFilter.setInputHierarchicalTableId(widget.getTicket());
            applyFilter.setResultHierarchicalTableId(ticket);
            connection.hierarchicalTableServiceClient().apply(applyFilter, connection.metadata(), c::apply);
        }), connection);
        return filteredTable;
    }

    private TicketAndPromise<?> prepareSort(TicketAndPromise<?> prevTicket) {
        if (sortedTable != null) {
            return sortedTable;
        }
        if (nextSort.isEmpty()) {
            return prevTicket;
        }
        Ticket ticket = connection.getConfig().newTicket();
        sortedTable = new TicketAndPromise<>(ticket, Callbacks.grpcUnaryPromise(c -> {
            HierarchicalTableApplyRequest applyFilter = new HierarchicalTableApplyRequest();
            applyFilter.setSortsList(nextSort.stream().map(Sort::makeDescriptor).toArray(
                    io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SortDescriptor[]::new));
            applyFilter.setInputHierarchicalTableId(prevTicket.ticket());
            applyFilter.setResultHierarchicalTableId(ticket);
            connection.hierarchicalTableServiceClient().apply(applyFilter, connection.metadata(), c::apply);
        }), connection);
        return sortedTable;
    }

    private Promise<JsTable> makeKeyTable() {
        if (keyTable != null) {
            return keyTable;
        }
        JsArray<Column> keyTableColumns = keyColumns.slice();
        keyTableColumns.push(rowDepthCol);
        keyTableColumns.push(actionCol);
        keyTable = connection.newTable(
                Js.uncheckedCast(keyTableColumns.map((p0, p1) -> p0.getName())),
                Js.uncheckedCast(keyTableColumns.map((p0, p1) -> p0.getType())),
                keyTableData,
                null,
                null);
        return keyTable;
    }

    private TicketAndPromise<?> makeView(TicketAndPromise<?> prevTicket) {
        if (viewTicket != null) {
            return viewTicket;
        }
        Ticket ticket = connection.getConfig().newTicket();
        Promise<JsTable> keyTable = makeKeyTable();
        viewTicket = new TicketAndPromise<>(ticket, Callbacks.grpcUnaryPromise(c -> {
            HierarchicalTableViewRequest viewRequest = new HierarchicalTableViewRequest();
            viewRequest.setHierarchicalTableId(prevTicket.ticket());
            viewRequest.setResultViewId(ticket);
            keyTable.then(t -> {
                if (keyTableData[0].length > 0) {
                    HierarchicalTableViewKeyTableDescriptor expansions = new HierarchicalTableViewKeyTableDescriptor();
                    expansions.setKeyTableId(t.getHandle().makeTicket());
                    expansions.setKeyTableActionColumn(actionCol.getName());
                    viewRequest.setExpansions(expansions);
                }
                connection.hierarchicalTableServiceClient().view(viewRequest, connection.metadata(), c::apply);
                return null;
            }, error -> {
                c.apply(error, null);
                return null;
            });
        }), connection);
        return viewTicket;
    }

    private int constituentDepth() {
        return keyColumns.length + 2;
    }

    public class TreeSubscription extends AbstractTableSubscription {
        @TsIgnore
        public class TreeViewportDataImpl extends AbstractTableSubscription.UpdateEventData
                implements TreeViewportData {
            private final double treeSize;

            private final JsArray<Column> columns;

            private TreeViewportDataImpl(WebBarrageSubscription subscription, int rowStyleColumn,
                    JsArray<Column> columns,
                    RangeSet added, RangeSet removed, RangeSet modified, ShiftedRange[] shifted) {
                super(subscription, rowStyleColumn, columns, added, removed, modified, shifted);

                this.treeSize = barrageSubscription.getCurrentRowSet().size();
                this.columns = JsObject.freeze(Js.cast(Js.<JsArray<Column>>uncheckedCast(columns).slice()));
            }

            @Override
            public Any getData(int index, Column column) {
                Column sourceColumn = sourceColumns.get(column.getName());
                if (sourceColumn == null) {
                    // no constituent column, call super
                    return super.getData(index, column);
                }
                if (barrageSubscription.getData(index, rowDepthCol.getIndex()).asInt() != constituentDepth()) {
                    // not at constituent depth, call super
                    return super.getData(index, column);
                }
                // read source col instead
                return super.getData(index, sourceColumn);
            }

            @Override
            public Any getData(long index, Column column) {
                Column sourceColumn = sourceColumns.get(column.getName());
                if (sourceColumn == null) {
                    // no constituent column, call super
                    return super.getData(index, column);
                }
                if (barrageSubscription.getData(index, rowDepthCol.getIndex()).asInt() != constituentDepth()) {
                    // not at constituent depth, call super
                    return super.getData(index, column);
                }
                // read source col instead
                return super.getData(index, sourceColumn);
            }

            @Override
            public Format getFormat(int index, Column column) {
                Column sourceColumn = sourceColumns.get(column.getName());
                if (sourceColumn == null) {
                    // no constituent column, call super
                    return super.getFormat(index, column);
                }
                if (barrageSubscription.getData(index, rowDepthCol.getIndex()).asInt() != constituentDepth()) {
                    // not at constituent depth, call super
                    return super.getFormat(index, column);
                }
                // read source col instead
                return super.getFormat(index, sourceColumn);
            }

            @Override
            public Format getFormat(long index, Column column) {
                Column sourceColumn = sourceColumns.get(column.getName());
                if (sourceColumn == null) {
                    // no constituent column, call super
                    return super.getFormat(index, column);
                }
                if (barrageSubscription.getData(index, rowDepthCol.getIndex()).asInt() != constituentDepth()) {
                    // not at constituent depth, call super
                    return super.getFormat(index, column);
                }
                // read source col instead
                return super.getFormat(index, sourceColumn);
            }

            @Override
            public JsArray<Column> getColumns() {
                // This looks like its superclass, but we're actually returning a different field
                return columns;
            }

            @Override
            protected SubscriptionRow makeRow(long index) {
                return new TreeRowImpl(subscription, index);
            }

            @JsProperty
            public double getTreeSize() {
                return treeSize;
            }
        }

        public class TreeRowImpl extends SubscriptionRow implements TreeViewportData.TreeRow {

            public TreeRowImpl(WebBarrageSubscription subscription, long index) {
                super(subscription, rowStyleColumn, index);
            }

            @Override
            public boolean isExpanded() {
                return barrageSubscription.getData(index, rowExpandedCol.getIndex()).uncheckedCast() == Boolean.TRUE;
            }

            @Override
            public boolean hasChildren() {
                return barrageSubscription.getData(index, rowExpandedCol.getIndex()).uncheckedCast() != null;
            }

            @Override
            public int depth() {
                return Js.coerceToInt(barrageSubscription.getData(index, rowDepthCol.getIndex()));
            }

            public void appendKeyData(Object[][] keyTableData, double action) {
                int i;
                for (i = 0; i < keyColumns.length; i++) {
                    Js.<JsArray<Any>>cast(keyTableData[i]).push(keyColumns.getAt(i).get(this));
                }
                Js.<JsArray<Double>>cast(keyTableData[i++]).push((double) depth());
                Js.<JsArray<Double>>cast(keyTableData[i++]).push(action);
            }

            @Override
            public Any get(Column column) {
                Column sourceColumn = sourceColumns.get(column.getName());
                if (sourceColumn == null) {
                    // no constituent column, call super
                    return super.get(column);
                }
                if (barrageSubscription.getData(index, rowDepthCol.getIndex()).asInt() != constituentDepth()) {
                    // not at constituent depth, call super
                    return super.get(column);
                }
                // read source col instead
                return super.get(sourceColumn);
            }

            @Override
            public Format getFormat(Column column) {
                Column sourceColumn = sourceColumns.get(column.getName());
                if (sourceColumn == null) {
                    // no constituent column, call super
                    return super.getFormat(column);
                }
                if (barrageSubscription.getData(index, rowDepthCol.getIndex()).asInt() != constituentDepth()) {
                    // not at constituent depth, call super
                    return super.getFormat(column);
                }
                // read source col instead
                return super.getFormat(sourceColumn);
            }
        }

        private RangeSet serverViewport;

        public TreeSubscription(ClientTableState state, WorkerConnection connection) {
            super(SubscriptionType.VIEWPORT_SUBSCRIPTION, state, connection);
        }

        @Override
        protected void sendFirstSubscriptionRequest() {
            setViewport(firstRow, lastRow, Js.uncheckedCast(columns), (double) updateInterval);
        }

        @Override
        protected BitSet makeColumnBitset(JsArray<Column> columns) {
            BitSet requested = super.makeColumnBitset(columns);
            requested.or(makeColumnSubscriptionBitset());
            return requested;
        }

        @Override
        protected void onStreamEnd(ResponseStreamWrapper.Status status) {
            super.onStreamEnd(status);
            JsTreeTable.this.stream = null;
            if (!status.isOk()) {
                failureHandled(status.getDetails());
            }
        }

        public void setViewport(double firstRow, double lastRow, JsArray<Column> columns, Double updateInterval) {
            serverViewport = RangeSet.ofRange((long) firstRow, (long) lastRow);

            sendBarrageSubscriptionRequest(RangeSet.ofRange((long) firstRow, (long) lastRow), Js.uncheckedCast(columns),
                    updateInterval, false);
        }

        @Override
        protected void notifyUpdate(RangeSet rowsAdded, RangeSet rowsRemoved, RangeSet totalMods,
                ShiftedRange[] shifted) {
            TreeViewportDataImpl detail =
                    new TreeViewportDataImpl(barrageSubscription, rowStyleColumn, getColumns(), rowsAdded,
                            rowsRemoved, totalMods, shifted);
            detail.setOffset(this.serverViewport.getFirstRow());
            fireEvent(EVENT_UPDATED, detail);
        }
    }

    private void replaceSubscription(RebuildStep step) {
        // Perform steps required to remove the existing intermediate tickets.
        // Fall-through between steps is deliberate.
        switch (step) {
            case FILTER:
                if (filteredTable != null) {
                    filteredTable.release();
                    filteredTable = null;
                }
            case SORT:
                if (sortedTable != null) {
                    sortedTable.release();
                    sortedTable = null;
                }
            case HIERARCHICAL_TABLE_VIEW:
                if (viewTicket != null) {
                    viewTicket.release();
                    viewTicket = null;
                }

                // In all of the above cases, we replace the subscription
                if (stream != null) {
                    stream.then(stream -> {
                        stream.close();
                        return null;
                    });
                    stream = null;
                }
                break;
            case SUBSCRIPTION:
                // If it exists, adjust the existing subscription, otherwise create a new one
                if (stream != null) {
                    stream.then(subscription -> {
                        subscription.setViewport(firstRow, lastRow, Js.uncheckedCast(columns), (double) updateInterval);
                        return null;
                    });
                    return;
                }
        }

        Promise<TreeSubscription> stream = Promise.resolve(defer())
                .then(ignore -> {
                    makeKeyTable();
                    TicketAndPromise filter = prepareFilter();
                    TicketAndPromise sort = prepareSort(filter);
                    TicketAndPromise view = makeView(sort);
                    return Promise.all(
                            keyTable,
                            filter.promise(),
                            sort.promise(),
                            view.promise());
                })
                .then(results -> {
                    BitSet columnsBitset = makeColumnSubscriptionBitset();
                    RangeSet range = RangeSet.ofRange((long) (double) firstRow, (long) (double) lastRow);

                    boolean alwaysFireEvent = this.alwaysFireNextEvent;
                    this.alwaysFireNextEvent = false;

                    JsLog.debug("Sending tree table request", this,
                            LazyString.of(() -> widget.getTicket().getTicket_asB64()),
                            columnsBitset,
                            range,
                            alwaysFireEvent);

                    ClientTableState state = new ClientTableState(connection,
                            new TableTicket(viewTicket.ticket().getTicket_asU8()), (callback, newState, metadata) -> {
                                callback.apply("fail, trees dont reconnect like this", null);
                            }, "");
                    ExportedTableCreationResponse def = new ExportedTableCreationResponse();
                    HierarchicalTableDescriptor treeDescriptor =
                            HierarchicalTableDescriptor.deserializeBinary(widget.getDataAsU8());
                    def.setSchemaHeader(treeDescriptor.getSnapshotSchema_asU8());
                    def.setResultId(new TableReference());
                    def.getResultId().setTicket(viewTicket.ticket());
                    state.applyTableCreationResponse(def);

                    TreeSubscription subscription = new TreeSubscription(state, connection);

                    subscription.addEventListener(TreeSubscription.EVENT_UPDATED,
                            (Event<AbstractTableSubscription.UpdateEventData> data) -> {
                                TreeSubscription.TreeViewportDataImpl detail =
                                        (TreeSubscription.TreeViewportDataImpl) data.getDetail();

                                handleUpdate(nextSort, nextFilters, detail, alwaysFireEvent);
                            });
                    return Promise.resolve(subscription);
                });
        stream.catch_(err -> {
            // if this is the active attempt at a subscription, report the error
            if (this.stream == stream) {
                failureHandled(err.toString());
            }
            return Promise.reject(err);
        });
        this.stream = stream;
    }

    /**
     * Instead of a micro-task between chained promises, insert a regular task so that control is returned to the
     * browser long enough to prevent the UI hanging.
     */
    private <T> IThenable.ThenOnFulfilledCallbackFn<T, T> defer() {
        return val -> new Promise<>((resolve, reject) -> {
            DomGlobal.setTimeout(ignoreArgs -> resolve.onInvoke(val), 0);
        });
    }

    private void handleUpdate(List<Sort> nextSort, List<FilterCondition> nextFilters,
            TreeSubscription.TreeViewportDataImpl viewportData, boolean alwaysFireEvent) {
        JsLog.debug("tree table response arrived", viewportData);
        if (closed) {
            // ignore
            return;
        }

        // TODO #3310 if requested to fire the event, or if the data has changed in some way, fire the event
        final boolean fireEvent = true;

        this.currentViewportData = viewportData;

        this.sorts = nextSort;
        this.filters = nextFilters;

        if (fireEvent) {
            fireEvent(EVENT_UPDATED, viewportData);
        }
    }

    private BitSet makeColumnSubscriptionBitset() {
        // Build the bitset for the columns that are needed to get the data, style, and maintain structure
        BitSet columnsBitset = new BitSet(tableDefinition.getColumns().length);
        Arrays.stream(columns).flatMapToInt(Column::getRequiredColumns).forEach(columnsBitset::set);
        Arrays.stream(columns)
                .map(Column::getName)
                .map(sourceColumns::get)
                .filter(Objects::nonNull)
                .flatMapToInt(Column::getRequiredColumns)
                .forEach(columnsBitset::set);
        for (ColumnDefinition column : tableDefinition.getColumns()) {
            if (column.isForRow()) {
                columnsBitset.set(column.getColumnIndex());
            }
        }
        columnsBitset.set(rowDepthCol.getIndex());
        columnsBitset.set(rowExpandedCol.getIndex());
        keyColumns.forEach((p0, p1) -> {
            columnsBitset.set(p0.getIndex());
            return null;
        });
        return columnsBitset;
    }

    private void replaceKeyTable() {
        if (keyTable != null) {
            keyTable.then(t -> {
                t.close();
                return null;
            });
            keyTable = null;
        }
        replaceSubscription(RebuildStep.HIERARCHICAL_TABLE_VIEW);
    }

    private void replaceKeyTableData(double action) {
        keyTableData = new Object[keyColumns.length + 2][1];
        int i = keyColumns.length;
        Js.<JsArray<Double>>cast(keyTableData[i++]).setAt(0, (double) 0);
        Js.<JsArray<Double>>cast(keyTableData[i++]).setAt(0, action);
        replaceKeyTable();
    }

    /**
     * Expands the given node, so that its children are visible when they are in the viewport. The parameter can be the
     * row index, or the row object itself. The second parameter is a boolean value, false by default, specifying if the
     * row and all descendants should be fully expanded. Equivalent to `setExpanded(row, true)` with an optional third
     * boolean parameter.
     *
     * @param row
     * @param expandDescendants
     */
    public void expand(RowReferenceUnion row, @JsOptional Boolean expandDescendants) {
        setExpanded(row, true, expandDescendants);
    }

    /**
     * Collapses the given node, so that its children and descendants are not visible in the size or the viewport. The
     * parameter can be the row index, or the row object itself. Equivalent to <b>setExpanded(row, false, false)</b>.
     *
     * @param row
     */
    public void collapse(RowReferenceUnion row) {
        setExpanded(row, false, false);
    }

    @TsUnion
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface RowReferenceUnion {
        @JsOverlay
        static RowReferenceUnion of(@DoNotAutobox Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default boolean isTreeRow() {
            return this instanceof TreeSubscription.TreeRowImpl;
        }

        @JsOverlay
        default boolean isNumber() {
            return (Object) this instanceof Double;
        }

        @JsOverlay
        @TsUnionMember
        default TreeViewportData.TreeRow asTreeRow() {
            return Js.cast(this);
        }

        @JsOverlay
        @TsUnionMember
        default double asNumber() {
            return Js.asDouble(this);
        }
    }

    /**
     * Specifies if the given node should be expanded or collapsed. If this node has children, and the value is changed,
     * the size of the table will change. If node is to be expanded and the third parameter, <b>expandDescendants</b>,
     * is true, then its children will also be expanded.
     *
     * @param row the row to expand or collapse, either the absolute row index or the row object
     * @param isExpanded true to expand the row, false to collapse
     * @param expandDescendants true to expand the row and all descendants, false to expand only the row, defaults to
     *        false
     */
    public void setExpanded(RowReferenceUnion row, boolean isExpanded, @JsOptional Boolean expandDescendants) {
        // TODO check row number is within bounds
        final double action;
        if (!isExpanded) {
            action = ACTION_COLLAPSE;
        } else if (expandDescendants == Boolean.TRUE) {
            action = ACTION_EXPAND_WITH_DESCENDENTS;
        } else {
            action = ACTION_EXPAND;
        }

        final TreeSubscription.TreeRowImpl r;
        if (row.isNumber()) {
            r = (TreeSubscription.TreeRowImpl) currentViewportData.getRows()
                    .getAt((int) (row.asNumber() - currentViewportData.getOffset()));
        } else if (row.isTreeRow()) {
            r = (TreeSubscription.TreeRowImpl) row.asTreeRow();
        } else {
            throw new IllegalArgumentException("row parameter must be an index or a row");
        }

        r.appendKeyData(keyTableData, action);
        replaceKeyTable();
    }

    public void expandAll() {
        replaceKeyTableData(ACTION_EXPAND_WITH_DESCENDENTS);
    }

    public void collapseAll() {
        replaceKeyTableData(ACTION_EXPAND);
    }

    /**
     * Tests if the specified row is expanded.
     * 
     * @param row the row to test, either the absolute row index or the row object
     * @return boolean true if the row is expanded, false otherwise
     */
    public boolean isExpanded(RowReferenceUnion row) {
        final TreeSubscription.TreeRowImpl r;
        if (row.isNumber()) {
            r = (TreeSubscription.TreeRowImpl) currentViewportData.getRows()
                    .getAt((int) (row.asNumber() - currentViewportData.getOffset()));
        } else if (row.isTreeRow()) {
            r = (TreeSubscription.TreeRowImpl) row.asTreeRow();
        } else {
            throw new IllegalArgumentException("row parameter must be an index or a row");
        }

        return r.isExpanded();
    }

    // JsTable-like methods
    public void setViewport(double firstRow, double lastRow, @JsOptional @JsNullable JsArray<Column> columns,
            @JsNullable @JsOptional Double updateInterval) {
        this.firstRow = firstRow;
        this.lastRow = lastRow;
        this.columns = columns != null ? Js.uncheckedCast(columns.slice()) : visibleColumns;
        this.updateInterval = updateInterval == null ? 1000 : (int) (double) updateInterval;

        replaceSubscription(RebuildStep.SUBSCRIPTION);
    }

    public Promise<@TsTypeRef(TreeViewportData.class) Object> getViewportData() {
        LazyPromise<Object> promise = new LazyPromise<>();

        if (currentViewportData == null) {
            // only one of these two will fire, and when they do, they'll remove both handlers.
            addEventListenerOneShot(
                    EventPair.of(EVENT_UPDATED, e -> promise.succeed(currentViewportData)),
                    EventPair.of(EVENT_REQUEST_FAILED, promise::fail));
        } else {
            promise.succeed(currentViewportData);
        }
        return promise.asPromise();
    }

    /**
     * True if this table has been closed.
     *
     * @return boolean
     */
    @JsProperty(name = "isClosed")
    public boolean isClosed() {
        return closed;
    }

    /**
     * True if this table may receive updates from the server, including size changed events, updated events after
     * initial snapshot.
     *
     * @return boolean
     */
    @JsProperty(name = "isRefreshing")
    public boolean isRefreshing() {
        return isRefreshing;
    }

    /**
     * Indicates that the table will no longer be used, and server resources can be freed.
     */
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        JsLog.debug("Closing tree table", this);

        connection.unregisterSimpleReconnectable(this);

        connection.releaseTicket(widget.getTicket());

        if (filteredTable != null) {
            filteredTable.release();
            filteredTable = null;
        }
        if (sortedTable != null) {
            sortedTable.release();
            sortedTable = null;
        }
        if (viewTicket != null) {
            viewTicket.release();
            viewTicket = null;
        }
        if (stream != null) {
            stream.then(stream -> {
                stream.close();
                return null;
            });
            stream = null;
        }

        if (sourceTable.isAvailable()) {
            sourceTable.get().then(table -> {
                table.close();
                return null;
            });
        }
    }

    @Override
    @JsIgnore
    public TypedTicket typedTicket() {
        return widget.typedTicket();
    }

    /**
     * Applies the given sort to all levels of the tree. Returns the previous sort in use.
     *
     * @param sort
     * @return {@link Sort} array
     */
    @SuppressWarnings("unusable-by-js")
    public JsArray<Sort> applySort(Sort[] sort) {
        for (int i = 0; i < sort.length; i++) {
            if (sort[i].getDirection().equalsIgnoreCase("reverse")) {
                throw new IllegalArgumentException("Tree Tables do no support reverse");
            }
        }
        nextSort = Arrays.asList(sort);

        replaceSubscription(RebuildStep.SORT);

        return getSort();
    }

    /**
     * Applies the given filter to the contents of the tree in such a way that if any node is visible, then any parent
     * node will be visible as well even if that parent node would not normally be visible due to the filter's
     * condition. Returns the previous sort in use.
     *
     * @param filter
     * @return {@link FilterCondition} array
     */
    @SuppressWarnings("unusable-by-js")
    public JsArray<FilterCondition> applyFilter(FilterCondition[] filter) {
        nextFilters = Arrays.asList(filter);

        replaceSubscription(RebuildStep.FILTER);

        return getFilter();
    }

    @JsProperty
    @JsNullable
    public String getDescription() {
        return tableDefinition.getAttributes().getDescription();
    }

    @JsProperty
    @JsNullable
    public JsLayoutHints getLayoutHints() {
        if (layoutHints == null) {
            createLayoutHints();
        }
        return layoutHints;
    }

    private void createLayoutHints() {
        String hintsString = tableDefinition.getAttributes().getLayoutHints();
        JsLayoutHints jsHints = new JsLayoutHints();
        if (hintsString == null) {
            layoutHints = null;
        } else {
            layoutHints = jsHints.parse(hintsString);
        }
    }

    /**
     * The current number of rows given the table's contents and the various expand/collapse states of each node. (No
     * totalSize is provided at this time; its definition becomes unclear between roll-up and tree tables, especially
     * when considering collapse/expand states).
     *
     * @return double
     */
    @JsProperty
    public double getSize() {
        // read the size of the last tree response
        if (currentViewportData != null) {
            return currentViewportData.getTreeSize();
        }
        return -1;// not ready yet
    }

    /**
     * The current sort configuration of this Tree Table
     * 
     * @return {@link Sort} array.
     */
    @JsProperty
    public JsArray<Sort> getSort() {
        return JsItr.slice(sorts);
    }

    /**
     * The current filter configuration of this Tree Table.
     * 
     * @return {@link FilterCondition} array
     */
    @JsProperty
    public JsArray<FilterCondition> getFilter() {
        return JsItr.slice(filters);
    }

    /**
     * The columns that can be shown in this Tree Table.
     * 
     * @return {@link Column} array
     */
    @JsProperty
    public JsArray<Column> getColumns() {
        return Js.uncheckedCast(visibleColumns);
    }

    /**
     * a column with the given name, or throws an exception if it cannot be found
     * 
     * @param key
     * @return {@link Column}
     */
    public Column findColumn(String key) {
        Column c = columnsByName.get(key);
        if (c == null) {
            throw new NoSuchElementException(key);
        }
        return c;
    }

    /**
     * True if this is a roll-up and will provide the original rows that make up each grouping.
     * 
     * @return boolean
     */
    @JsProperty
    public boolean isIncludeConstituents() {
        return Arrays.stream(tableDefinition.getColumns()).anyMatch(ColumnDefinition::isRollupConstituentNodeColumn);
    }

    @JsProperty
    public JsArray<Column> getGroupedColumns() {
        return groupedColumns;
    }

    /**
     * an array with all of the named columns in order, or throws an exception if one cannot be found.
     * 
     * @param keys
     * @return {@link Column} array
     */
    public Column[] findColumns(String[] keys) {
        Column[] result = new Column[keys.length];
        for (int i = 0; i < keys.length; i++) {
            result[i] = findColumn(keys[i]);
        }
        return result;
    }

    /**
     * Provides Table-like selectDistinct functionality, but with a few quirks, since it is only fetching the distinct
     * values for the given columns in the source table:
     * <ul>
     * <li>Rollups may make no sense, since values are aggregated.</li>
     * <li>Values found on orphaned (and removed) nodes will show up in the resulting table, even though they are not in
     * the tree.</li>
     * <li>Values found on parent nodes which are only present in the tree since a child is visible will not be present
     * in the resulting table.</li>
     * </ul>
     */
    public Promise<JsTable> selectDistinct(Column[] columns) {
        return sourceTable.get().then(t -> {
            // if this is the first time it is used, it might not be filtered correctly, so check that the filters match
            // up.
            if (!t.getFilter().asList().equals(getFilter().asList())) {
                t.applyFilter(getFilter().asArray(new FilterCondition[0]));
            }
            return Promise.resolve(t.selectDistinct(columns));
        });
    }

    public Promise<JsTotalsTableConfig> getTotalsTableConfig() {
        // we want to communicate to the JS dev that there is no default config, so we allow
        // returning null here, rather than a default config. They can then easily build a
        // default config, but without this ability, there is no way to indicate that the
        // config omitted a totals table
        return sourceTable.get().then(t -> Promise.resolve(t.getTotalsTableConfig()));
    }

    public Promise<JsTotalsTable> getTotalsTable(@JsOptional Object config) {
        return sourceTable.get().then(t -> {
            // if this is the first time it is used, it might not be filtered correctly, so check that the filters match
            // up.
            if (!t.getFilter().asList().equals(getFilter().asList())) {
                t.applyFilter(getFilter().asArray(new FilterCondition[0]));
            }
            return Promise.resolve(t.getTotalsTable(config));
        });
    }

    public Promise<JsTotalsTable> getGrandTotalsTable(@JsOptional Object config) {
        return sourceTable.get().then(t -> Promise.resolve(t.getGrandTotalsTable(config)));
    }

    // TODO core#279 restore this with protobuf once smartkey has some pb-based analog
    // private static final int SERIALIZED_VERSION = 1;
    //
    // @JsMethod
    // public String saveExpandedState() {
    // if (expandedMap.get(Key.root()).expandedChildren.isEmpty()) {
    // return "";//empty string means nothing expanded, don't bother with preamble
    // }
    // KeySerializer serializer = new KeySerializer_Impl();
    // TypeSerializer typeSerializer = serializer.createSerializer();
    // final StringSerializationStreamWriter writer = new StringSerializationStreamWriter(typeSerializer);
    // writer.prepareToWrite();
    // writer.writeInt(SERIALIZED_VERSION);
    // writer.writeString(typeSerializer.getChecksum());
    //
    // // Starting from the root node, write a node, its child count, then call this recursively.
    // // Normally we would write the key first, but the root key is a special case where we don't
    // // do this.
    // try {
    // writeTreeNode(serializer, writer, Key.root());
    // } catch (SerializationException e) {
    // throw new IllegalStateException("Failed to serialize content: " + e.getMessage(), e);
    // }
    //
    // return writer.toString();
    // }
    //
    // private void writeTreeNode(KeySerializer serializer, SerializationStreamWriter writer, Key key) throws
    // SerializationException {
    // TreeNodeState node = expandedMap.get(key);
    // if (node == null) {
    // writer.writeInt(0);
    // return;
    // }
    // writer.writeInt(node.expandedChildren.size());
    // for (Key child : node.expandedChildren) {
    // serializer.write(child, writer);
    // writeTreeNode(serializer, writer, child);
    // }
    // }
    //
    // @JsMethod
    // public void restoreExpandedState(String nodesToRestore) throws SerializationException {
    // // sanity check that nothing has been expanded yet so we can safely do this
    // if (!expandedMap.get(Key.root()).expandedChildren.isEmpty()) {
    // throw new IllegalArgumentException("Tree already has expanded children, ignoring restoreExpandedState call");
    // }
    // if (nodesToRestore.isEmpty()) {
    // // no work to do, empty set of items expanded
    // return;
    // }
    // KeySerializer serializer = new KeySerializer_Impl();
    // TypeSerializer typeSerializer = serializer.createSerializer();
    // StringSerializationStreamReader reader = new StringSerializationStreamReader(typeSerializer, nodesToRestore);
    // int vers = reader.readInt();
    // if (vers != SERIALIZED_VERSION) {
    // throw new IllegalArgumentException("Failed to deserialize, current version doesn't match the serialized data.
    // Expected version " + SERIALIZED_VERSION + ", actual version " + vers);
    // }
    // String checksum = reader.readString();
    // if (!checksum.equals(typeSerializer.getChecksum())) {
    // throw new IllegalArgumentException("Failed to deserialize, current type definition doesn't match the serialized
    // data. Expected: " + typeSerializer.getChecksum() + ", actual: " + checksum);
    // }
    //
    // // read each key, assuming root as the first key
    // readTreeNode(serializer, reader, Key.root());
    // }
    //
    // private void readTreeNode(KeySerializer serializer, SerializationStreamReader reader, Key key) throws
    // SerializationException {
    // TreeNodeState node = expandedMap.get(key);
    // int count = reader.readInt();
    // for (int i = 0; i < count; i++) {
    // Key child = serializer.read(reader);
    // node.expand(child);
    // readTreeNode(serializer, reader, child);
    // }
    // }

    /**
     * a new copy of this treetable, so it can be sorted and filtered separately, and maintain a different viewport.
     * Unlike Table, this will _not_ copy the filter or sort, since tree table viewport semantics differ, and without a
     * viewport set, the treetable doesn't evaluate these settings, and they aren't readable on the properties. Expanded
     * state is also not copied.
     *
     * @return Promise of dh.TreeTable
     */
    public Promise<JsTreeTable> copy() {
        return connection.newState((c, state, metadata) -> {
            // connection.getServer().reexport(this.baseTable.getHandle(), state.getHandle(), c);
            throw new UnsupportedOperationException("reexport");// probably not needed at all with new session
                                                                // mechanism?
        }, "reexport for tree.copy()")
                .refetch(this, connection.metadata())
                .then(state -> Promise.resolve(new JsTreeTable(connection, widget)));
    }
}
