/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.Uint8Array;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf.Message;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf.MessageHeader;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf.RecordBatch;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.schema_generated.org.apache.arrow.flatbuf.Schema;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.Builder;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageSnapshotOptions;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.ColumnConversionMode;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.Hierarchicaltable_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.RollupDescriptorDetails;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.hierarchicaltabledescriptor.DetailsCase;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.BarrageUtils;
import io.deephaven.web.client.api.barrage.def.ColumnDefinition;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.filter.FilterCondition;
import io.deephaven.web.client.api.subscription.ViewportData;
import io.deephaven.web.client.api.subscription.ViewportRow;
import io.deephaven.web.client.api.tree.JsTreeTable.TreeViewportData.TreeRow;
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.client.fu.JsItr;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.data.*;
import io.deephaven.web.shared.data.columns.ColumnData;
import io.deephaven.web.shared.data.treetable.TableDetails;
import io.deephaven.web.shared.data.treetable.TreeTableRequest;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.util.*;

import static io.deephaven.web.client.api.barrage.BarrageUtils.makeUint8ArrayFromBitset;
import static io.deephaven.web.client.api.barrage.BarrageUtils.serializeRanges;
import static io.deephaven.web.client.api.subscription.ViewportData.NO_ROW_FORMAT_COLUMN;

/**
 * Behaves like a JsTable externally, but data, state, and viewports are managed by an entirely different mechanism, and
 * so reimplemented here.
 *
 * Any time a change is made, we build a new request and send it to the server, and wait for the updated state.
 *
 * Semantics around getting updates from the server are slightly different - we don't "unset" the viewport here after
 * operations are performed, but encourage the client code to re-set them to the desired position.
 *
 * The "__Hierarchical_Children" column should generally be left out of the UI, but is provided for debugging purposes.
 *
 * The table size will be -1 until a viewport has been fetched.
 */
public class JsTreeTable extends HasEventHandling {
    @JsProperty(namespace = "dh.TreeTable")
    public static final String EVENT_UPDATED = "updated",
            EVENT_DISCONNECT = "disconnect",
            EVENT_RECONNECT = "reconnect",
            EVENT_RECONNECTFAILED = "reconnectfailed";

    private static final String TABLE_AGGREGATION_COLUMN_PREFIX = "Rollup_";

    private static final String[] TABLE_AGGREGATIONS = new String[] {JsAggregationOperation.COUNT};

    private enum NextSnapshotState {
        TIMER_RUNNING, QUERY_WHEN_TIMER_ENDS, QUERY_WHEN_UPDATE_SEEN
    }

    private static boolean isTableAggregationColumn(String columnName) {
        if (!columnName.startsWith(TABLE_AGGREGATION_COLUMN_PREFIX)) {
            return false;
        }
        String suffix = columnName.substring(TABLE_AGGREGATION_COLUMN_PREFIX.length());
        for (int i = 0; i < TABLE_AGGREGATIONS.length; i++) {
            if (TABLE_AGGREGATIONS[i].equals(suffix)) {
                return true;
            }
        }
        return false;
    }

    class TreeViewportData {
        private final Boolean[] expandedColumn;
        private final int[] depthColumn;
        private final double offset;

        private final JsArray<Column> columns;
        private final JsArray<TreeRow> rows;

        private final ColumnData[] columnData;
        private final Object[] data;

        private TreeViewportData(RangeSet includedRows, ColumnData[] dataColumns, Column[] columns) {
            this.offset = includedRows.getFirstRow();
            this.columns = JsObject.freeze(Js.cast(Js.<JsArray<Column>>uncheckedCast(columns).slice()));

            // Unlike ViewportData, assume that we own this copy of the data and can mutate at will. As such,
            // we'll just clean the data that the requested columns know about for now.
            // TODO to improve this, we can have synthetic columns to handle data that wasn't requested/expected,
            // and then can share code with ViewportData
            this.data = new Object[dataColumns.length];

            columnData = dataColumns;

            expandedColumn = (Boolean[]) ViewportData.cleanData(columnData[rowExpandedCol.getIndex()].getData(), rowExpandedCol);
            depthColumn = (int[]) ViewportData.cleanData(columnData[rowDepthCol.getIndex()].getData(), rowDepthCol);

            int constituentDepth = keyColumns.length + 1;
            for (int i = 0; i < columns.length; i++) {
                Column c = columns[i];
                int index = c.getIndex();
                if (dataColumns[index] == null) {
                    // no data for this column, not requested in viewport
                    continue;
                }

                // clean the data, since it will be exposed to the client
                data[index] = ViewportData.cleanData(dataColumns[index].getData(), c);
                if (c.getStyleColumnIndex() != null) {
                    data[c.getStyleColumnIndex()] = dataColumns[c.getStyleColumnIndex()].getData();
                }
                if (c.getFormatStringColumnIndex() != null) {
                    data[c.getFormatStringColumnIndex()] = dataColumns[c.getFormatStringColumnIndex()].getData();
                }

                // if there is a matching constituent column array, clean it and copy from it
                Column sourceColumn = sourceColumns.get(c.getName());
                if (sourceColumn != null) {
                    ColumnData constituentColumn = dataColumns[sourceColumn.getIndex()];
                    if (constituentColumn != null) {
                        JsArray<Any> cleanConstituentColumn =
                                Js.uncheckedCast(ViewportData.cleanData(constituentColumn.getData(), sourceColumn));
                        // Overwrite the data with constituent values, if any
                        // We use cleanConstituentColumn to find max item rather than data[index], since we
                        // are okay stopping at the last constituent value, in case the server sends shorter
                        // arrays.
                        for (int rowIndex = 0; rowIndex < cleanConstituentColumn.length; rowIndex++) {
                            if (depthColumn[rowIndex] == constituentDepth)
                            Js.asArrayLike(data[index]).setAt(rowIndex, cleanConstituentColumn.getAt(rowIndex));
                        }
                    }
                }
            }
            if (rowFormatColumn != NO_ROW_FORMAT_COLUMN) {
                data[rowFormatColumn] = dataColumns[rowFormatColumn].getData();
            }

            rows = new JsArray<>();
            for (int i = 0; i < includedRows.size(); i++) {
                rows.push(new TreeRow(i, data, data[rowFormatColumn]));
            }
        }

        @JsProperty
        public double getOffset() {
            return offset;
        }

        @JsProperty
        public JsArray<Column> getColumns() {
            return columns;
        }

        @JsProperty
        public JsArray<TreeRow> getRows() {
            return rows;
        }


        /**
         * Checks if two viewport data objects contain the same data, based on comparing four fields, none of which can
         * be null.
         * <ul>
         * <li>The columnData array is the actual contents of the rows - if these change, clearly we have different
         * data</li>
         * <li>The constituentColumns array is the actual contents of the constituent column values, mapped by their
         * column name - if these change, the visible data will be different</li>
         * <li>The childPresence field is the main other change that could happen, where a node changes its status of
         * having children.</li>
         * <li>The keyColumn contents, if they change, might require the UI to change the "expanded" property. This is a
         * stretch, but it could happen.</li>
         * <li>The parentColumn is even more of a stretch, but if it were to change without the item itself moving its
         * position in the viewport, the depth (and indentation in the UI) would visibly change.</li>
         * </ul>
         *
         * We aren't interested in the other fields - rows and data are just a different way to see the original data in
         * the columnData field, and if either offset or columns change, we would automatically force an event to happen
         * anyway, so that we confirm to the user that the change happened (even if the visible data didn't change for
         * some reason).
         */
        public boolean containsSameDataAs(TreeViewportData that) {
            return Arrays.equals(keyColumn, that.keyColumn)
                    && Arrays.equals(parentKeyColumn, that.parentKeyColumn)
                    && childPresence.equals(that.childPresence)
                    && Arrays.equals(columnData, that.columnData)
                    && Objects.equals(constituentColumns, that.constituentColumns);
        }

        /**
         * Row implementation that also provides additional read-only properties.
         */
        class TreeRow extends ViewportRow {
            public TreeRow(int offsetInSnapshot, Object[] dataColumns, Object rowStyleColumn) {
                super(offsetInSnapshot, dataColumns, rowStyleColumn);
            }

            @JsProperty(name = "isExpanded")
            public boolean isExpanded() {
                return expandedColumn[offsetInSnapshot] == Boolean.TRUE;
            }

            @JsProperty(name = "hasChildren")
            public boolean hasChildren() {
                return expandedColumn[offsetInSnapshot] != null;
            }

            @JsProperty(name = "depth")
            public int depth() {
                return depthColumn[offsetInSnapshot];
            }
        }
    }

//    /**
//     * Tracks state of a given table that is part of the tree. Updates from the server in the form of a TableDetails
//     * object are folded into this as needed
//     */
//    class TreeNodeState {
//        private Key key;
//        private Set<Key> expandedChildren = new HashSet<>();
//
//        public TreeNodeState(Key key) {
//            this.key = key;
//        }
//
//        public void expand(Key child) {
//            if (expandedChildren.add(child)) {
//                TreeNodeState childState = new TreeNodeState(child);
//                expandedMap.put(child, childState);
//                JsLog.debug("user expanded ", child);
//                scheduleSnapshotQuery(false);
//            }
//        }
//
//        public void collapse(Key child) {
//            if (expandedChildren.remove(child)) {
//                JsLog.debug("user collapsed ", child);
//
//                // from the entire tree's map, remove this child's expanded details,
//                // then iteratively remove each expanded child in the same way
//                List<TreeNodeState> removed = new ArrayList<>();
//                removed.add(expandedMap.remove(child));
//                while (!removed.isEmpty()) {
//                    TreeNodeState treeNode = removed.remove(0);
//                    for (Key expandedChild : treeNode.expandedChildren) {
//                        removed.add(expandedMap.remove(expandedChild));
//                    }
//                }
//
//                scheduleSnapshotQuery(false);
//            }
//        }
//
//        public TableDetails toTableDetails() {
//            TableDetails output = new TableDetails();
//            output.setKey(key);
//            output.setChildren(expandedChildren.toArray(new Key[0]));
//            return output;
//        }
//    }

    private final WorkerConnection connection;

    private final JsWidget widget;
    private final HierarchicalTableDescriptor treeDescriptor;
    private final InitialTableDefinition tableDefinition;
    private final Column[] visibleColumns;
    private final Map<String, Column> columnsByName = new HashMap<>();
    private final int rowFormatColumn;
    private final Map<String, Column> sourceColumns = new HashMap<>();
    private final JsArray<Column> keyColumns;
    private Column rowDepthCol;
    private Column rowExpandedCol;

    private final JsLazy<Promise<JsTable>> sourceTable;

    private List<FilterCondition> filters = new ArrayList<>();
    private List<Sort> sorts = new ArrayList<>();

    // the "next" set of filters/sorts that we'll use. these either are "==" to the above fields, or are scheduled
    // to replace them soon.
    private List<FilterCondition> nextFilters = new ArrayList<>();
    private List<Sort> nextSort = new ArrayList<>();

    // viewport information
    private Double firstRow;
    private Double lastRow;
    private Column[] columns;
    private int updateInterval = 1000;

//    private final Map<Key, TreeNodeState> expandedMap = new HashMap<>();

    private JsRunnable queuedOperations = null;
    private TreeTableRequest.TreeRequestOperation[] nextRequestOps = new TreeTableRequest.TreeRequestOperation[0];

    private Double viewportUpdateTimeoutId;
    private boolean scheduled;
    private boolean running;
//    private TreeTableResult lastResult;
    private TreeViewportData currentViewportData;

    private boolean alwaysFireNextEvent = false;

    private NextSnapshotState nextSnapshotState;

    private JsArray<Column> groupedColumns = null;

    private boolean closed = false;

    public JsTreeTable(WorkerConnection workerConnection, JsWidget widget) {
        this.connection = workerConnection;
        this.widget = widget;
        this.treeDescriptor = HierarchicalTableDescriptor.deserializeBinary(widget.getDataAsU8());

        Uint8Array flightSchemaMessage = treeDescriptor.getSnapshotDefinitionSchema_asU8();
        Schema schema = BarrageUtils.readSchemaMessage(flightSchemaMessage);

        this.tableDefinition = BarrageUtils.readTableDefinition(schema);
        Column[] columns = new Column[0];
        Map<String, ColumnDefinition> columnDefsByName = tableDefinition.getColumnsByName();
        int rowFormatColumn = -1;

        for (ColumnDefinition definition : tableDefinition.getColumns()) {
            Column column = definition.makeJsColumn(columns.length, columnDefsByName);
            if (definition.isForRow()) {
                assert rowFormatColumn == -1;
                rowFormatColumn = definition.getColumnIndex();
                continue;
            }

            if (definition.isVisible()) {
                columns[columns.length] = column;
            } else if (definition.getName().equals(treeDescriptor.getRowDepthColumn())) {
                rowDepthCol = column;
            } else if (definition.getName().equals(treeDescriptor.getRowExpandedColumn())) {
                rowExpandedCol = column;
            }
        }
        this.rowFormatColumn = rowFormatColumn;
        this.visibleColumns = JsObject.freeze(columns);
        for (int i = 0; i < visibleColumns.length; i++) {
            Column column = visibleColumns[i];
            columnsByName.put(column.getName(), column);
        }
        keyColumns = treeDescriptor.getExpandByColumnsList().map((p0, p1, p2) -> columnsByName.get(p0));

        if (treeDescriptor.getDetailsCase() == DetailsCase.ROLLUP) {
            RollupDescriptorDetails rollupDef = treeDescriptor.getRollup();

            final Set<String> presentNames = new HashSet<>();
            rollupDef.getOutputInputColumnPairsList().forEach((pair, index, arr) -> {
                String[] split = pair.split("=");
                String rollupColName = split[0];
                presentNames.add(rollupColName);
                String sourceColName = split[1];
                if (rollupDef.getLeafNodeType() == Hierarchicaltable_pb.RollupNodeType.getCONSTITUENT()
                        && !isTableAggregationColumn(sourceColName)) {
                    Column sourceColumn = findColumn(sourceColName);
                    String sourceType = sourceColumn.getType();
                    findColumn(rollupColName).setConstituentType(sourceType);
                    this.sourceColumns.put(rollupColName, sourceColumn);
                }

                return null;
            });
            groupedColumns = JsObject
                    .freeze(getColumns()
                            .filter((column, index, array) -> !presentNames.contains(column.getName())));
            if (rollupDef.getLeafNodeType() == Hierarchicaltable_pb.RollupNodeType.getCONSTITUENT()) {
                groupedColumns.forEach((column, index, array) -> {
                    column.setConstituentType(null);
                    return null;
                });
            }

        } else {
            assert treeDescriptor.getDetailsCase() == DetailsCase.TREE : "Unexpected type " + treeDescriptor.getDetailsCase();
        }

        sourceTable = JsLazy.of(() -> {
            throw new UnsupportedOperationException("source table isn't yet supported");
        });
    }

    /**
     * Requests an update as soon as possible, canceling any future update but scheduling a new one. Any change in
     * viewport or configuration should call this.
     *
     * @param alwaysFireEvent force the updated event to fire based on this scheduled snapshot, even if the data is the
     *        same as before
     */
    private void scheduleSnapshotQuery(boolean alwaysFireEvent) {
        // track if we should force the event to fire when the data comes back, even if there is no change
        alwaysFireNextEvent |= alwaysFireEvent;

        if (running) {
            // already in flight, so when the response comes back make sure the queue is non-empty so we schedule
            // another
            if (queuedOperations == null) {
                queuedOperations = JsRunnable.doNothing();
            }
            return;
        }
        if (scheduled) {
            // next one is already set up, upgrade requirement for event, if need be
            return;
        }
        scheduled = true;
        if (viewportUpdateTimeoutId != null) {
            DomGlobal.clearTimeout(viewportUpdateTimeoutId);
        }
        viewportUpdateTimeoutId = DomGlobal.setTimeout(p0 -> this.snapshotQuery(), 30);
    }

    /**
     * Requests an update from the server. Should only be called by itself and scheduleSnapshotQuery.
     */
    private void snapshotQuery() {
        if (closed) {
            JsLog.warn("TreeTable already closed, cannot perform operation");
            return;
        }
        // we're running now, so mark as already scheduled
        scheduled = false;

        if (firstRow == null || lastRow == null) {
            // viewport not set yet, don't start loading data
            return;
        }

        // clear any size update, we're getting data either way, future updates should be noted
        nextSnapshotState = NextSnapshotState.TIMER_RUNNING;
        viewportUpdateTimeoutId = DomGlobal.setTimeout(p -> {
            // timer has elapsed, we'll actually perform our regular check only if a change was seen since we were
            // started
            if (nextSnapshotState == NextSnapshotState.QUERY_WHEN_TIMER_ENDS) {
                scheduleSnapshotQuery(false);
            } else {
                // otherwise, this means that we should query right away if any update arrives
                nextSnapshotState = NextSnapshotState.QUERY_WHEN_UPDATE_SEEN;
            }
        }, updateInterval);
        if (connection.isUsable()) {
            running = true;
            TreeTableRequest query = buildQuery();
            Column[] queryColumns = this.columns;

            boolean alwaysFireEvent = this.alwaysFireNextEvent;
            this.alwaysFireNextEvent = false;

            JsLog.debug("Sending tree table request", this, LazyString.of(() -> widget.getTicket().getTicket_asB64()),
                    query,
                    alwaysFireEvent);
            BiDiStream<FlightData, FlightData> doExchange = connection.<FlightData, FlightData>streamFactory().create(
                    headers -> connection.flightServiceClient().doExchange(headers),
                    (first, headers) -> connection.browserFlightServiceClient().openDoExchange(first, headers),
                    (next, headers, c) -> connection.browserFlightServiceClient().nextDoExchange(next, headers,
                            c::apply),
                    new FlightData());

            FlightData snapshotRequestWrapper = new FlightData();
            Builder doGetRequest = new Builder(1024);
            double columnsOffset = BarrageSubscriptionRequest.createColumnsVector(doGetRequest,
                    makeUint8ArrayFromBitset(query.getColumns()));
            double viewportOffset = BarrageSubscriptionRequest.createViewportVector(doGetRequest, serializeRanges(
                    Collections.singleton(RangeSet.ofRange(query.getViewportStart(), query.getViewportEnd()))));
            double serializationOptionsOffset = BarrageSnapshotOptions
                    .createBarrageSnapshotOptions(doGetRequest, ColumnConversionMode.Stringify, true, 0, 0);
            double tableTicketOffset =
                    BarrageSubscriptionRequest.createTicketVector(doGetRequest, widget.getDataAsU8());
            BarrageSnapshotRequest.startBarrageSnapshotRequest(doGetRequest);
            BarrageSnapshotRequest.addTicket(doGetRequest, tableTicketOffset);
            BarrageSnapshotRequest.addColumns(doGetRequest, columnsOffset);
            BarrageSnapshotRequest.addSnapshotOptions(doGetRequest, serializationOptionsOffset);
            BarrageSnapshotRequest.addViewport(doGetRequest, viewportOffset);
            doGetRequest.finish(BarrageSnapshotRequest.endBarrageSnapshotRequest(doGetRequest));

            snapshotRequestWrapper.setAppMetadata(
                    BarrageUtils.wrapMessage(doGetRequest, BarrageMessageType.BarrageSnapshotRequest));
            doExchange.send(snapshotRequestWrapper);

            // hang on to this to get server-sent snapshots, cancel when needed
            // doExchange.end();

            String[] columnTypes = Arrays.stream(tableDefinition.getColumns())
                    .map(ColumnDefinition::getType)
                    .toArray(String[]::new);
            doExchange.onData(flightData -> {
                Message message = Message.getRootAsMessage(new ByteBuffer(flightData.getDataHeader_asU8()));
                if (message.headerType() == MessageHeader.Schema) {
                    // ignore for now, we'll handle this later
                    return;
                }
                assert message.headerType() == MessageHeader.RecordBatch;
                RecordBatch header = message.header(new RecordBatch());
                Uint8Array appMetadataBytes = flightData.getAppMetadata_asU8();
                BarrageUpdateMetadata update = null;
                if (appMetadataBytes.length != 0) {
                    BarrageMessageWrapper barrageMessageWrapper =
                            BarrageMessageWrapper.getRootAsBarrageMessageWrapper(
                                    new io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer(
                                            appMetadataBytes));

                    update = BarrageUpdateMetadata.getRootAsBarrageUpdateMetadata(
                            new ByteBuffer(
                                    new Uint8Array(barrageMessageWrapper.msgPayloadArray())));
                }
                TableSnapshot snapshot = BarrageUtils.createSnapshot(header,
                        BarrageUtils.typedArrayToLittleEndianByteBuffer(flightData.getDataBody_asU8()), update,
                        true,
                        columnTypes);

                final RangeSet includedRows = snapshot.getIncludedRows();
                TreeViewportData vd = new TreeViewportData(
                        includedRows,
                        snapshot.getDataColumns(),
                        columns);

                try {
                    handleUpdate(queryColumns, nextSort, nextFilters, vd, alwaysFireEvent);
                } finally {
                    running = false;
                    if (queuedOperations != null) {
                        // Something changed since our last request, start another one.
                        // We allow skipping the event since whatever enqueued the operation should have passed true
                        // if needed, or it could have been a slow reply from the server, etc.
                        scheduleSnapshotQuery(false);
                    }
                }
            });

        } else {
            JsLog.debug("Connection not ready, skipping tree table poll", this);
        }
    }

    private void handleUpdate(Column[] columns, List<Sort> nextSort, List<FilterCondition> nextFilters,
                              TreeViewportData viewportData, boolean alwaysFireEvent) {
        JsLog.debug("tree table response arrived", viewportData);
        lastResult = result;
        if (closed) {
            if (viewportUpdateTimeoutId != null) {
                DomGlobal.clearTimeout(viewportUpdateTimeoutId);
            }

            return;
        }

        // if requested to fire the event, or if the data has changed in some way, fire the event
        final boolean fireEvent = alwaysFireEvent || !viewportData.containsSameDataAs(currentViewportData);

        this.currentViewportData = viewportData;

//        Set<Key> presentKeys = new HashSet<>();
//        for (int i = 0; i < result.getTableDetails().length; i++) {
//            TableDetails detail = result.getTableDetails()[i];
//            TreeNodeState treeNodeState = expandedMap.get(detail.getKey());
//
//            Set<Key> expandedChildren = new HashSet<>(Arrays.asList(detail.getChildren()));
//            expandedChildren.retainAll(expandedMap.keySet());
//            treeNodeState.expandedChildren = expandedChildren;
//
//            presentKeys.add(detail.getKey());
//        }
//        expandedMap.entrySet().removeIf(e -> {
//            if (!presentKeys.contains(e.getKey())) {
//                return true;
//            } else {
//                return false;
//            }
//        });

        this.sorts = nextSort;
        this.filters = nextFilters;

        if (fireEvent) {
            CustomEventInit updatedEvent = CustomEventInit.create();
            updatedEvent.setDetail(viewportData);
            fireEvent(EVENT_UPDATED, updatedEvent);
        }
    }

    /**
     * Creates a request object based on the current state of request info. We don't presently build this ahead of time
     * and maintain it as things change, but instead read from the rest of the tree's state to decide what to build.
     *
     * Sort is always assigned, since a node could be expanded, might now have children (and the server needs to know
     * how to sort it), etc - the server will only sort individual "children" tables lazily, so this must always be
     * provided.
     *
     * Filters are sent when the filter changes, or when something else changes that will result in rebuilding one or
     * more children trees - the two cases that exist today are changing the sort, or reconnecting to the server. When
     * filters are changed, the bookkeeping is done automatically, but for other purposes the releaseAllNodes helper
     * method should be used to both release nodes and indicate that when refetched the filter may need to be applied
     * again as well.
     */
    private TreeTableRequest buildQuery() {
        TreeTableRequest request = new TreeTableRequest();


        // before building, evaluate all queued operations
        if (queuedOperations != null) {
            queuedOperations.run();
            queuedOperations = null;
        }

        // if any of those operations asks for a close, just do the close and skip the rest
        if (Arrays.asList(nextRequestOps).contains(TreeTableRequest.TreeRequestOperation.Close)) {
            closed = true;
            request.setIncludedOps(
                    new TreeTableRequest.TreeRequestOperation[] {TreeTableRequest.TreeRequestOperation.Close});
            request.setExpandedNodes(new TableDetails[0]);
            request.setSorts(new SortDescriptor[0]);
            request.setFilters(new FilterDescriptor[0]);
            request.setColumns(new BitSet());
            return request;
        }

        request.setExpandedNodes(
                expandedMap.values().stream().map(TreeNodeState::toTableDetails).toArray(TableDetails[]::new));

//        final int hierarchicalChildrenColumnIndex = Arrays.stream(tableDefinition.getColumns())
//                .filter(col -> col.getName()
//                        .equals(tableDefinition.getAttributes().getTreeHierarchicalColumnName()))
//                .mapToInt(ColumnDefinition::getColumnIndex)
//                .findFirst()
//                .orElseThrow(() -> new IllegalStateException("TreeTable definition has no hierarchy column"));
//
//        request.setKeyColumn(hierarchicalChildrenColumnIndex);

        // avoid sending filters unless they changed for smaller overhead on requests, there is no need to recompute
        // filters on child tables, only on the root table
        if (!filters.equals(nextFilters)) {
            request.setFilters(
                    nextFilters.stream().map(FilterCondition::makeDescriptor).toArray(FilterDescriptor[]::new));
        } else {
            request.setFilters(new FilterDescriptor[0]);
        }

        // always include the sort setup, the viewport content could have changed in practically any way
        request.setSorts(nextSort.stream().map(Sort::makeDescriptor).toArray(SortDescriptor[]::new));

        BitSet columnsBitset = new BitSet(tableDefinition.getColumns().length);
        Arrays.stream(columns).flatMapToInt(Column::getRequiredColumns).forEach(columnsBitset::set);
        for (ColumnDefinition column : tableDefinition.getColumns()) {
            if (column.isForRow()) {
                columnsBitset.set(column.getColumnIndex());
            }
        }
//        columnsBitset.set(hierarchicalChildrenColumnIndex);
        request.setColumns(columnsBitset);
        request.setViewportEnd((long) (double) lastRow);
        request.setViewportStart((long) (double) firstRow);

        request.setIncludedOps(nextRequestOps);
        nextRequestOps = new TreeTableRequest.TreeRequestOperation[0];

        return request;
    }

    private void enqueue(TreeTableRequest.TreeRequestOperation operation, JsRunnable r) {
        if (queuedOperations != null) {
            JsRunnable old = queuedOperations;
            queuedOperations = () -> {
                nextRequestOps[nextRequestOps.length] = operation;
                old.run();
                r.run();
            };
        } else {
            queuedOperations = () -> {
                nextRequestOps[nextRequestOps.length] = operation;
                r.run();
            };
        }
    }

    @JsMethod
    public void expand(Object row) {
        setExpanded(row, true);
    }

    @JsMethod
    public void collapse(Object row) {
        setExpanded(row, false);
    }

    @JsMethod
    public void setExpanded(Object row, boolean isExpanded) {
        // TODO check row number is within bounds

        final TreeRow r;
        if (row instanceof Double) {
            r = currentViewportData.rows.getAt((int) ((double) row - lastResult.getSnapshotStart()));
        } else if (row instanceof TreeRow) {
            r = (TreeRow) row;
        } else {
            throw new IllegalArgumentException("row parameter must be an index or a row");
        }

//        Key myRowKey = r.myKey();
//        Key parentRowKey = r.parentKey();
        JsLog.debug("setExpanded enqueued");
        // With the keys collected for the currently-visible item to expand/collapse, we can enqueue an operation
        // to modify that node
        enqueue(isExpanded ? TreeTableRequest.TreeRequestOperation.Expand
                : TreeTableRequest.TreeRequestOperation.Contract, () -> {
                    TreeNodeState node = expandedMap.get(parentRowKey);
                    if (node == null) {
                        throw new IllegalStateException("Parent isn't available, can't manipulate child: setExpanded("
                                + row + ", " + isExpanded + ")");
                    }
                    if (isExpanded) {
                        node.expand(myRowKey);
                    } else {
                        node.collapse(myRowKey);
                    }
                });
        scheduleSnapshotQuery(true);
    }

    @JsMethod
    public boolean isExpanded(Object row) {
        if (row instanceof Double) {
            row = currentViewportData.rows.getAt((int) ((double) row - lastResult.getSnapshotStart()));
        } else if (!(row instanceof TreeRow)) {
            throw new IllegalArgumentException("row parameter must be an index or a row");
        }
        TreeRow r = (TreeRow) row;

        return r.isExpanded();
    }

    // JsTable-like methods
    @JsMethod
    public void setViewport(double firstRow, double lastRow, @JsOptional JsArray<Column> columns,
            @JsOptional Double updateInterval) {
        this.firstRow = firstRow;
        this.lastRow = lastRow;
        this.columns = columns != null ? Js.uncheckedCast(columns.slice()) : visibleColumns;
        this.updateInterval = updateInterval == null ? 1000 : (int) (double) updateInterval;

        scheduleSnapshotQuery(true);
    }

    @JsMethod
    public Promise<TreeViewportData> getViewportData() {
        LazyPromise<TreeViewportData> promise = new LazyPromise<>();

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

    @JsMethod
    public void close() {
        JsLog.debug("Closing tree table", this);
        // perform one final call, mark the object as closed
        enqueue(TreeTableRequest.TreeRequestOperation.Close, JsRunnable.doNothing());
        scheduleSnapshotQuery(false);
    }

    @JsMethod
    @SuppressWarnings("unusable-by-js")
    public JsArray<Sort> applySort(Sort[] sort) {
        for (int i = 0; i < sort.length; i++) {
            if (sort[i].getDirection().equalsIgnoreCase("reverse")) {
                throw new IllegalArgumentException("Tree Tables do no support reverse");
            }
        }
        enqueue(TreeTableRequest.TreeRequestOperation.SortChanged, () -> {
            this.nextSort = Arrays.asList(sort);

            releaseAllNodes(true);
        });
        scheduleSnapshotQuery(true);

        return getSort();
    }

    /**
     * Helper to indicate that table handles in nodes are no longer valid and need to be rebuilt. Parameter
     * "invalidateFilters" should be true in all cases except when setting a new filter, and will result in the current
     * filters being sent to the server to correctly filter any re-created handles.
     */
    private void releaseAllNodes(boolean invalidateFilters) {
        if (invalidateFilters) {
            // clear the "current" filters - either we didn't need them (already blank), or this
            // will make them unequal and we'll send them to the server for this request.
            this.filters = new ArrayList<>();
        }
    }

    @JsMethod
    @SuppressWarnings("unusable-by-js")
    public JsArray<FilterCondition> applyFilter(FilterCondition[] filter) {
        enqueue(TreeTableRequest.TreeRequestOperation.FilterChanged, () -> {
            this.nextFilters = Arrays.asList(filter);

            // apply the filter to the source table (used to produce totals table, limits our
            // size changed events)
            sourceTable.get().then(t -> {
                t.applyFilter(filter);
                return null;
            });

            // don't invalidate filters, we've already replaced them
            releaseAllNodes(false);
        });
        scheduleSnapshotQuery(true);

        return getFilter();
    }

    @JsProperty
    public String getDescription() {
        return tableDefinition.getAttributes().getDescription();
    }

    @JsProperty
    public double getSize() {
        // read the size of the last tree response
        if (lastResult != null) {
            return (double) lastResult.getTreeSize();
        }
        return -1;// not ready yet
    }

    @JsProperty
    public JsArray<Sort> getSort() {
        return JsItr.slice(sorts);
    }

    @JsProperty
    public JsArray<FilterCondition> getFilter() {
        return JsItr.slice(filters);
    }

    @JsProperty
    public JsArray<Column> getColumns() {
        return Js.uncheckedCast(visibleColumns);
    }

    @JsMethod
    public Column findColumn(String key) {
        Column c = columnsByName.get(key);
        if (c == null) {
            throw new NoSuchElementException(key);
        }
        return c;
    }

    @JsProperty
    public boolean isIncludeConstituents() {
        if (treeDescriptor.hasRollup()) {
            return treeDescriptor.getRollup().getLeafNodeType() == Hierarchicaltable_pb.RollupNodeType.getCONSTITUENT();
        }
        return false;
    }

    @JsProperty
    public JsArray<Column> getGroupedColumns() {
        return groupedColumns;
    }

    @JsMethod
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
     * <li>Values found on orphaned (and remvoed) nodes will show up in the resulting table, even though they are not in
     * the tree.</li>
     * <li>Values found on parent nodes which are only present in the tree since a child is visible will not be present
     * in the resulting table.</li>
     * </ul>
     */
    @JsMethod
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

    @JsMethod
    public Promise<JsTotalsTableConfig> getTotalsTableConfig() {
        // we want to communicate to the JS dev that there is no default config, so we allow
        // returning null here, rather than a default config. They can then easily build a
        // default config, but without this ability, there is no way to indicate that the
        // config omitted a totals table
        return sourceTable.get().then(t -> Promise.resolve(t.getTotalsTableConfig()));
    }

    @JsMethod
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

    @JsMethod
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

    @JsMethod
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
