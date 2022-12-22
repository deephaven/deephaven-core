/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.Uint8Array;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
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
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.ColumnConversionMode;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.Hierarchicaltable_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableApplyRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.HierarchicalTableViewRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.RollupDescriptorDetails;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.hierarchicaltabledescriptor.DetailsCase;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Condition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
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
import io.deephaven.web.shared.data.treetable.TreeTableRequest;
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
        private final double treeSize;

        private final JsArray<Column> columns;
        private final JsArray<TreeRow> rows;

        private final ColumnData[] columnData;
        private final Object[] data;

        private TreeViewportData(RangeSet includedRows, double treeSize, ColumnData[] dataColumns, Column[] columns) {
            this.offset = includedRows.getFirstRow();
            this.treeSize = treeSize;
            this.columns = JsObject.freeze(Js.cast(Js.<JsArray<Column>>uncheckedCast(columns).slice()));

            // Unlike ViewportData, assume that we own this copy of the data and can mutate at will. As such,
            // we'll just clean the data that the requested columns know about for now.
            // TODO to improve this, we can have synthetic columns to handle data that wasn't requested/expected,
            // and then can share code with ViewportData
            this.data = new Object[dataColumns.length];

            columnData = dataColumns;

            expandedColumn =
                    (Boolean[]) ViewportData.cleanData(columnData[rowExpandedCol.getIndex()].getData(), rowExpandedCol);
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

        public double getTreeSize() {
            return treeSize;
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

    private final WorkerConnection connection;

    // This group of fields represent the underlying state of the original HierarchicalTable
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
    private final JsArray<Column> groupedColumns;

    // The source JsTable behind the original HierarchicalTable, lazily built at this time
    private final JsLazy<Promise<JsTable>> sourceTable;

    // The current filter and sort state
    private List<FilterCondition> filters = new ArrayList<>();
    private List<Sort> sorts = new ArrayList<>();
    private Promise<Ticket> filteredTable;
    private Promise<Ticket> sortedTable;

    private Promise<Ticket> viewTicket;
    private Promise<BiDiStream<?, ?>> stream;

    // the "next" set of filters/sorts that we'll use. these either are "==" to the above fields, or are scheduled
    // to replace them soon.
    private List<FilterCondition> nextFilters = new ArrayList<>();
    private List<Sort> nextSort = new ArrayList<>();

    // viewport information
    private Double firstRow;
    private Double lastRow;
    private Column[] columns;
    private int updateInterval = 1000;

    private TreeViewportData currentViewportData;

    private boolean alwaysFireNextEvent = false;

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
            assert treeDescriptor.getDetailsCase() == DetailsCase.TREE
                    : "Unexpected type " + treeDescriptor.getDetailsCase();
            groupedColumns = null;
        }

        sourceTable = JsLazy.of(() -> {
            throw new UnsupportedOperationException("source table isn't yet supported");
        });
    }

    private Promise<Ticket> prepareFilter() {
        if (filteredTable != null) {
            return filteredTable;
        }
        if (nextFilters.isEmpty()) {
            return Promise.resolve(widget.getTicket());
        }
        Ticket ticket = connection.getConfig().newTicket();
        filteredTable = Callbacks.grpcUnaryPromise(c -> {

            HierarchicalTableApplyRequest applyFilter = new HierarchicalTableApplyRequest();
            applyFilter.setFiltersList(
                    nextFilters.stream().map(FilterCondition::makeDescriptor).toArray(Condition[]::new));
            applyFilter.setInputHierarchicalTableId(widget.getTicket());
            applyFilter.setResultHierarchicalTableId(ticket);
            connection.hierarchicalTableServiceClient().apply(applyFilter, connection.metadata(), c::apply);
        }).then(ignore -> Promise.resolve(ticket));
        return filteredTable;
    }

    private Promise<Ticket> prepareSort(Ticket prevTicket) {
        if (sortedTable != null) {
            return sortedTable;
        }
        if (nextSort.isEmpty()) {
            return Promise.resolve(prevTicket);
        }
        Ticket ticket = connection.getConfig().newTicket();
        sortedTable = Callbacks.grpcUnaryPromise(c -> {

            HierarchicalTableApplyRequest applyFilter = new HierarchicalTableApplyRequest();
            applyFilter.setSortsList(nextSort.stream().map(Sort::makeDescriptor).toArray(
                    io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SortDescriptor[]::new));
            applyFilter.setInputHierarchicalTableId(prevTicket);
            applyFilter.setResultHierarchicalTableId(ticket);
            connection.hierarchicalTableServiceClient().apply(applyFilter, connection.metadata(), c::apply);
        }).then(ignore -> Promise.resolve(ticket));
        return sortedTable;
    }

    private Promise<Ticket> makeView(Ticket prevTicket) {
        if (viewTicket != null) {
            return viewTicket;
        }
        Ticket ticket = connection.getConfig().newTicket();
        viewTicket = Callbacks.grpcUnaryPromise(c -> {
            HierarchicalTableViewRequest viewRequest = new HierarchicalTableViewRequest();
            viewRequest.setHierarchicalTableId(prevTicket);
            viewRequest.setResultViewId(ticket);
            connection.hierarchicalTableServiceClient().view(viewRequest, connection.metadata(), c::apply);
        }).then(ignore -> Promise.resolve(ticket));
        return viewTicket;
    }

    private void replaceSubscription() {
        this.stream = Promise.resolve(defer())
                .then(ignore -> prepareFilter())
                .then(this::prepareSort)
                .then(this::makeView)
                .then(ticket -> {
                    TreeTableRequest query = buildQuery();
                    Column[] queryColumns = this.columns;

                    boolean alwaysFireEvent = this.alwaysFireNextEvent;
                    this.alwaysFireNextEvent = false;

                    JsLog.debug("Sending tree table request", this,
                            LazyString.of(() -> widget.getTicket().getTicket_asB64()),
                            query,
                            alwaysFireEvent);
                    BiDiStream<FlightData, FlightData> doExchange =
                            connection.<FlightData, FlightData>streamFactory().create(
                                    headers -> connection.flightServiceClient().doExchange(headers),
                                    (first, headers) -> connection.browserFlightServiceClient().openDoExchange(first,
                                            headers),
                                    (next, headers, c) -> connection.browserFlightServiceClient().nextDoExchange(next,
                                            headers,
                                            c::apply),
                                    new FlightData());

                    FlightData subscriptionRequestWrapper = new FlightData();
                    Builder doGetRequest = new Builder(1024);
                    double columnsOffset = BarrageSubscriptionRequest.createColumnsVector(doGetRequest,
                            makeUint8ArrayFromBitset(query.getColumns()));
                    double viewportOffset = BarrageSubscriptionRequest.createViewportVector(doGetRequest,
                            serializeRanges(
                                    Collections.singleton(
                                            RangeSet.ofRange(query.getViewportStart(), query.getViewportEnd()))));
                    double serializationOptionsOffset = BarrageSubscriptionOptions
                            .createBarrageSubscriptionOptions(doGetRequest, ColumnConversionMode.Stringify, true,
                                    updateInterval, 0, 0);
                    double tableTicketOffset =
                            BarrageSubscriptionRequest.createTicketVector(doGetRequest, widget.getDataAsU8());
                    BarrageSubscriptionRequest.startBarrageSubscriptionRequest(doGetRequest);
                    BarrageSubscriptionRequest.addTicket(doGetRequest, tableTicketOffset);
                    BarrageSubscriptionRequest.addColumns(doGetRequest, columnsOffset);
                    BarrageSubscriptionRequest.addSubscriptionOptions(doGetRequest, serializationOptionsOffset);
                    BarrageSubscriptionRequest.addViewport(doGetRequest, viewportOffset);
                    doGetRequest.finish(BarrageSubscriptionRequest.endBarrageSubscriptionRequest(doGetRequest));

                    subscriptionRequestWrapper.setAppMetadata(
                            BarrageUtils.wrapMessage(doGetRequest, BarrageMessageType.BarrageSubscriptionRequest));
                    doExchange.send(subscriptionRequestWrapper);

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
                                snapshot.getTableSize(),
                                snapshot.getDataColumns(),
                                queryColumns);

                        handleUpdate(nextSort, nextFilters, vd, alwaysFireEvent);
                    });
                    return Promise.resolve(doExchange);
                });
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
            TreeViewportData viewportData, boolean alwaysFireEvent) {
        JsLog.debug("tree table response arrived", viewportData);
        if (closed) {
            // ignore
            return;
        }

        // if requested to fire the event, or if the data has changed in some way, fire the event
        final boolean fireEvent = true;// alwaysFireEvent || !viewportData.containsSameDataAs(currentViewportData);

        this.currentViewportData = viewportData;

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

        // TODO DoPut the expanded nodes table
        // connection.newTable(
        // keyTableColumnNames,
        // keyTableColumnTypes,
        //
        // )

        // Build the bitset for the columns that are needed to get the data, style, and maintain structure
        BitSet columnsBitset = new BitSet(tableDefinition.getColumns().length);
        Arrays.stream(columns).flatMapToInt(Column::getRequiredColumns).forEach(columnsBitset::set);
        for (ColumnDefinition column : tableDefinition.getColumns()) {
            if (column.isForRow()) {
                columnsBitset.set(column.getColumnIndex());
            }
        }
        columnsBitset.set(rowDepthCol.getIndex());
        columnsBitset.set(rowExpandedCol.getIndex());
        keyColumns.forEach((p0, p1, p2) -> {
            columnsBitset.set(p0.getIndex());
            return null;
        });

        request.setColumns(columnsBitset);
        request.setViewportEnd((long) (double) lastRow);
        request.setViewportStart((long) (double) firstRow);

        return request;
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
            r = currentViewportData.rows.getAt((int) ((double) row - currentViewportData.offset));
        } else if (row instanceof TreeRow) {
            r = (TreeRow) row;
        } else {
            throw new IllegalArgumentException("row parameter must be an index or a row");
        }

        if (viewTicket != null) {
            viewTicket.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            viewTicket = null;
        }
        if (stream != null) {
            stream.then(stream -> {
                stream.end();
                return null;
            });
            stream = null;
        }
        replaceSubscription();
    }

    @JsMethod
    public boolean isExpanded(Object row) {
        if (row instanceof Double) {
            row = currentViewportData.rows.getAt((int) ((double) row - currentViewportData.offset));
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

        if (stream != null) {
            stream.then(stream -> {
                stream.end();
                return null;
            });
            stream = null;
        }
        replaceSubscription();
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

        connection.releaseTicket(widget.getTicket());

        if (filteredTable != null) {
            filteredTable.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            filteredTable = null;
        }
        if (sortedTable != null) {
            sortedTable.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            sortedTable = null;
        }
        if (viewTicket != null) {
            viewTicket.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            viewTicket = null;
        }
        if (stream != null) {
            stream.then(stream -> {
                stream.end();
                return null;
            });
            stream = null;
        }
    }

    @JsMethod
    @SuppressWarnings("unusable-by-js")
    public JsArray<Sort> applySort(Sort[] sort) {
        for (int i = 0; i < sort.length; i++) {
            if (sort[i].getDirection().equalsIgnoreCase("reverse")) {
                throw new IllegalArgumentException("Tree Tables do no support reverse");
            }
        }
        nextSort = Arrays.asList(sort);
        if (sortedTable != null) {
            sortedTable.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            sortedTable = null;
        }
        if (viewTicket != null) {
            viewTicket.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            viewTicket = null;
        }
        if (stream != null) {
            stream.then(stream -> {
                stream.end();
                return null;
            });
            stream = null;
        }

        replaceSubscription();

        return getSort();
    }

    @JsMethod
    @SuppressWarnings("unusable-by-js")
    public JsArray<FilterCondition> applyFilter(FilterCondition[] filter) {
        nextFilters = Arrays.asList(filter);
        if (filteredTable != null) {
            filteredTable.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            filteredTable = null;
        }
        if (sortedTable != null) {
            sortedTable.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            sortedTable = null;
        }
        if (viewTicket != null) {
            viewTicket.then(ticket -> {
                connection.releaseTicket(ticket);
                return null;
            });
            viewTicket = null;
        }
        if (stream != null) {
            stream.then(stream -> {
                stream.end();
                return null;
            });
            stream = null;
        }

        replaceSubscription();

        return getFilter();
    }

    @JsProperty
    public String getDescription() {
        return tableDefinition.getAttributes().getDescription();
    }

    @JsProperty
    public double getSize() {
        // read the size of the last tree response
        if (currentViewportData != null) {
            return currentViewportData.getTreeSize();
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
