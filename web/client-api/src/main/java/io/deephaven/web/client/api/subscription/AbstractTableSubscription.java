//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.google.flatbuffers.FlatBufferBuilder;
import com.vertispan.tsdefs.annotations.TsIgnore;
import elemental2.core.JsArray;
import elemental2.dom.CustomEventInit;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.ColumnConversionMode;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.Format;
import io.deephaven.web.client.api.HasEventHandling;
import io.deephaven.web.client.api.JsRangeSet;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.api.TableData;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.CompressedRangeSetReader;
import io.deephaven.web.client.api.barrage.WebBarrageMessage;
import io.deephaven.web.client.api.barrage.WebBarrageMessageReader;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.data.WebBarrageSubscription;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.fu.JsSettings;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;
import jsinterop.base.Js;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.BitSet;

/**
 * Superclass of various subscription types, allowing specific implementations to customize behavior for their needs.
 * <p>
 * Instances are not ready to use right away, owing to the fact that we need to wait both for the provided state to
 * resolve (so that we have the table schema, know what kind of subscription we will make, and know what column types
 * will be resolves), and because until the subscription has finished being set up, we will not have received the size
 * of the table. When closed, it cannot be reused again.
 * <p>
 * This is also a base class for types exposed to JS.
 * <p>
 * This is a rough analog of the JVM's {@code BarrageSubscriptionImpl} class. In contrast to the JVM code, this is
 * exposed to api consumers, rather than wrapping in a Table type, as it handles the barrage stream and provides events
 * that client code can listen to.
 */
public abstract class AbstractTableSubscription extends HasEventHandling {
    /**
     * Indicates that some new data is available on the client, either an initial snapshot or a delta update. The
     * <b>detail</b> field of the event will contain a TableSubscriptionEventData detailing what has changed, or
     * allowing access to the entire range of items currently in the subscribed columns.
     */
    public static final String EVENT_UPDATED = "updated";

    public enum Status {
        /** Waiting for some prerequisite before we can use it for the first time. */
        STARTING,
        /** Successfully created, not waiting for any messages to be accurate. */
        ACTIVE,
        /** Waiting for an update to return from being active to being active again. */
        PENDING_UPDATE,
        /** Closed or otherwise stopped, cannot be used again. */
        DONE;
    }

    private final ClientTableState state;
    private final WorkerConnection connection;
    protected final int rowStyleColumn;
    private JsArray<Column> columns;
    private BitSet columnBitSet;
    protected RangeSet viewportRowSet;
    private boolean isReverseViewport;
    private BarrageSubscriptionOptions options;

    private BiDiStream<FlightData, FlightData> doExchange;
    protected WebBarrageSubscription barrageSubscription;

    protected Status status = Status.STARTING;

    private String failMsg;

    public AbstractTableSubscription(ClientTableState state, WorkerConnection connection) {
        state.retain(this);
        this.state = state;
        this.connection = connection;
        rowStyleColumn = state.getRowFormatColumn() == null ? TableData.NO_ROW_FORMAT_COLUMN
                : state.getRowFormatColumn().getIndex();

        revive();
    }

    /**
     * Creates the connection to the server. Used on initial connection, and for viewport reconnects.
     */
    protected void revive() {
        // Once the state is running, set up the actual subscription
        // Don't let subscription be used again, table failed and user will have already gotten an error elsewhere
        state.onRunning(s -> {
            if (status != Status.STARTING) {
                // already closed
                return;
            }
            WebBarrageSubscription.ViewportChangedHandler viewportChangedHandler = this::onViewportChange;
            WebBarrageSubscription.DataChangedHandler dataChangedHandler = this::onDataChanged;

            status = Status.ACTIVE;
            this.barrageSubscription =
                    WebBarrageSubscription.subscribe(state, viewportChangedHandler, dataChangedHandler);

            doExchange =
                    connection.<FlightData, FlightData>streamFactory().create(
                            headers -> connection.flightServiceClient().doExchange(headers),
                            (first, headers) -> connection.browserFlightServiceClient().openDoExchange(first, headers),
                            (next, headers, c) -> connection.browserFlightServiceClient().nextDoExchange(next, headers,
                                    c::apply),
                            new FlightData());

            doExchange.onData(this::onFlightData);
            doExchange.onEnd(this::onStreamEnd);

            sendFirstSubscriptionRequest();
        },
                // If the upstream table fails, kill the subscription
                this::fail,
                // If the upstream table is closed, its because this subscription released it, do nothing
                JsRunnable.doNothing());
    }

    public Status getStatus() {
        return status;
    }

    protected static FlatBufferBuilder subscriptionRequest(byte[] tableTicket, BitSet columns,
            @Nullable RangeSet viewport,
            BarrageSubscriptionOptions options, boolean isReverseViewport) {
        FlatBufferBuilder sub = new FlatBufferBuilder(1024);
        int colOffset = BarrageSubscriptionRequest.createColumnsVector(sub, columns.toByteArray());
        int viewportOffset = 0;
        if (viewport != null) {
            viewportOffset =
                    BarrageSubscriptionRequest.createViewportVector(sub, CompressedRangeSetReader.writeRange(viewport));
        }
        int optionsOffset = options.appendTo(sub);
        int tableTicketOffset = BarrageSubscriptionRequest.createTicketVector(sub, tableTicket);
        BarrageSubscriptionRequest.startBarrageSubscriptionRequest(sub);
        BarrageSubscriptionRequest.addColumns(sub, colOffset);
        BarrageSubscriptionRequest.addViewport(sub, viewportOffset);
        BarrageSubscriptionRequest.addSubscriptionOptions(sub, optionsOffset);
        BarrageSubscriptionRequest.addTicket(sub, tableTicketOffset);
        BarrageSubscriptionRequest.addReverseViewport(sub, isReverseViewport);
        sub.finish(BarrageSubscriptionRequest.endBarrageSubscriptionRequest(sub));

        return sub;
    }

    protected abstract void sendFirstSubscriptionRequest();

    protected void sendBarrageSubscriptionRequest(RangeSet viewport, JsArray<Column> columns, Double updateIntervalMs,
            boolean isReverseViewport) {
        if (status == Status.DONE) {
            if (failMsg == null) {
                throw new IllegalStateException("Can't change subscription, already closed");
            } else {
                throw new IllegalStateException("Can't change subscription, already failed: " + failMsg);
            }
        }
        status = Status.PENDING_UPDATE;
        this.columns = columns;
        this.viewportRowSet = viewport;
        this.columnBitSet = makeColumnBitset(columns);
        this.isReverseViewport = isReverseViewport;
        this.options = BarrageSubscriptionOptions.builder()
                .batchSize(WebBarrageSubscription.BATCH_SIZE)
                .maxMessageSize(WebBarrageSubscription.MAX_MESSAGE_SIZE)
                .columnConversionMode(ColumnConversionMode.Stringify)
                .minUpdateIntervalMs(updateIntervalMs == null ? 0 : (int) (double) updateIntervalMs)
                .columnsAsList(false)// TODO(deephaven-core#5927) flip this to true
                .useDeephavenNulls(true)
                .build();
        FlatBufferBuilder request = subscriptionRequest(
                Js.uncheckedCast(state.getHandle().getTicket()),
                columnBitSet,
                viewport,
                options,
                isReverseViewport);
        FlightData subscriptionRequest = new FlightData();
        subscriptionRequest
                .setAppMetadata(WebBarrageUtils.wrapMessage(request, BarrageMessageType.BarrageSubscriptionRequest));
        doExchange.send(subscriptionRequest);
    }

    protected BitSet makeColumnBitset(JsArray<Column> columns) {
        return state.makeBitset(Js.uncheckedCast(columns));
    }

    public ClientTableState state() {
        return state;
    }

    protected WorkerConnection connection() {
        return connection;
    }

    protected boolean isSubscriptionReady() {
        return status == Status.ACTIVE;
    }

    public double size() {
        if (status == Status.ACTIVE) {
            return barrageSubscription.getCurrentRowSet().size();
        }
        if (status == Status.DONE) {
            throw new IllegalStateException("Can't read size when already closed");
        }
        return state.getSize();
    }

    private void onDataChanged(RangeSet rowsAdded, RangeSet rowsRemoved, RangeSet totalMods, ShiftedRange[] shifted,
            BitSet modifiedColumnSet) {
        if (!isSubscriptionReady()) {
            return;
        }

        notifyUpdate(rowsAdded, rowsRemoved, totalMods, shifted);
    }

    protected void notifyUpdate(RangeSet rowsAdded, RangeSet rowsRemoved, RangeSet totalMods, ShiftedRange[] shifted) {
        // TODO (deephaven-core#2435) Rewrite shifts as adds/removed/modifies
        UpdateEventData detail = new SubscriptionEventData(
                barrageSubscription,
                rowStyleColumn,
                columns,
                rowsAdded,
                rowsRemoved,
                totalMods,
                shifted);
        CustomEventInit<UpdateEventData> event = CustomEventInit.create();
        event.setDetail(detail);
        fireEvent(TableSubscription.EVENT_UPDATED, event);
    }

    public static class SubscriptionRow implements TableData.Row {
        private final WebBarrageSubscription subscription;
        private final int rowStyleColumn;
        protected final long index;
        public LongWrapper indexCached;

        public SubscriptionRow(WebBarrageSubscription subscription, int rowStyleColumn, long index) {
            this.subscription = subscription;
            this.rowStyleColumn = rowStyleColumn;
            this.index = index;
        }

        @Override
        public LongWrapper getIndex() {
            if (indexCached == null) {
                indexCached = LongWrapper.of(index);
            }
            return indexCached;
        }

        @Override
        public Any get(Column column) {
            return subscription.getData(index, column.getIndex());
        }

        @Override
        public Format getFormat(Column column) {
            long cellColors = 0;
            long rowColors = 0;
            String numberFormat = null;
            String formatString = null;
            if (column.getStyleColumnIndex() != null) {
                LongWrapper wrapper = subscription.getData(index, column.getStyleColumnIndex()).uncheckedCast();
                cellColors = wrapper == null ? 0 : wrapper.getWrapped();
            }
            if (rowStyleColumn != TableData.NO_ROW_FORMAT_COLUMN) {
                LongWrapper wrapper = subscription.getData(index, rowStyleColumn).uncheckedCast();
                rowColors = wrapper == null ? 0 : wrapper.getWrapped();
            }
            if (column.getFormatStringColumnIndex() != null) {
                numberFormat = subscription.getData(index, column.getFormatStringColumnIndex()).uncheckedCast();
            }
            if (column.getFormatStringColumnIndex() != null) {
                formatString = subscription.getData(index, column.getFormatStringColumnIndex()).uncheckedCast();
            }
            return new Format(cellColors, rowColors, numberFormat, formatString);
        }
    }

    /**
     * TableData type for full table subscriptions.
     */
    @TsIgnore
    public static class SubscriptionEventData extends UpdateEventData implements ViewportData, SubscriptionTableData {
        public SubscriptionEventData(WebBarrageSubscription subscription, int rowStyleColumn, JsArray<Column> columns,
                RangeSet added, RangeSet removed, RangeSet modified, ShiftedRange[] shifted) {
            super(subscription, rowStyleColumn, columns, added, removed, modified, shifted);
        }

        @Override
        public JsRangeSet getAdded() {
            return added;
        }

        @Override
        public JsRangeSet getRemoved() {
            return removed;
        }

        @Override
        public JsRangeSet getModified() {
            return modified;
        }

        @Override
        public JsRangeSet getFullIndex() {
            return fullRowSet;
        }
    }

    /**
     * TableData type for viewport subscriptions.
     */
    @TsIgnore
    public static class ViewportEventData extends SubscriptionEventData {
        public ViewportEventData(WebBarrageSubscription subscription, int rowStyleColumn, JsArray<Column> columns,
                RangeSet added, RangeSet removed, RangeSet modified, ShiftedRange[] shifted) {
            super(subscription, rowStyleColumn, columns, added, removed, modified, shifted);
        }

        @Override
        public Any getData(long key, Column column) {
            return super.getData(fullRowSet.getRange().get(key), column);
        }

        @Override
        public Format getFormat(long index, Column column) {
            return super.getFormat(fullRowSet.getRange().get(index), column);
        }
    }


    /**
     * Base type to allow trees to extend from here separately from tables.
     */
    @TsIgnore
    public abstract static class UpdateEventData implements TableData {
        protected final WebBarrageSubscription subscription;
        private final int rowStyleColumn;
        private final JsArray<Column> columns;
        protected final JsRangeSet added;
        protected final JsRangeSet removed;
        protected final JsRangeSet modified;
        protected final JsRangeSet fullRowSet;

        // cached copy in case it was requested, could be requested again
        private JsArray<SubscriptionRow> allRows;

        private double offset;

        public UpdateEventData(WebBarrageSubscription subscription, int rowStyleColumn, JsArray<Column> columns,
                RangeSet added, RangeSet removed, RangeSet modified, ShiftedRange[] shifted) {
            this.subscription = subscription;
            this.rowStyleColumn = rowStyleColumn;
            this.columns = columns;
            this.added = new JsRangeSet(added);
            this.removed = new JsRangeSet(removed);
            this.modified = new JsRangeSet(modified);
            this.fullRowSet = new JsRangeSet(transformRowsetForConsumer(subscription.getCurrentRowSet(),
                    subscription.getServerViewport(), subscription.isReversed()));
        }

        // for ViewportData
        @JsProperty
        public Double getOffset() {
            return offset;
        }

        public void setOffset(double offset) {
            this.offset = offset;
        }

        @Override
        public JsArray<TableData.Row> getRows() {
            if (allRows == null) {
                allRows = new JsArray<>();
                fullRowSet.getRange().indexIterator().forEachRemaining((long index) -> {
                    allRows.push(makeRow(index));
                });
                if (JsSettings.isDevMode()) {
                    assert allRows.length == fullRowSet.getSize();
                }
            }
            return (JsArray<Row>) (JsArray) allRows;
        }

        protected SubscriptionRow makeRow(long index) {
            return new SubscriptionRow(subscription, rowStyleColumn, index);
        }

        @Override
        public Row get(int index) {
            return this.get((long) index);
        }

        @Override
        public Row get(long index) {
            return makeRow(index);
        }

        @Override
        public Any getData(int index, Column column) {
            return getData((long) index, column);
        }

        @Override
        public Any getData(long key, Column column) {
            return subscription.getData(key, column.getIndex());
        }

        @Override
        public Format getFormat(int index, Column column) {
            return getFormat((long) index, column);
        }

        @Override
        public Format getFormat(long index, Column column) {
            long cellColors = 0;
            long rowColors = 0;
            String numberFormat = null;
            String formatString = null;
            if (column.getStyleColumnIndex() != null) {
                LongWrapper wrapper = subscription.getData(index, column.getStyleColumnIndex()).uncheckedCast();
                cellColors = wrapper == null ? 0 : wrapper.getWrapped();
            }
            if (rowStyleColumn != NO_ROW_FORMAT_COLUMN) {
                LongWrapper wrapper = subscription.getData(index, column.getStyleColumnIndex()).uncheckedCast();
                rowColors = wrapper == null ? 0 : wrapper.getWrapped();
            }
            if (column.getFormatStringColumnIndex() != null) {
                numberFormat = subscription.getData(index, column.getFormatStringColumnIndex()).uncheckedCast();
            }
            if (column.getFormatStringColumnIndex() != null) {
                formatString = subscription.getData(index, column.getFormatStringColumnIndex()).uncheckedCast();
            }
            return new Format(cellColors, rowColors, numberFormat, formatString);
        }

        @Override
        public JsArray<Column> getColumns() {
            return columns;
        }
    }

    /**
     * If a viewport is in use, transforms the given rowset to position space based on that viewport.
     * 
     * @param rowSet the rowset to possibly transform
     * @return a transformed rowset
     */
    private static RangeSet transformRowsetForConsumer(RangeSet rowSet, @Nullable RangeSet viewport, boolean reversed) {
        if (viewport != null) {
            return rowSet.subsetForPositions(viewport, reversed);
        }
        return rowSet;
    }

    private void onViewportChange(RangeSet serverViewport, BitSet serverColumns, boolean serverReverseViewport) {
        boolean subscriptionReady = ((serverColumns == null && columnBitSet == null)
                || (serverColumns == null && columnBitSet.cardinality() == state.getTableDef().getColumns().length)
                || (serverColumns != null && serverColumns.equals(this.columnBitSet)))
                && (serverViewport == null && this.viewportRowSet == null
                        || (serverViewport != null && serverViewport.equals(this.viewportRowSet)))
                && serverReverseViewport == isReverseViewport;
        if (subscriptionReady) {
            status = Status.ACTIVE;
        }
    }

    private final WebBarrageMessageReader reader = new WebBarrageMessageReader();

    private void onFlightData(FlightData data) {
        WebBarrageMessage message;
        try {
            message = reader.parseFrom(options, state.chunkTypes(), state.columnTypes(), state.componentTypes(), data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (message != null) {
            // This payload resulted in an update to the table's contents, inform the subscription
            barrageSubscription.applyUpdates(message);
        }
    }

    protected void onStreamEnd(ResponseStreamWrapper.Status status) {
        if (this.status == Status.DONE) {
            return;
        }
        if (status.isTransportError()) {
            // If the subscription isn't closed and we hit a transport error, allow it to restart
            this.status = Status.STARTING;
        } else {
            // Subscription failed somehow, fire an event
            fail(status.getDetails());
        }
    }

    private void fail(String message) {
        failureHandled(message);
        this.status = Status.DONE;
        doExchange = null;
        failMsg = message;
    }

    /**
     * The columns that were subscribed to when this subscription was created
     *
     * @return {@link Column}
     */
    public JsArray<Column> getColumns() {
        return columns;
    }

    /**
     * Stops the subscription on the server.
     */
    public void close() {
        state.unretain(this);
        if (doExchange != null) {
            doExchange.end();
            doExchange.cancel();
        }
        status = Status.DONE;
    }
}
