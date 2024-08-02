//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.google.flatbuffers.FlatBufferBuilder;
import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
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
import io.deephaven.web.client.api.barrage.WebBarrageStreamReader;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.data.WebBarrageSubscription;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.fu.JsSettings;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
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
    @Deprecated // remove this, use status instead
    private boolean subscriptionReady;

    public AbstractTableSubscription(ClientTableState state, WorkerConnection connection) {
        state.retain(this);
        this.state = state;
        this.connection = connection;
        rowStyleColumn = state.getRowFormatColumn() == null ? TableData.NO_ROW_FORMAT_COLUMN
                : state.getRowFormatColumn().getIndex();

        // Once the state is running, set up the actual subscription
        state.onRunning(s -> {
            if (status != Status.STARTING) {
                // already closed
                return;
            }
            // TODO going to need "started change" so we don't let data escape when still updating
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
            // TODO handle stream ending, error
            doExchange.onEnd(this::onStreamEnd);

            sendFirstSubscriptionRequest();
        }, () -> {
            // TODO fail
        });

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
        assert status == Status.ACTIVE || status == Status.PENDING_UPDATE : status;
        this.columns = columns;
        this.viewportRowSet = viewport;
        this.columnBitSet = makeColumnBitset(columns);
        this.isReverseViewport = isReverseViewport;
        // TODO validate that we can change updateinterval
        this.options = BarrageSubscriptionOptions.builder()
                .batchSize(WebBarrageSubscription.BATCH_SIZE)
                .maxMessageSize(WebBarrageSubscription.MAX_MESSAGE_SIZE)
                .columnConversionMode(ColumnConversionMode.Stringify)
                .minUpdateIntervalMs(updateIntervalMs == null ? 0 : (int) (double) updateIntervalMs)
                .columnsAsList(false)// TODO flip this to true
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
        return subscriptionReady;
    }

    public double size() {
        if (status == Status.ACTIVE) {
            return barrageSubscription.getCurrentRowSet().size();
        }
        return state.getSize();
    }

    private void onDataChanged(RangeSet rowsAdded, RangeSet rowsRemoved, RangeSet totalMods, ShiftedRange[] shifted,
            BitSet modifiedColumnSet) {
        if (!subscriptionReady) {
            return;
        }

        // TODO if this was a snapshot (or subscriptionReady was false for some interval), we probably need to
        // notify of the entire table as a single big change
        notifyUpdate(rowsAdded, rowsRemoved, totalMods, shifted);
    }

    protected void notifyUpdate(RangeSet rowsAdded, RangeSet rowsRemoved, RangeSet totalMods, ShiftedRange[] shifted) {
        UpdateEventData detail = new UpdateEventData(
                barrageSubscription,
                rowStyleColumn,
                columns,
                transformRowsetForConsumer(rowsAdded, barrageSubscription.getServerViewport(),
                        barrageSubscription.isReversed()),
                transformRowsetForConsumer(rowsRemoved, barrageSubscription.getServerViewport(),
                        barrageSubscription.isReversed()),
                transformRowsetForConsumer(totalMods, barrageSubscription.getServerViewport(),
                        barrageSubscription.isReversed()),
                barrageSubscription.getServerViewport() != null ? null : shifted);
        CustomEventInit<UpdateEventData> event = CustomEventInit.create();
        event.setDetail(detail);
        fireEvent(TableSubscription.EVENT_UPDATED, event);
    }

    @TsInterface
    @TsName(namespace = "dh")
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
                cellColors = subscription.getData(index, column.getStyleColumnIndex()).uncheckedCast();
            }
            if (rowStyleColumn != TableData.NO_ROW_FORMAT_COLUMN) {
                rowColors = subscription.getData(index, rowStyleColumn).uncheckedCast();
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


    public static class UpdateEventData implements SubscriptionTableData, ViewportData {
        protected final WebBarrageSubscription subscription;
        private final int rowStyleColumn;
        private final JsArray<Column> columns;
        private final JsRangeSet added;
        private final JsRangeSet removed;
        private final JsRangeSet modified;

        // cached copy in case it was requested, could be requested again
        private JsArray<SubscriptionRow> allRows;

        // TODO expose this property only if this is a viewport
        public double offset;

        public UpdateEventData(WebBarrageSubscription subscription, int rowStyleColumn, JsArray<Column> columns,
                RangeSet added, RangeSet removed, RangeSet modified, ShiftedRange[] shifted) {
            this.subscription = subscription;
            this.rowStyleColumn = rowStyleColumn;
            this.columns = columns;
            this.added = new JsRangeSet(added);
            this.removed = new JsRangeSet(removed);
            this.modified = new JsRangeSet(modified);
        }

        /**
         * The position of the first returned row.
         *
         * @return double
         */
        @JsProperty
        public Double getOffset() {
            return offset;
        }

        @Override
        public JsArray<@TsTypeRef(SubscriptionRow.class) ? extends SubscriptionRow> getRows() {
            if (allRows == null) {
                allRows = new JsArray<>();
                RangeSet rowSet = subscription.getCurrentRowSet();
                RangeSet positions =
                        transformRowsetForConsumer(rowSet, subscription.getServerViewport(), subscription.isReversed());
                positions.indexIterator().forEachRemaining((long index) -> {
                    allRows.push(makeRow(index));
                });
                if (JsSettings.isDevMode()) {
                    assert allRows.length == positions.size();
                }
            }
            return allRows;
        }

        protected SubscriptionRow makeRow(long index) {
            return new SubscriptionRow(subscription, rowStyleColumn, index);
        }

        @Override
        public Row get(int index) {
            return this.get((long) index);
        }

        @Override
        public SubscriptionRow get(long index) {
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
                cellColors = subscription.getData(index, column.getStyleColumnIndex()).uncheckedCast();
            }
            if (rowStyleColumn != NO_ROW_FORMAT_COLUMN) {
                rowColors = subscription.getData(index, rowStyleColumn).uncheckedCast();
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
            return new JsRangeSet(subscription.getCurrentRowSet());
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
        subscriptionReady = ((serverColumns == null && columnBitSet == null)
                || (serverColumns == null && columnBitSet.cardinality() == state.getTableDef().getColumns().length)
                || (serverColumns != null && serverColumns.equals(this.columnBitSet)))
                && (serverViewport == null && this.viewportRowSet == null
                        || (serverViewport != null && serverViewport.equals(this.viewportRowSet)))
                && serverReverseViewport == isReverseViewport;
    }

    private final WebBarrageStreamReader reader = new WebBarrageStreamReader();

    private void onFlightData(FlightData data) {
        WebBarrageMessage message;
        try {
            message = reader.parseFrom(options, null, state.chunkTypes(), state.columnTypes(), state.componentTypes(),
                    data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (message != null) {
            // This payload resulted in an update to the table's contents, inform the subscription
            barrageSubscription.applyUpdates(message);
        }
    }

    protected void onStreamEnd(ResponseStreamWrapper.Status status) {
        // TODO handle stream end/error
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
        if (doExchange != null) {
            doExchange.end();
            doExchange.cancel();
        }
        status = Status.DONE;
    }
}
