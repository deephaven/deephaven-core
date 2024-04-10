//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.google.flatbuffers.FlatBufferBuilder;
import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
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
import io.deephaven.web.client.api.barrage.WebBarrageMessage;
import io.deephaven.web.client.api.barrage.WebBarrageStreamReader;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.data.WebBarrageSubscription;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.fu.JsSettings;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.io.IOException;
import java.util.BitSet;

import static io.deephaven.web.client.api.subscription.ViewportData.NO_ROW_FORMAT_COLUMN;

public abstract class AbstractTableSubscription extends HasEventHandling {
    /**
     * Indicates that some new data is available on the client, either an initial snapshot or a delta update. The
     * <b>detail</b> field of the event will contain a TableSubscriptionEventData detailing what has changed, or
     * allowing access to the entire range of items currently in the subscribed columns.
     */
    public static final String EVENT_UPDATED = "updated";

    private final ClientTableState state;
    private final WorkerConnection connection;
    private final int rowStyleColumn;
    private JsArray<Column> columns;
    private BitSet columnBitSet;
    private BarrageSubscriptionOptions options;

    private final BiDiStream<FlightData, FlightData> doExchange;
    private final WebBarrageSubscription barrageSubscription;

    private boolean subscriptionReady;

    public AbstractTableSubscription(ClientTableState state, WorkerConnection connection) {
        state.retain(this);
        this.state = state;
        this.connection = connection;
        rowStyleColumn = state.getRowFormatColumn() == null ? NO_ROW_FORMAT_COLUMN
                : state.getRowFormatColumn().getIndex();

        doExchange =
                connection.<FlightData, FlightData>streamFactory().create(
                        headers -> connection.flightServiceClient().doExchange(headers),
                        (first, headers) -> connection.browserFlightServiceClient().openDoExchange(first, headers),
                        (next, headers, c) -> connection.browserFlightServiceClient().nextDoExchange(next, headers,
                                c::apply),
                        new FlightData());

        doExchange.onData(this::onFlightData);
        // TODO handle stream ending, error

        // TODO going to need "started change" so we don't let data escape when still updating
        barrageSubscription = WebBarrageSubscription.subscribe(state, this::onViewportChange, this::onDataChanged);
    }

    protected void sendBarrageSubscriptionRequest(RangeSet viewport, JsArray<Column> columns, Double updateIntervalMs,
            boolean isReverseViewport) {
        this.columns = columns;
        this.columnBitSet = state.makeBitset(Js.uncheckedCast(columns));
        // TODO validate that we can change updateinterval
        this.options = BarrageSubscriptionOptions.builder()
                .batchSize(WebBarrageSubscription.BATCH_SIZE)
                .maxMessageSize(WebBarrageSubscription.MAX_MESSAGE_SIZE)
                .columnConversionMode(ColumnConversionMode.Stringify)
                .minUpdateIntervalMs(updateIntervalMs == null ? 0 : (int) (double) updateIntervalMs)
                .columnsAsList(false)
                .build();
        FlatBufferBuilder request = WebBarrageSubscription.subscriptionRequest(
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

    protected ClientTableState state() {
        return state;
    }

    protected WorkerConnection connection() {
        return connection;
    }

    protected boolean isSubscriptionReady() {
        return subscriptionReady;
    }

    public double size() {
        return barrageSubscription.getCurrentRowSet().size();
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
                transformRowsetForConsumer(rowsAdded),
                transformRowsetForConsumer(rowsRemoved),
                transformRowsetForConsumer(totalMods),
                barrageSubscription.getServerViewport() != null ? null : shifted
        );
        CustomEventInit<UpdateEventData> event = CustomEventInit.create();
        event.setDetail(detail);
        fireEvent(TableSubscription.EVENT_UPDATED, event);
    }

    @TsInterface
    @TsName(namespace = "dh")
    public class SubscriptionRow implements TableData.Row {
        private final long index;
        public LongWrapper indexCached;

        public SubscriptionRow(long index) {
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
            return barrageSubscription.getData(index, column.getIndex());
        }

        @Override
        public Format getFormat(Column column) {
            long cellColors = 0;
            long rowColors = 0;
            String numberFormat = null;
            String formatString = null;
            if (column.getStyleColumnIndex() != null) {
                cellColors = barrageSubscription.getData(index, column.getStyleColumnIndex());
            }
            if (rowStyleColumn != TableData.NO_ROW_FORMAT_COLUMN) {
                rowColors = barrageSubscription.getData(index, rowStyleColumn);
            }
            if (column.getFormatStringColumnIndex() != null) {
                numberFormat = barrageSubscription.getData(index, column.getFormatStringColumnIndex());
            }
            if (column.getFormatStringColumnIndex() != null) {
                formatString = barrageSubscription.getData(index, column.getFormatStringColumnIndex());
            }
            return new Format(cellColors, rowColors, numberFormat, formatString);
        }
    }


    @TsInterface
    @TsName(name = "SubscriptionTableData", namespace = "dh")
    public class UpdateEventData implements TableData {
        private final JsRangeSet added;
        private final JsRangeSet removed;
        private final JsRangeSet modified;

        // cached copy in case it was requested, could be requested again
        private JsArray<SubscriptionRow> allRows;

        // TODO expose this property only if this is a viewport
        public double offset;

        public UpdateEventData(RangeSet added, RangeSet removed, RangeSet modified, ShiftedRange[] shifted) {
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

        /**
         * A lazily computed array of all rows in the entire table
         *
         * @return {@link SubscriptionRow} array.
         */
        @Override
        public JsArray<SubscriptionRow> getRows() {
            if (allRows == null) {
                allRows = new JsArray<>();
                RangeSet rowSet = barrageSubscription.getCurrentRowSet();
                RangeSet positions = transformRowsetForConsumer(rowSet);
                DomGlobal.console.log(rowSet, positions);
                positions.indexIterator().forEachRemaining((long index) -> {
                    allRows.push(new SubscriptionRow(index));
                });
                if (JsSettings.isDevMode()) {
                    assert allRows.length == positions.size();
                }
            }
            return allRows;
        }

        @Override
        public Row get(int index) {
            return this.get((long) index);
        }

        /**
         * Reads a row object from the table, from which any subscribed column can be read
         *
         * @param index
         * @return {@link SubscriptionRow}
         */
        @Override
        public SubscriptionRow get(long index) {
            return new SubscriptionRow(index);
        }

        @Override
        public Any getData(int index, Column column) {
            return getData((long) index, column);
        }

        /**
         * a specific cell from the table, from the specified row and column
         *
         * @param index
         * @param column
         * @return Any
         */
        @Override
        public Any getData(long index, Column column) {
            return barrageSubscription.getData(index, column.getIndex());
        }

        /**
         * the Format to use for a cell from the specified row and column
         *
         * @param index
         * @param column
         * @return {@link Format}
         */
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
                cellColors = barrageSubscription.getData(index, column.getStyleColumnIndex());
            }
            if (rowStyleColumn != NO_ROW_FORMAT_COLUMN) {
                rowColors = barrageSubscription.getData(index, rowStyleColumn);
            }
            if (column.getFormatStringColumnIndex() != null) {
                numberFormat = barrageSubscription.getData(index, column.getFormatStringColumnIndex());
            }
            if (column.getFormatStringColumnIndex() != null) {
                formatString = barrageSubscription.getData(index, column.getFormatStringColumnIndex());
            }
            return new Format(cellColors, rowColors, numberFormat, formatString);
        }

        @Override
        public JsArray<Column> getColumns() {
            return columns;
        }

        /**
         * The ordered set of row indexes added since the last update
         *
         * @return dh.RangeSet
         */
        @Override
        public JsRangeSet getAdded() {
            return added;
        }

        /**
         * The ordered set of row indexes removed since the last update
         *
         * @return dh.RangeSet
         */
        @Override
        public JsRangeSet getRemoved() {
            return removed;
        }

        /**
         * The ordered set of row indexes updated since the last update
         *
         * @return dh.RangeSet
         */
        @Override
        public JsRangeSet getModified() {
            return modified;
        }

        @Override
        public JsRangeSet getFullIndex() {
            return new JsRangeSet(barrageSubscription.getCurrentRowSet());
        }
    }

    /**
     * If a viewport is in use, transforms the given rowset to position space based on
     * that viewport.
     * @param rowSet the rowset to possibly transform
     * @return a transformed rowset
     */
    private RangeSet transformRowsetForConsumer(RangeSet rowSet) {
        if (barrageSubscription.getServerViewport() != null) {
            return rowSet.subsetForPositions(barrageSubscription.getServerViewport(), false);//TODO reverse
        }
        return rowSet;
    }

    protected void onViewportChange(RangeSet serverViewport, BitSet serverColumns, boolean serverReverseViewport) {
        // if (serverViewport != null || serverReverseViewport) {
        // throw new IllegalStateException("Not a viewport subscription");
        // }
        subscriptionReady = (serverColumns == null && columnBitSet == null)
                || (serverColumns == null && columnBitSet.cardinality() == state.getColumns().length)
                || (serverColumns != null && serverColumns.equals(this.columnBitSet));
    }

    private void onFlightData(FlightData data) {
        WebBarrageStreamReader reader = new WebBarrageStreamReader();
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
        doExchange.end();
        doExchange.cancel();
    }
}
