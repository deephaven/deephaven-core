package io.deephaven.web.client.api.subscription;

import elemental2.dom.CustomEvent;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.dom.Event;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf.Message;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf.MessageHeader;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.flatbuf.message_generated.org.apache.arrow.flatbuf.RecordBatch;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Code;
import io.deephaven.web.client.api.*;
import io.deephaven.web.client.api.barrage.BarrageUtils;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.TableSnapshot;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.base.Js;

import java.util.Arrays;
import java.util.BitSet;

import static io.deephaven.web.client.api.subscription.ViewportData.NO_ROW_FORMAT_COLUMN;

/**
 * Encapsulates event handling around table subscriptions by "cheating" and wrapping up a JsTable
 * instance to do the real dirty work. This allows a viewport to stay open on the old table if
 * desired, while this one remains open.
 * <p>
 * As this just wraps a JsTable (and thus a CTS), it holds its own flattened, pUT'd handle to get
 * deltas from the server. The setViewport method can be used to adjust this table instead of
 * creating a new one.
 * <p>
 * Existing methods on JsTable like setViewport and getViewportData are intended to proxy to this,
 * which then will talk to the underlying handle and accumulated data.
 * <p>
 * As long as we keep the existing methods/events on JsTable, close() is not required if no other
 * method is called, with the idea then that the caller did not actually use this type. This means
 * that for every exported method (which then will mark the instance of "actually being used, please
 * don't automatically close me"), there must be an internal version called by those existing
 * JsTable method, which will allow this instance to be cleaned up once the JsTable deems it no
 * longer in use.
 * <p>
 * Note that if the caller does close an instance, this shuts down the JsTable's use of this (while
 * the converse is not true), providing a way to stop the server from streaming updates to the
 * client.
 */
public class TableViewportSubscription extends HasEventHandling {
    /**
     * Describes the possible lifecycle of the viewport as far as anything external cares about it
     */
    public enum Status {
        /**
         * Waiting for some prerequisite before we can begin, usually waiting to make sure the
         * original table is ready to be subscribed to. Once the original table is ready, we will
         * enter the ACTIVE state, even if the first update hasn't yet arrived.
         */
        STARTING,
        /**
         * Successfully created, viewport is at least begun on the server, updates are subscribed
         * and if changes happen on the server, we will be notified.
         */
        ACTIVE,
        /**
         * Closed or otherwise dead, can not be used again.
         */
        DONE
    }

    private final double refresh;

    private final JsTable original;
    private final ClientTableState originalState;
    private final Promise<JsTable> copy;
    private JsTable realized;

    private boolean retained;// if the sub is set up to not close the underlying table once the
                             // original table is done with it
    private boolean originalActive = true;

    private Status status = Status.STARTING;

    public TableViewportSubscription(double firstRow, double lastRow, Column[] columns,
        Double updateIntervalMs, JsTable existingTable) {
        refresh = updateIntervalMs == null ? 1000.0 : updateIntervalMs;
        // first off, copy the table, and flatten/pUT it, then apply the new viewport to that
        this.original = existingTable;
        this.originalState = original.state();
        copy = existingTable.copy(false).then(table -> new Promise<>((resolve, reject) -> {

            // Wait until the state is running to copy it
            originalState.onRunning(newState -> {
                table.batch(batcher -> {
                    batcher.customColumns(newState.getCustomColumns());
                    batcher.filter(newState.getFilters());
                    batcher.sort(newState.getSorts());

                    batcher.setFlat(true);
                });
                // TODO handle updateInterval core#188
                table.setInternalViewport(firstRow, lastRow, columns);

                // Listen for events and refire them on ourselves, optionally on the original table
                table.addEventListener(JsTable.EVENT_UPDATED, this::refire);
                table.addEventListener(JsTable.EVENT_ROWADDED, this::refire);
                table.addEventListener(JsTable.EVENT_ROWREMOVED, this::refire);
                table.addEventListener(JsTable.EVENT_ROWUPDATED, this::refire);
                table.addEventListener(JsTable.EVENT_SIZECHANGED, this::refire);

                // Take over for the "parent" table
                // Cache original table size so we can tell if we need to notify about a change
                double originalSize = newState.getSize();
                realized = table;
                status = Status.ACTIVE;
                // At this point we're now responsible for notifying of size changes, since we will
                // shortly have a viewport,
                // a more precise way to track the table size (at least w.r.t. the range of the
                // viewport), so if there
                // is any difference in size between "realized" and "original", notify now to finish
                // the transition.
                if (realized.getSize() != originalSize) {
                    JsLog.debug(
                        "firing size changed to transition between table managing its own size changes and viewport sub taking over",
                        realized.getSize());
                    CustomEventInit init = CustomEventInit.create();
                    init.setDetail(realized.getSize());
                    refire(new CustomEvent(JsTable.EVENT_SIZECHANGED, init));
                }

                resolve.onInvoke(table);
            }, table::close);
        }));
    }

    /**
     * Reflects the state of the original table, before being flattened.
     */
    public ClientTableState state() {
        return originalState;
    }

    private void refire(Event e) {
        this.fireEvent(e.type, e);
        if (originalActive && state() == original.state()) {
            // When these fail to match, it probably means that the original's state was paused, but
            // we're still
            // holding on to it. Since we haven't been internalClose()d yet, that means we're still
            // waiting for
            // the new state to resolve or fail, so we can be restored, or stopped. In theory, we
            // should put this
            // assert back, and make the pause code also tell us to pause.
            // assert state() == original.state() : "Table owning this viewport subscription forgot
            // to release it";
            original.fireEvent(e.type, e);
        }
    }

    private void retainForExternalUse() {
        retained = true;
    }

    @JsMethod
    public void setViewport(double firstRow, double lastRow, @JsOptional Column[] columns,
        @JsOptional Double updateIntervalMs) {
        retainForExternalUse();
        setInternalViewport(firstRow, lastRow, columns, updateIntervalMs);
    }

    public void setInternalViewport(double firstRow, double lastRow, Column[] columns,
        Double updateIntervalMs) {
        if (updateIntervalMs != null && refresh != updateIntervalMs) {
            throw new IllegalArgumentException(
                "Can't change refreshIntervalMs on a later call to setViewport, it must be consistent or omitted");
        }
        copy.then(table -> {
            table.setInternalViewport(firstRow, lastRow, columns);
            return Promise.resolve(table);
        });
    }

    @JsMethod
    public void close() {
        if (status == Status.DONE) {
            JsLog.warn(
                "TableViewportSubscription.close called on subscription that's already done.");
        }
        retained = false;
        internalClose();
    }

    /**
     * Internal API method to indicate that the Table itself has no further use for this. The
     * subscription should stop forwarding events and optionally close the underlying
     * table/subscription.
     */
    public void internalClose() {
        // indicate that the base table shouldn't get events any more, even if it this is still
        // retained elsewhere
        originalActive = false;

        if (retained || status == Status.DONE) {
            // the JsTable has indicated it is no longer interested in this viewport, but other
            // calling
            // code has retained it, keep it open for now.
            return;
        }

        status = Status.DONE;

        // not retained externally, and the original is inactive, mark as "not realized"
        realized = null;

        copy.then(table -> {
            table.close();
            return Promise.resolve(table);
        });
    }

    @JsMethod
    public Promise<TableData> getViewportData() {
        retainForExternalUse();
        return getInternalViewportData();
    }

    public Promise<TableData> getInternalViewportData() {
        return copy.then(JsTable::getInternalViewportData);
    }

    public Status getStatus() {
        if (realized == null) {
            assert status != Status.ACTIVE
                : "when the realized table is null, status should only be DONE or STARTING, instead is "
                    + status;
        } else {
            if (realized.isAlive()) {
                assert status == Status.ACTIVE
                    : "realized table is alive, expected status ACTIVE, instead is " + status;
            } else {
                assert status == Status.DONE
                    : "realized table is closed, expected status DONE, instead is " + status;
            }
        }

        return status;
    }

    public double size() {
        assert getStatus() == Status.ACTIVE;
        return realized.getSize();
    }

    public double totalSize() {
        assert getStatus() == Status.ACTIVE;
        return realized.getTotalSize();
    }

    @JsMethod
    public Promise<TableData> snapshot(JsRangeSet rows, Column[] columns) {
        // TODO #1039 slice rows and drop columns
        return copy.then(table -> {
            final ClientTableState state = table.state();
            String[] columnTypes = Arrays.stream(state.getAllColumns())
                .map(Column::getType)
                .toArray(String[]::new);

            final BitSet columnBitset = table.lastVisibleState().makeBitset(columns);
            return Callbacks.<TableSnapshot, String>promise(this, c -> {
                ResponseStreamWrapper<FlightData> stream =
                    ResponseStreamWrapper.of(table.getConnection().flightServiceClient().doGet(
                        Js.uncheckedCast(state.getHandle().makeTicket()),
                        table.getConnection().metadata()));
                stream.onData(flightData -> {

                    Message message =
                        Message.getRootAsMessage(new ByteBuffer(flightData.getDataHeader_asU8()));
                    if (message.headerType() == MessageHeader.Schema) {
                        // ignore for now, we'll handle this later
                        return;
                    }
                    assert message.headerType() == MessageHeader.RecordBatch;
                    RecordBatch header = message.header(new RecordBatch());
                    TableSnapshot snapshot = BarrageUtils.createSnapshot(header, BarrageUtils
                        .typedArrayToLittleEndianByteBuffer(flightData.getDataBody_asU8()), null,
                        true, columnTypes);

                    c.onSuccess(snapshot);
                });
                stream.onStatus(status -> {
                    if (status.getCode() != Code.OK) {
                        c.onFailure(status.getDetails());
                    }
                });
            }).then(defer()).then(snapshot -> {
                SubscriptionTableData pretendSubscription =
                    new SubscriptionTableData(Js.uncheckedCast(columns),
                        state.getRowFormatColumn() == null ? NO_ROW_FORMAT_COLUMN
                            : state.getRowFormatColumn().getIndex(),
                        null);
                TableData data = pretendSubscription.handleSnapshot(snapshot);
                return Promise.resolve(data);
            }).then(defer());
        });
    }

    /**
     * Instead of a micro-task between chained promises, insert a regular task so that control is
     * returned to the browser long enough to prevent the UI hanging.
     */
    private <T> IThenable.ThenOnFulfilledCallbackFn<T, T> defer() {
        return val -> new Promise<>((resolve, reject) -> {
            DomGlobal.setTimeout(ignoreArgs -> resolve.onInvoke(val), 0);
        });
    }
}
