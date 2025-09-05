//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsName;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb.ConfigValue;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.FlattenRequest;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsRangeSet;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.TableData;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.event.Event;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.shared.data.Range;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
import io.deephaven.web.shared.fu.RemoverFn;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PrimitiveIterator;

import static io.deephaven.web.client.api.JsTable.EVENT_ROWADDED;
import static io.deephaven.web.client.api.JsTable.EVENT_ROWREMOVED;
import static io.deephaven.web.client.api.JsTable.EVENT_ROWUPDATED;

/**
 * This object serves as a "handle" to a subscription, allowing it to be acted on directly or canceled outright. If you
 * retain an instance of this, you have two choices - either only use it to call `close()` on it to stop the table's
 * viewport without creating a new one, or listen directly to this object instead of the table for data events, and
 * always call `close()` when finished. Calling any method on this object other than close() will result in it
 * continuing to live on after `setViewport` is called on the original table, or after the table is modified.
 */
@TsName(namespace = "dh")
public class TableViewportSubscription extends AbstractTableSubscription {

    private RangeSet rows;
    private Column[] columns;
    private double refresh;

    private final JsTable original;
    private final RemoverFn reconnectSubscription;

    /** The initial state of the provided table, before flattening. */
    private final ClientTableState initialState;

    /**
     * true if the sub is set up to not close the underlying table once the original table is done with it, otherwise
     * false.
     */
    private boolean originalActive = true;
    /**
     * true if the developer has called methods directly on the subscription, otherwise false.
     */
    private boolean retained;

    private UpdateEventData viewportData;

    public static TableViewportSubscription make(RangeSet rows, Column[] columns,
            Double updateIntervalMs, JsTable existingTable) {
        ClientTableState tableState = existingTable.state();
        WorkerConnection connection = existingTable.getConnection();

        final ClientTableState stateToSubscribe;
        ConfigValue flattenViewport = connection.getServerConfigValue("web.flattenViewports");
        if (flattenViewport != null && flattenViewport.hasStringValue()
                && "true".equalsIgnoreCase(flattenViewport.getStringValue())) {
            stateToSubscribe = connection.newState((callback, newState, metadata) -> {
                FlattenRequest flatten = new FlattenRequest();
                flatten.setSourceId(tableState.getHandle().makeTableReference());
                flatten.setResultId(newState.getHandle().makeTicket());
                connection.tableServiceClient().flatten(flatten, metadata, callback::apply);
            }, "flatten");
            stateToSubscribe.refetch(null, connection.metadata()).then(result -> null, err -> null);
        } else {
            stateToSubscribe = tableState;
        }

        TableViewportSubscription sub = new TableViewportSubscription(stateToSubscribe, connection, existingTable);
        sub.setInternalViewport(rows, columns, updateIntervalMs, false);
        return sub;
    }

    public TableViewportSubscription(ClientTableState state, WorkerConnection connection, JsTable existingTable) {
        super(SubscriptionType.VIEWPORT_SUBSCRIPTION, state, connection);
        this.original = existingTable;

        initialState = existingTable.state();
        this.reconnectSubscription = existingTable.addEventListener(JsTable.EVENT_RECONNECT, e -> {
            if (existingTable.state() == initialState) {
                revive();
            }
        });
    }

    // Expose this as public
    @Override
    public void revive() {
        super.revive();
    }

    @Override
    protected void sendFirstSubscriptionRequest() {
        setInternalViewport(rows, columns, refresh, null);
    }

    @Override
    protected void notifyUpdate(RangeSet rowsAdded, RangeSet rowsRemoved, RangeSet totalMods, ShiftedRange[] shifted) {
        // viewport subscriptions are sometimes required to notify of size change events
        if (rowsAdded.size() != rowsRemoved.size() && originalActive) {
            fireEvent(JsTable.EVENT_SIZECHANGED, size());
        }
        UpdateEventData detail = new ViewportEventData(barrageSubscription, rowStyleColumn, getColumns(), rowsAdded,
                rowsRemoved, totalMods, shifted);

        detail.setOffset(this.viewportRowSet.getFirstRow());
        this.viewportData = detail;
        refire(new Event<>(EVENT_UPDATED, detail));

        if (hasListeners(EVENT_ROWADDED) || hasListeners(EVENT_ROWREMOVED) || hasListeners(EVENT_ROWUPDATED)) {
            RangeSet modifiedCopy = totalMods.copy();
            // exclude added items from being marked as modified, since we're hiding shifts from api consumers
            modifiedCopy.removeRangeSet(rowsAdded);
            RangeSet removedCopy = rowsRemoved.copy();
            RangeSet addedCopy = rowsAdded.copy();

            // Any position which was both added and removed should instead be marked as modified, this cleans
            // up anything excluded above that didn't otherwise make sense
            for (PrimitiveIterator.OfLong it = removedCopy.indexIterator(); it.hasNext();) {
                long index = it.nextLong();
                if (addedCopy.contains(index)) {
                    addedCopy.removeRange(new Range(index, index));
                    it.remove();
                    modifiedCopy.addRange(new Range(index, index));
                }
            }

            fireLegacyEventOnRowsetEntries(EVENT_ROWADDED, detail, rowsAdded);
            fireLegacyEventOnRowsetEntries(EVENT_ROWUPDATED, detail, totalMods);
            fireLegacyEventOnRowsetEntries(EVENT_ROWREMOVED, detail, rowsRemoved);
        }
    }

    private void fireLegacyEventOnRowsetEntries(String eventName, UpdateEventData updateEventData, RangeSet rowset) {
        if (hasListeners(eventName)) {
            rowset.indexIterator().forEachRemaining((long row) -> {
                JsPropertyMap<?> detail = wrap((SubscriptionRow) updateEventData.getRows().getAt((int) row), (int) row);
                fireEvent(eventName, detail);
            });
        }
    }

    private static JsPropertyMap<?> wrap(SubscriptionRow rowObj, int row) {
        return JsPropertyMap.of("row", rowObj, "index", (double) row);
    }

    @Override
    public void fireEvent(String type) {
        refire(new Event<>(type, null));
    }

    @Override
    public <T> void fireEvent(String type, T detail) {
        refire(new Event<T>(type, detail));
    }

    @Override
    public <T> void fireEvent(Event<T> e) {
        refire(e);
    }

    @Override
    public boolean hasListeners(String name) {
        if (originalActive && initialState == original.state()) {
            if (original.hasListeners(name)) {
                return true;
            }
        }
        return super.hasListeners(name);
    }

    /**
     * Utility to fire an event on this object and also optionally on the parent if still active. All {@code fireEvent}
     * overloads dispatch to this.
     *
     * @param e the event to fire
     * @param <T> the type of the custom event data
     */
    private <T> void refire(Event<T> e) {
        // explicitly calling super.fireEvent to avoid calling ourselves recursively
        super.fireEvent(e);
        if (originalActive && initialState == original.state()) {
            // When these fail to match, it probably means that the original's state was paused, but we're still
            // holding on to it. Since we haven't been internalClose()d yet, that means we're still waiting for
            // the new state to resolve or fail, so we can be restored, or stopped. In theory, we should put this
            // assert back, and make the pause code also tell us to pause.
            // assert initialState == original.state() : "Table owning this viewport subscription forgot to release it";
            original.fireEvent(e);
        }
    }

    private void retainForExternalUse() {
        retained = true;
    }

    /**
     * Changes the rows and columns set on this viewport. This cannot be used to change the update interval.
     * 
     * @param firstRow
     * @param lastRow
     * @param columns
     * @param updateIntervalMs
     * @deprecated use {@link #update(DataOptions.ViewportSubscriptionOptions)} instead
     */
    @JsMethod
    public void setViewport(double firstRow, double lastRow, @JsOptional @JsNullable Column[] columns,
            @JsOptional @JsNullable Double updateIntervalMs,
            @JsOptional @JsNullable Boolean isReverseViewport) {
        retainForExternalUse();
        setInternalViewport(RangeSet.ofRange((long) firstRow, (long) lastRow), columns, updateIntervalMs, isReverseViewport);
    }

    @JsMethod
    public void update(DataOptions.ViewportSubscriptionOptions options) {
        retainForExternalUse();
        // TODO validate that preview and updateinterval are not set, or make a specific type without them
        setInternalViewport(options.rows.asRangeSet().getRange(), Js.uncheckedCast(options.columns), options.updateIntervalMs, false);
    }

    public void setInternalViewport(RangeSet rows, Column[] columns, Double updateIntervalMs,
            Boolean isReverseViewport) {
        // Until we've created the stream, we just cache the requested viewport
        if (status == Status.STARTING) {
            this.rows = rows;
            this.columns = columns;
            this.refresh = updateIntervalMs == null ? 1000.0 : updateIntervalMs;
            return;
        }
        if (columns == null) {
            // Null columns means the user wants all columns, only supported on viewports. This can't be done until the
            // CTS has resolved
            columns = state().getColumns();
        } else {
            // If columns were provided, sort a copy so that we have them in the expected order
            columns = Js.<JsArray<Column>>uncheckedCast(columns).slice().asArray(new Column[0]);
            Arrays.sort(columns, Comparator.comparing(Column::getIndex));
        }
        if (updateIntervalMs != null && refresh != updateIntervalMs) {
            throw new IllegalArgumentException(
                    "Can't change refreshIntervalMs on a later call to setViewport, it must be consistent or omitted");
        }
        if (isReverseViewport == null) {
            isReverseViewport = false;
        }
        try {
            this.sendBarrageSubscriptionRequest(rows, Js.uncheckedCast(columns), updateIntervalMs, isReverseViewport);
        } catch (Exception e) {
            fireEvent(JsTable.EVENT_REQUEST_FAILED, e.getMessage());
        }
    }

    /**
     * Stops this viewport from running, stopping all events on itself and on the table that created it.
     */
    @JsMethod
    public void close() {
        if (isClosed()) {
            JsLog.warn("TableViewportSubscription.close called on subscription that's already done.");
        }
        retained = false;

        // Instead of calling super.close(), we delegate to internalClose()
        internalClose();
    }

    /**
     * Internal API method to indicate that the Table itself has no further use for this. The subscription should stop
     * forwarding events and optionally close the underlying table/subscription.
     */
    public void internalClose() {
        // indicate that the base table shouldn't get events anymore, even if it is still retained elsewhere
        originalActive = false;

        reconnectSubscription.remove();

        if (retained || isClosed()) {
            // the JsTable has indicated it is no longer interested in this viewport, but other calling
            // code has retained it, keep it open for now.
            return;
        }

        super.close();
    }

    /**
     * Gets the data currently visible in this viewport
     * 
     * @return Promise of {@link TableData}.
     */
    @JsMethod
    public Promise<@TsTypeRef(ViewportData.class) UpdateEventData> getViewportData() {
        retainForExternalUse();
        return getInternalViewportData();
    }

    public Promise<@TsTypeRef(ViewportData.class) UpdateEventData> getInternalViewportData() {
        if (isSubscriptionReady()) {
            return Promise.resolve(viewportData);
        }
        final LazyPromise<UpdateEventData> promise = new LazyPromise<>();
        addEventListenerOneShot(EVENT_UPDATED, ignored -> promise.succeed(viewportData));
        return promise.asPromise();
    }

    @JsMethod
    @Deprecated
    public Promise<TableData> snapshot(JsRangeSet rows, Column[] columns) {
        retainForExternalUse();
        DataOptions.SnapshotOptions options = new DataOptions.SnapshotOptions();
        options.previewOptions = new DataOptions.PreviewOptions();
        options.previewOptions.convertToString = true;
        options.previewOptions.array = 0.0;
        options.rows = Js.uncheckedCast(rows);
        if (columns == null) {
            columns = state().getColumns();
        }
        options.columns = Js.uncheckedCast(columns);
        options.isReverseViewport = false;
        return original.createSnapshot(options);
    }
}
