//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.subscription;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.dom.CustomEvent;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.promise.IThenable;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.JsRangeSet;
import io.deephaven.web.client.api.JsTable;
import io.deephaven.web.client.api.TableData;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.base.Js;

/**
 * Encapsulates event handling around table subscriptions by "cheating" and wrapping up a JsTable instance to do the
 * real dirty work. This allows a viewport to stay open on the old table if desired, while this one remains open.
 * <p>
 * As this just wraps a JsTable (and thus a CTS), it holds its own flattened, pUT'd handle to get deltas from the
 * server. The setViewport method can be used to adjust this table instead of creating a new one.
 * <p>
 * Existing methods on JsTable like setViewport and getViewportData are intended to proxy to this, which then will talk
 * to the underlying handle and accumulated data.
 * <p>
 * As long as we keep the existing methods/events on JsTable, close() is not required if no other method is called, with
 * the idea then that the caller did not actually use this type. This means that for every exported method (which then
 * will mark the instance of "actually being used, please don't automatically close me"), there must be an internal
 * version called by those existing JsTable method, which will allow this instance to be cleaned up once the JsTable
 * deems it no longer in use.
 * <p>
 * Note that if the caller does close an instance, this shuts down the JsTable's use of this (while the converse is not
 * true), providing a way to stop the server from streaming updates to the client.
 * <p>
 * This object serves as a "handle" to a subscription, allowing it to be acted on directly or canceled outright. If you
 * retain an instance of this, you have two choices - either only use it to call `close()` on it to stop the table's
 * viewport without creating a new one, or listen directly to this object instead of the table for data events, and
 * always call `close()` when finished. Calling any method on this object other than close() will result in it
 * continuing to live on after `setViewport` is called on the original table, or after the table is modified.
 *
 *
 *
 */
@TsInterface
@TsName(namespace = "dh")
public class TableViewportSubscription extends AbstractTableSubscription {

    // TODO move to superclass and check on viewport change
    private RangeSet serverViewport;

    private double firstRow;
    private double lastRow;
    private Column[] columns;
    private double refresh;

    private final JsTable original;

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

    public TableViewportSubscription(double firstRow, double lastRow, Column[] columns, Double updateIntervalMs,
            JsTable existingTable) {
        super(existingTable.state(), existingTable.getConnection());
        this.firstRow = firstRow;
        this.lastRow = lastRow;
        this.columns = columns;

        refresh = updateIntervalMs == null ? 1000.0 : updateIntervalMs;
        this.original = existingTable;
    }

    @Override
    protected void sendFirstSubscriptionRequest() {
        setInternalViewport(firstRow, lastRow, columns, refresh, null);
    }

    @Override
    protected void notifyUpdate(RangeSet rowsAdded, RangeSet rowsRemoved, RangeSet totalMods, ShiftedRange[] shifted) {
        // viewport subscriptions are sometimes required to notify of size change events
        if (rowsAdded.size() != rowsRemoved.size() && originalActive) {
            fireEventWithDetail(JsTable.EVENT_SIZECHANGED, size());
        }

        // TODO fire legacy table row added/updated/modified events
        // for (Integer index : mergeResults.added) {
        // CustomEventInit<JsPropertyMap<?>> addedEvent = CustomEventInit.create();
        // addedEvent.setDetail(wrap(vpd.getRows().getAt(index), index));
        // fireEvent(EVENT_ROWADDED, addedEvent);
        // }
        // for (Integer index : mergeResults.modified) {
        // CustomEventInit<JsPropertyMap<?>> addedEvent = CustomEventInit.create();
        // addedEvent.setDetail(wrap(vpd.getRows().getAt(index), index));
        // fireEvent(EVENT_ROWUPDATED, addedEvent);
        // }
        // for (Integer index : mergeResults.removed) {
        // CustomEventInit<JsPropertyMap<?>> addedEvent = CustomEventInit.create();
        // addedEvent.setDetail(wrap(vpd.getRows().getAt(index), index));
        // fireEvent(EVENT_ROWREMOVED, addedEvent);
        // }

        // TODO Rewrite shifts as adds/removed/modifies? in the past we ignored them...
        UpdateEventData detail = new UpdateEventData(rowsAdded, rowsRemoved, totalMods, shifted);
        detail.offset = this.serverViewport.getFirstRow();
        this.viewportData = detail;
        CustomEventInit<UpdateEventData> event = CustomEventInit.create();
        event.setDetail(detail);
        refire(new CustomEvent<>(EVENT_UPDATED, event));
    }

    @Override
    public void fireEvent(String type) {
        refire(new CustomEvent<>(type));
    }

    @Override
    public <T> void fireEventWithDetail(String type, T detail) {
        CustomEventInit<T> init = CustomEventInit.create();
        init.setDetail(detail);
        refire(new CustomEvent<T>(type, init));
    }

    @Override
    public <T> void fireEvent(String type, CustomEventInit<T> init) {
        refire(new CustomEvent<T>(type, init));
    }

    @Override
    public <T> void fireEvent(String type, CustomEvent<T> e) {
        if (type.equals(e.type)) {
            throw new IllegalArgumentException(type + " != " + e.type);
        }
        refire(e);
    }

    /**
     * Utility to fire an event on this object and also optionally on the parent if still active. All {@code fireEvent}
     * overloads dispatch to this.
     *
     * @param e the event to fire
     * @param <T> the type of the custom event data
     */
    private <T> void refire(CustomEvent<T> e) {
        // explicitly calling super.fireEvent to avoid calling ourselves recursively
        super.fireEvent(e.type, e);
        if (originalActive && state() == original.state()) {
            // When these fail to match, it probably means that the original's state was paused, but we're still
            // holding on to it. Since we haven't been internalClose()d yet, that means we're still waiting for
            // the new state to resolve or fail, so we can be restored, or stopped. In theory, we should put this
            // assert back, and make the pause code also tell us to pause.
            // assert state() == original.state() : "Table owning this viewport subscription forgot to release it";
            original.fireEvent(e.type, e);
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
     */
    @JsMethod
    public void setViewport(double firstRow, double lastRow, @JsOptional @JsNullable Column[] columns,
            @JsOptional @JsNullable Double updateIntervalMs,
            @JsOptional @JsNullable Boolean isReverseViewport) {
        retainForExternalUse();
        setInternalViewport(firstRow, lastRow, columns, updateIntervalMs, isReverseViewport);
    }

    public void setInternalViewport(double firstRow, double lastRow, Column[] columns, Double updateIntervalMs,
            Boolean isReverseViewport) {
        if (status == Status.STARTING) {
            this.firstRow = firstRow;
            this.lastRow = lastRow;
            this.columns = columns;
            this.refresh = updateIntervalMs == null ? 1000.0 : updateIntervalMs;
            return;
        }
        if (updateIntervalMs != null && refresh != updateIntervalMs) {
            throw new IllegalArgumentException(
                    "Can't change refreshIntervalMs on a later call to setViewport, it must be consistent or omitted");
        }
        if (isReverseViewport == null) {
            isReverseViewport = false;
        }
        if (!state().getTableDef().getAttributes().isBlinkTable()) {
            // we only set blink table viewports once; and that's in the constructor
            serverViewport = RangeSet.ofRange((long) firstRow, (long) lastRow);
            this.sendBarrageSubscriptionRequest(
                    serverViewport, Js.uncheckedCast(columns), updateIntervalMs, isReverseViewport);
        }
    }

    /**
     * Stops this viewport from running, stopping all events on itself and on the table that created it.
     */
    @JsMethod
    public void close() {
        // if (status == Status.DONE) {
        // JsLog.warn("TableViewportSubscription.close called on subscription that's already done.");
        // }
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

        // if (retained || status == Status.DONE) {
        // // the JsTable has indicated it is no longer interested in this viewport, but other calling
        // // code has retained it, keep it open for now.
        // return;
        // }
        //
        // status = Status.DONE;

        super.close();
    }

    /**
     * Gets the data currently visible in this viewport
     * 
     * @return Promise of {@link TableData}.
     */
    @JsMethod
    public Promise<TableData> getViewportData() {
        retainForExternalUse();
        return getInternalViewportData();
    }

    public Promise<TableData> getInternalViewportData() {
        if (isSubscriptionReady()) {
            return Promise.resolve(viewportData);
        }
        final LazyPromise<TableData> promise = new LazyPromise<>();
        addEventListenerOneShot(EVENT_UPDATED, ignored -> promise.succeed(viewportData));
        return promise.asPromise();
    }

    public Status getStatus() {
        // if (realized == null) {
        // assert status != Status.ACTIVE
        // : "when the realized table is null, status should only be DONE or STARTING, instead is " + status;
        // } else {
        // if (realized.isAlive()) {
        // assert status == Status.ACTIVE
        // : "realized table is alive, expected status ACTIVE, instead is " + status;
        // } else {
        // assert status == Status.DONE : "realized table is closed, expected status DONE, instead is " + status;
        // }
        // }

        return status;
    }

    public double size() {
        // TODO this is wrong
        assert getStatus() != Status.DONE;
        return super.size();
    }

    @JsMethod
    public Promise<TableData> snapshot(JsRangeSet rows, Column[] columns) {
        retainForExternalUse();
        // TODO #1039 slice rows and drop columns


        return null;
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
}
