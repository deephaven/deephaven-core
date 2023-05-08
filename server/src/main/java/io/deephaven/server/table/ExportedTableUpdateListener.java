/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.table;

import com.google.rpc.Code;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.NotificationStepReceiver;
import io.deephaven.engine.table.impl.SwapListener;
import io.deephaven.engine.table.impl.UncoalescedTable;
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.session.SessionState;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyComplete;

/**
 * Manage the lifecycle of exports that are Tables.
 *
 * Initially we receive a run of exports from the session state. This allows us to timely notify the observer of
 * existing table sizes for both static tables and tables that won't tick frequently. When the run is complete we are
 * sent a notification for exportId == 0 (which is otherwise an invalid export id).
 */
public class ExportedTableUpdateListener implements StreamObserver<ExportNotification> {

    private static final Logger log = LoggerFactory.getLogger(ExportedTableUpdateListener.class);

    private final SessionState session;

    private final String logPrefix;
    private final StreamObserver<ExportedTableUpdateMessage> responseObserver;
    private final KeyedLongObjectHashMap<ListenerImpl> updateListenerMap = new KeyedLongObjectHashMap<>(EXPORT_KEY);

    private volatile boolean isDestroyed = false;

    public ExportedTableUpdateListener(
            final SessionState session,
            final StreamObserver<ExportedTableUpdateMessage> responseObserver) {
        this.session = session;
        this.logPrefix = "ExportedTableUpdateListener(" + Integer.toHexString(System.identityHashCode(this)) + ") ";
        this.responseObserver = responseObserver;
    }

    /**
     * Process the lifecycle update of an export from the session.
     *
     * @param notification the export state change notification
     */
    public void onNext(final ExportNotification notification) {
        if (isDestroyed) {
            throw Exceptions.statusRuntimeException(Code.CANCELLED, "client cancelled the stream");
        }

        final Ticket ticket = notification.getTicket();
        final int exportId = ExportTicketHelper.ticketToExportId(ticket, "ticket");

        try {
            final ExportNotification.State state = notification.getExportState();
            if (state == ExportNotification.State.EXPORTED) {
                final SessionState.ExportObject<?> export = session.getExport(ticket, "ticket");
                if (export.tryRetainReference()) {
                    try {
                        final Object obj = export.get();
                        if (obj instanceof BaseTable) {
                            onNewTableExport(ticket, exportId, (BaseTable) obj);
                        }
                    } finally {
                        export.dropReference();
                    }
                }
            } else if (SessionState.isExportStateTerminal(state)) {
                final ListenerImpl listener = updateListenerMap.remove(exportId);
                if (listener != null) {
                    listener.dropReference();
                }
            }
        } catch (final StatusRuntimeException ignored) {
            // we ignore race conditions related to liveness of an export/session
        }
    }

    @Override
    public void onError(final Throwable t) {
        onCompleted();
    }

    @Override
    public synchronized void onCompleted() {
        if (isDestroyed) {
            return;
        }
        isDestroyed = true;
        safelyComplete(responseObserver);
        updateListenerMap.forEach(ListenerImpl::dropReference);
        updateListenerMap.clear();
        log.info().append(logPrefix).append("is complete").endl();
    }

    /**
     * Initialize the listener for a newly exported table. This method is synchronized to prevent a race from the table
     * ticking before we append the initial run msg.
     *
     * @param ticket of the table being exported
     * @param exportId the export id of the table being exported
     * @param table the table that was just exported
     */
    private synchronized void onNewTableExport(final Ticket ticket, final int exportId, final BaseTable table) {
        if (table instanceof UncoalescedTable) {
            // uncoalesced tables have no size and don't get updates
            return;
        }
        if (!table.isRefreshing()) {
            sendUpdateMessage(ticket, table.size(), null);
            return;
        }

        // we may receive duplicate creation messages
        if (updateListenerMap.contains(exportId)) {
            return;
        }

        final SwapListener swapListener = new SwapListener(table);
        swapListener.subscribeForUpdates();
        final ListenerImpl listener = new ListenerImpl(table, exportId);
        listener.tryRetainReference();
        updateListenerMap.put(exportId, listener);

        final MutableLong initSize = new MutableLong();
        BaseTable.initializeWithSnapshot(logPrefix, swapListener, (usePrev, beforeClockValue) -> {
            swapListener.setListenerAndResult(listener, NOOP_NOTIFICATION_STEP_RECEIVER);
            final TrackingRowSet rowSet = table.getRowSet();
            initSize.setValue(usePrev ? rowSet.sizePrev() : rowSet.size());
            return true;
        });
        sendUpdateMessage(ticket, initSize.longValue(), null);
    }

    /**
     * Append an update message to the batch being built this cycle. If this is the first update on this UGP cycle then
     * this also adds the terminal notification to flush the outstanding updates.
     *
     * @param ticket ticket of the table that has updated
     * @param size the current size of the table
     * @param error any propagated error of the table
     */
    private synchronized void sendUpdateMessage(final Ticket ticket, final long size, final Throwable error) {
        if (isDestroyed) {
            return;
        }

        final ExportedTableUpdateMessage.Builder update = ExportedTableUpdateMessage.newBuilder()
                .setExportId(ticket).setSize(size);

        if (error != null) {
            // TODO (core#801): revisit this error communication to properly match the API Error mode
            // Note if this does get turned into an INTERNAL_ERROR we should re-use the same UUID for all listeners.
            update.setUpdateFailureMessage(error.getMessage());
        }

        try {
            responseObserver.onNext(update.build());
        } catch (final RuntimeException err) {
            log.debug().append(logPrefix).append("failed to notify listener of state change: ").append(err).endl();
            session.removeExportListener(this);
        }
    }

    /**
     * The table listener implementation that propagates updates to our internal queue.
     */
    private class ListenerImpl extends InstrumentedTableUpdateListener {
        final private BaseTable table;
        final private int exportId;

        private ListenerImpl(final BaseTable table, final int exportId) {
            super("ExportedTableUpdateListener (" + exportId + ")");
            this.table = table;
            this.exportId = exportId;
            manage(table);
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            sendUpdateMessage(ExportTicketHelper.wrapExportIdInTicket(exportId), table.size(), null);
        }

        @Override
        public void onFailureInternal(final Throwable error, final Entry sourceEntry) {
            sendUpdateMessage(ExportTicketHelper.wrapExportIdInTicket(exportId), table.size(), error);
        }

        @Override
        public void destroy() {
            super.destroy();
            table.removeUpdateListener(this);
        }
    }

    private static final KeyedLongObjectKey<ListenerImpl> EXPORT_KEY =
            new KeyedLongObjectKey.BasicStrict<ListenerImpl>() {
                @Override
                public long getLongKey(@NotNull final ListenerImpl listener) {
                    return listener.exportId;
                }
            };

    private static final NotificationStepReceiver NOOP_NOTIFICATION_STEP_RECEIVER = lastNotificationStep -> {
    };
}
