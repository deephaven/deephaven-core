/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.table;

import com.google.rpc.Code;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.InstrumentedShiftAwareListener;
import io.deephaven.db.v2.NotificationStepReceiver;
import io.deephaven.db.v2.ShiftAwareSwapListener;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecute;

/**
 * Manage the lifecycle of exports that are Tables.
 *
 * Initially we receive a refresh of exports from the session state. This allows us to timely notify the observer
 * of existing table sizes for both static tables and tables that won't tick frequently. When the refresh is
 * complete we are sent a notification for exportId == 0 (which is otherwise an invalid export id).
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
            throw GrpcUtil.statusRuntimeException(Code.CANCELLED, "client cancelled the stream");
        }

        final Ticket ticket = notification.getTicket();
        final int exportId = ExportTicketHelper.ticketToExportId(ticket);

        try {
            final ExportNotification.State state = notification.getExportState();
            if (state == ExportNotification.State.EXPORTED) {
                final SessionState.ExportObject<?> export = session.getExport(ticket);
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
        safelyExecute(responseObserver::onCompleted);
        updateListenerMap.forEach(ListenerImpl::dropReference);
        updateListenerMap.clear();
        log.info().append(logPrefix).append("is complete").endl();
    }

    /**
     * Initialize the listener for a newly exported table. This method is synchronized to prevent a race from the
     * table ticking before we append the initial refresh msg.
     *
     * @param ticket of the table being exported
     * @param exportId the export id of the table being exported
     * @param table the table that was just exported
     */
    private synchronized void onNewTableExport(final Ticket ticket, final int exportId, final BaseTable table) {
        if (!table.isLive()) {
            sendUpdateMessage(ticket, table.size(), null);
            return;
        }

        // we may receive duplicate creation messages
        if (updateListenerMap.contains(exportId)) {
            return;
        }

        final ShiftAwareSwapListener swapListener = new ShiftAwareSwapListener(table);
        swapListener.subscribeForUpdates();
        final ListenerImpl listener = new ListenerImpl(table, exportId, swapListener);
        listener.tryRetainReference();
        updateListenerMap.put(exportId, listener);

        final MutableLong initSize = new MutableLong();
        table.initializeWithSnapshot(logPrefix, swapListener, (usePrev, beforeClockValue) -> {
            swapListener.setListenerAndResult(listener, NOOP_NOTIFICATION_STEP_RECEIVER);
            final Index index = table.getIndex();
            initSize.setValue(usePrev ? index.sizePrev() : index.size());
            return true;
        });
        sendUpdateMessage(ticket, initSize.longValue(), null);
    }

    /**
     * Append an update message to the batch being built this cycle. If this is the first update on this LTM cycle
     * then this also adds the terminal notification to flush the outstanding updates.
     *
     * @param ticket ticket of the table that has updated
     * @param size   the current size of the table
     * @param error  any propagated error of the table
     */
    private synchronized void sendUpdateMessage(final Ticket ticket, final long size, final Throwable error) {
        if (isDestroyed) {
            return;
        }

        final ExportedTableUpdateMessage.Builder update = ExportedTableUpdateMessage.newBuilder()
                .setExportId(ticket).setSize(size);

        if (error != null) {
            update.setUpdateFailureMessage(GrpcUtil.securelyWrapError(log, error).getMessage());
        }

        try {
            responseObserver.onNext(update.build());
        } catch (final RuntimeException err) {
            log.error().append(logPrefix).append("failed to notify listener of state change: ").append(err).endl();
            session.removeExportListener(this);
        }
    }

    /**
     * The table listener implementation that propagates updates to our internal queue.
     */
    private class ListenerImpl extends InstrumentedShiftAwareListener {
        final private BaseTable table;
        final private int exportId;

        @ReferentialIntegrity
        final ShiftAwareSwapListener swapListener;

        private ListenerImpl(final BaseTable table, final int exportId, final ShiftAwareSwapListener swapListener) {
            super("ExportedTableUpdateListener (" + exportId + ")");
            this.table = table;
            this.exportId = exportId;
            this.swapListener = swapListener;
            manage(swapListener);
        }

        @Override
        public void onUpdate(final Update upstream) {
            sendUpdateMessage(ExportTicketHelper.exportIdToTicket(exportId), table.size(), null);
        }

        @Override
        public void onFailureInternal(final Throwable error, final UpdatePerformanceTracker.Entry sourceEntry) {
            sendUpdateMessage(ExportTicketHelper.exportIdToTicket(exportId), table.size(), error);
        }
    }

    private static final KeyedLongObjectKey<ListenerImpl> EXPORT_KEY = new KeyedLongObjectKey.BasicStrict<ListenerImpl>() {
        @Override
        public long getLongKey(@NotNull final ListenerImpl listener) {
            return listener.exportId;
        }
    };

    private static final NotificationStepReceiver NOOP_NOTIFICATION_STEP_RECEIVER = lastNotificationStep -> {};
}
