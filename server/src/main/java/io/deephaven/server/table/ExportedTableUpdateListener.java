//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table;

import com.google.rpc.Code;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.SnapshotUnsuccessfulException;
import io.deephaven.engine.exceptions.TableAlreadyFailedException;
import io.deephaven.engine.liveness.LivenessStateException;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.NotificationStepReceiver;
import io.deephaven.engine.table.impl.OperationSnapshotControl;
import io.deephaven.engine.table.impl.UncoalescedTable;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyComplete;

/**
 * Manage the lifecycle of exports that are Tables.
 * <p>
 * Initially we receive a run of exports from the session state. This allows us to timely notify the observer of
 * existing table sizes for both static tables and tables that won't tick frequently. When the run is complete we are
 * sent a notification for exportId == 0 (which is otherwise an invalid export id).
 */
public class ExportedTableUpdateListener implements StreamObserver<ExportNotification> {
    @AssistedFactory
    public interface Factory {
        ExportedTableUpdateListener create(
                SessionState session,
                StreamObserver<ExportedTableUpdateMessage> responseObserver);
    }

    private static final Logger log = LoggerFactory.getLogger(ExportedTableUpdateListener.class);

    private final SessionState session;

    private final String logPrefix;
    private final StreamObserver<ExportedTableUpdateMessage> responseObserver;
    private final SessionService.ErrorTransformer errorTransformer;
    /**
     * Guards {@link #updateListenerMap}, {@link #isDestroyed}, each {@link ListenerImpl#sizeSent}, and messages to the
     * {@link #responseObserver}. The session synchronizes on this listener while notifying it, and a new export's
     * snapshot may wait for the update graph lock during that notification, so the update graph's listeners send their
     * sizes under this separate lock, which is never held while waiting for the update graph.
     */
    private final Object lock = new Object();
    private final KeyedLongObjectHashMap<ListenerImpl> updateListenerMap = new KeyedLongObjectHashMap<>(EXPORT_KEY);

    private volatile boolean isDestroyed = false;

    @AssistedInject
    public ExportedTableUpdateListener(
            @Assisted final SessionState session,
            @Assisted final StreamObserver<ExportedTableUpdateMessage> responseObserver,
            final SessionService.ErrorTransformer errorTransformer) {
        this.session = session;
        this.logPrefix = "ExportedTableUpdateListener(" + Integer.toHexString(System.identityHashCode(this)) + ") ";
        this.responseObserver = responseObserver;
        this.errorTransformer = errorTransformer;
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
        if (exportId == SessionState.NON_EXPORT_ID) {
            // ignore non-export notifications; we cannot look these up in the session
            return;
        }

        try {
            final ExportNotification.State state = notification.getExportState();
            if (state == ExportNotification.State.EXPORTED) {
                final SessionState.ExportObject<?> export = session.getExport(exportId);
                if (export.tryRetainReference()) {
                    try {
                        final Object obj = export.get();
                        if (obj instanceof BaseTable) {
                            try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(
                                    ((NotificationQueue.Dependency) obj).getUpdateGraph()).open()) {
                                onNewTableExport(ticket, exportId, (BaseTable<?>) obj);
                            }
                        }
                    } finally {
                        export.dropReference();
                    }
                }
            } else if (SessionState.isExportStateTerminal(state)) {
                final ListenerImpl listener;
                synchronized (lock) {
                    listener = updateListenerMap.remove(exportId);
                }
                if (listener != null) {
                    listener.dropReference();
                }
            }
        } catch (final LivenessStateException ignored) {
            // we ignore race conditions related to liveness of an export/session
        } catch (final RuntimeException err) {
            // let's catch any other error, be verbose about it, but not kill the RPC
            log.error().append(logPrefix).append("unexpected failure when processing export notification: ")
                    .append(err).endl();
        }
    }

    @Override
    public void onError(final Throwable t) {
        onCompleted();
    }

    @Override
    public void onCompleted() {
        synchronized (lock) {
            if (isDestroyed) {
                return;
            }
            isDestroyed = true;
            safelyComplete(responseObserver);
            updateListenerMap.forEach(ListenerImpl::dropReference);
            updateListenerMap.clear();
        }
        log.info().append(logPrefix).append("is complete").endl();
    }

    /**
     * Initialize the listener for a newly exported table. The snapshot that subscribes the listener does not hold
     * {@link #lock}, so the listener may send a size before the initial size is sent; the initial size is then stale,
     * and is not sent.
     *
     * @param ticket of the table being exported
     * @param exportId the export id of the table being exported
     * @param table the table that was just exported
     */
    private void onNewTableExport(final Ticket ticket, final int exportId, final BaseTable<?> table) {
        if (table instanceof UncoalescedTable) {
            // uncoalesced tables have no size and don't get updates
            return;
        }
        if (!table.isRefreshing()) {
            sendUpdateMessage(ticket, table.size(), null);
            return;
        }
        if (table.isFailed()) {
            sendUpdateMessage(ticket, -1, Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                    "Exported Table Already Failed"));
            return;
        }

        final ListenerImpl listener;
        synchronized (lock) {
            // we may receive duplicate creation messages
            if (isDestroyed || updateListenerMap.contains(exportId)) {
                return;
            }
            listener = new ListenerImpl(table, exportId);
            listener.tryRetainReference();
            updateListenerMap.put(exportId, listener);
        }

        final OperationSnapshotControl snapshotControl = new OperationSnapshotControl(table);
        final MutableLong initSize = new MutableLong();
        Throwable initError = null;
        try {
            BaseTable.initializeWithSnapshot(logPrefix, snapshotControl, (usePrev, beforeClockValue) -> {
                snapshotControl.setListenerAndResult(listener, NOOP_NOTIFICATION_STEP_RECEIVER);
                final TrackingRowSet rowSet = table.getRowSet();
                initSize.set(usePrev ? rowSet.sizePrev() : rowSet.size());
                return true;
            });
        } catch (final SnapshotUnsuccessfulException err) {
            initSize.set(-1);
            if (err.getCause() instanceof TableAlreadyFailedException) {
                initError = Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "Exported Table Already Failed");
            } else {
                initError = errorTransformer.transform(err);
            }
        }

        synchronized (lock) {
            if (updateListenerMap.get(exportId) != listener) {
                // the export was released, or this listener completed, during the snapshot; the dropped listener's
                // destroy may have run before the snapshot subscribed it
                table.removeUpdateListener(listener);
                return;
            }
            if (!listener.sizeSent) {
                listener.sizeSent = true;
                sendUpdateMessage(ticket, initSize.get(), initError);
            }
        }
    }

    /**
     * Append an update message to the batch being built this cycle. If this is the first update on this UGP cycle then
     * this also adds the terminal notification to flush the outstanding updates.
     *
     * @param ticket ticket of the table that has updated
     * @param size the current size of the table
     * @param error any propagated error of the table
     */
    private void sendUpdateMessage(final Ticket ticket, final long size, final Throwable error) {
        synchronized (lock) {
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
                log.debug().append(logPrefix).append("failed to notify listener of state change: ").append(err)
                        .endl();
                // The session synchronizes on this listener to remove it, and may hold that monitor while waiting for
                // the update graph, so the session removes this listener when it next notifies it, or when the client
                // cancels
                onCompleted();
            }
        }
    }

    /**
     * The table listener implementation that propagates updates to our internal queue.
     */
    private class ListenerImpl extends InstrumentedTableUpdateListener {
        final private BaseTable<?> table;
        final private int exportId;

        /**
         * Whether a size has been sent for this export, guarded by {@link #lock}.
         */
        private boolean sizeSent;

        private ListenerImpl(final BaseTable<?> table, final int exportId) {
            super("ExportedTableUpdateListener (" + exportId + ")");
            this.table = table;
            this.exportId = exportId;
            manage(table);
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            sendSize(null);
        }

        @Override
        public void onFailureInternal(final Throwable error, final Entry sourceEntry) {
            sendSize(errorTransformer.transform(error));
        }

        private void sendSize(final Throwable error) {
            synchronized (lock) {
                sizeSent = true;
                sendUpdateMessage(ExportTicketHelper.wrapExportIdInTicket(exportId), table.size(), error);
            }
        }

        @OverridingMethodsMustInvokeSuper
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
