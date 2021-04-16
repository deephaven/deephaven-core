package io.deephaven.grpc_api.table;

import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import io.deephaven.io.logger.Logger;
import com.google.rpc.Code;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.InstrumentedShiftAwareListener;
import io.deephaven.db.v2.NotificationStepReceiver;
import io.deephaven.db.v2.ShiftAwareSwapListener;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.TerminalNotification;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.util.annotations.ReferentialIntegrity;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateBatchMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.Ticket;
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
 * complete we are sent a notification for exportId == 0 (which is otherwise an invalid export id). Until we see
 * that this refresh was complete we will also queue any ticking updates into the refresh batch.
 *
 * When the refresh is complete, we wait until the end of the current LTM cycle to flush
 */
public class ExportedTableUpdateListener extends LivenessArtifact implements StreamObserver<ExportNotification> {
    @AssistedFactory
    public interface Factory {
        ExportedTableUpdateListener create(SessionState session, StreamObserver<ExportedTableUpdateBatchMessage> responseObserver);
    }

    private static final Logger log = LoggerFactory.getLogger(ExportedTableUpdateListener.class);

    private final LiveTableMonitor liveTableMonitor;
    private final SessionState session;

    private final String logPrefix;
    private final StreamObserver<ExportedTableUpdateBatchMessage> responseObserver;
    private final TerminalNotification flushNotification = new FlushNotification();
    private final KeyedLongObjectHashMap<ListenerImpl> updateListenerMap = new KeyedLongObjectHashMap<>(EXPORT_KEY);

    private volatile boolean isDestroyed = false;
    private ExportedTableUpdateBatchMessage.Builder nextMessage;

    @AssistedInject
    public ExportedTableUpdateListener(
            final LiveTableMonitor liveTableMonitor,
            @Assisted final SessionState session,
            @Assisted final StreamObserver<ExportedTableUpdateBatchMessage> responseObserver) {
        this.liveTableMonitor = liveTableMonitor;
        this.session = session;
        this.logPrefix = "ExportedTableUpdateListener(" + Integer.toHexString(System.identityHashCode(this)) + ") ";
        this.responseObserver = responseObserver;

        // initial refresh of table sizes is flushed when our export refresh is finished
        this.nextMessage = ExportedTableUpdateBatchMessage.newBuilder();
        session.manage(this);
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
        final long exportId = SessionState.ticketToExportId(ticket);

        if (exportId == 0) {
            liveTableMonitor.addNotification(flushNotification);
            return;
        }

        try {
            final ExportNotification.State state = notification.getExportState();
            if (state == ExportNotification.State.EXPORTED) {
                final SessionState.ExportObject<?> export = session.getExport(exportId);
                if (export.tryIncrementReferenceCount()) {
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
                    tryUnmanage(listener);
                    listener.destroy();
                }
            }
        } catch (final StatusRuntimeException ignored) {
            // we ignore race conditions related to liveness of an export/session
        }
    }

    @Override
    public void onError(final Throwable t) {
        destroy();
    }

    @Override
    public void onCompleted() {
        destroy();
    }

    @Override
    public synchronized void destroy() {
        if (isDestroyed) {
            return;
        }
        isDestroyed = true;
        session.removeExportListener(this);
        safelyExecute(responseObserver::onCompleted);
        updateListenerMap.forEach(ListenerImpl::destroy);
        updateListenerMap.clear();
        log.info().append(logPrefix).append("has been destroyed").endl();
    }

    /**
     * Initialize the listener for a newly exported table. This method is synchronized to prevent a race from the
     * table ticking before we append the initial refresh msg.
     *
     * @param ticket of the table being exported
     * @param exportId the export id of the table being exported
     * @param table the table that was just exported
     */
    private synchronized void onNewTableExport(final Ticket ticket, final long exportId, final BaseTable table) {
        if (!table.isLive()) {
            sendUpdateMessage(ticket, table.size(), null);
            return;
        }

        final ShiftAwareSwapListener swapListener = new ShiftAwareSwapListener(table);
        swapListener.subscribeForUpdates();
        final ListenerImpl listener = new ListenerImpl(table, exportId, swapListener);
        manage(listener);

        if (updateListenerMap.put(exportId, listener) != null) {
            throw new IllegalStateException("Duplicate ExportId Found: " + exportId);
        }

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

        if (nextMessage == null) {
            nextMessage = ExportedTableUpdateBatchMessage.newBuilder();
            liveTableMonitor.addNotification(flushNotification);
        }

        final ExportedTableUpdateMessage.Builder update = ExportedTableUpdateMessage.newBuilder()
                .setExportId(ticket).setSize(size);

        if (error != null) {
            update.setUpdateFailureMessage(GrpcUtil.securelyWrapError(log, error).getMessage());
        }

        nextMessage.addUpdates(update);
    }

    /**
     * The table listener implementation that propagates updates to our internal queue.
     */
    private class ListenerImpl extends InstrumentedShiftAwareListener {
        final private BaseTable table;
        final private long exportId;
        private volatile boolean destroyed = false;

        @ReferentialIntegrity
        final ShiftAwareSwapListener swapListener;

        private ListenerImpl(final BaseTable table, final long exportId, final ShiftAwareSwapListener swapListener) {
            super("ExportedTableUpdateListener (" + exportId + ")");
            this.table = table;
            this.exportId = exportId;
            this.swapListener = swapListener;
            manage(swapListener);
        }

        @Override
        public void onUpdate(final Update upstream) {
            sendUpdateMessage(SessionState.exportIdToTicket(exportId), table.size(), null);
        }

        @Override
        public void onFailureInternal(final Throwable error, final UpdatePerformanceTracker.Entry sourceEntry) {
            sendUpdateMessage(SessionState.exportIdToTicket(exportId), table.size(), error);
        }

        @Override
        public synchronized void destroy() {
            if (destroyed) {
                return;
            }
            destroyed = true;
            table.removeUpdateListener(swapListener);
        }
    }

    /**
     * At the end of the LTM cycle we flush all of the update messages that were aggregated that LTM cycle.
     */
    private class FlushNotification extends TerminalNotification {
        @Override
        public void run() {
            if (isDestroyed) {
                return;
            }

            try {
                responseObserver.onNext(nextMessage.build());
            } catch (final Error | RuntimeException error) {
                log.error().append(logPrefix).append("failed to notify listener of state change: ").append(error).endl();
                session.unmanageNonExport(ExportedTableUpdateListener.this);
                destroy();
            }
            nextMessage = null;
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
