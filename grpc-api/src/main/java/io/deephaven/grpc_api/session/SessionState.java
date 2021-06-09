package io.deephaven.grpc_api.session;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.rpc.Code;
import com.google.rpc.Status;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.remotequery.QueryProcessingResults;
import io.deephaven.db.tables.utils.QueryPerformanceNugget;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.MemoryTableLoggers;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api.util.Scheduler;
import io.deephaven.hash.KeyedLongObjectHash;
import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tablelogger.QueryOperationPerformanceLogLogger;
import io.deephaven.db.tablelogger.QueryPerformanceLogLogger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.auth.AuthContext;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecute;
import static io.deephaven.grpc_api.util.GrpcUtil.safelyExecuteLocked;

/**
 * SessionState manages all exports for a single session.
 *
 * It manages exported {@link io.deephaven.db.util.liveness.LivenessReferent}.
 * It cascades failures to child dependencies.
 *
 * TODO:
 * - cyclical dependency detection
 * - out-of-order dependency timeout
 *
 * Details Regarding Data Structure of ExportObjects:
 *
 * The exportMap map, exportListeners list, exportListenerVersion, and export object's exportListenerVersion work
 * together to enable a listener to synchronize with outstanding exports in addition to sending the listener updates
 * while they continue to subscribe.
 *
 * - SessionState::exportMap's purpose is to map from the export id to the export object
 * - SessionState::exportListener's purpose is to keep a list of active subscribers
 * - SessionState::exportListenerVersion's purpose is to know whether or not a subscriber has already seen a status
 *
 * A listener will receive an export notification for export id NON_EXPORT_ID (a zero) to indicate that the refresh has
 * completed. A listener may see an update for an export before receiving the "refresh has completed" message. A listener
 * should be prepared to receive duplicate/redundant updates.
 */
public class SessionState extends LivenessArtifact {
    // Some work items will be dependent on other exports, but do not export anything themselves.
    public static final long NON_EXPORT_ID = 0;

    @AssistedFactory
    public interface Factory {
        SessionState create(AuthContext authContext);
    }

    private static final Logger log = LoggerFactory.getLogger(SessionState.class);

    private final String logPrefix;
    private final Scheduler scheduler;
    private final LiveTableMonitor liveTableMonitor;
    private final AuthContext authContext;

    private final String sessionId;
    private volatile SessionService.TokenExpiration expiration = null;
    private static final AtomicReferenceFieldUpdater<SessionState, SessionService.TokenExpiration> EXPIRATION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(SessionState.class, SessionService.TokenExpiration.class, "expiration");

    // some types of exports have a more sound story if the server tells the client what to call it
    private volatile long nextServerAllocatedId = -1;
    private static final AtomicLongFieldUpdater<SessionState> SERVER_EXPORT_UPDATER =
            AtomicLongFieldUpdater.newUpdater(SessionState.class, "nextServerAllocatedId");

    // maintains all requested exports by this client's session
    private final KeyedLongObjectHashMap<ExportObject<?>> exportMap = new KeyedLongObjectHashMap<>(EXPORT_OBJECT_ID_KEY);

    // the list of active listeners
    private final ConcurrentLinkedQueue<ExportListener> exportListeners = new ConcurrentLinkedQueue<>();
    private volatile int exportListenerVersion = 0;
    private static final AtomicIntegerFieldUpdater<SessionState> EXPORT_LISTENER_VERSION_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(SessionState.class, "exportListenerVersion");

    // accumulate the set of used export id's; it is ideal if the api client uses few continuous ranges
    private final Index usedExportIds = Index.CURRENT_FACTORY.getEmptyIndex();

    @AssistedInject
    public SessionState(final Scheduler scheduler, final LiveTableMonitor liveTableMonitor, @Assisted final AuthContext authContext) {
        this.sessionId = UuidCreator.toString(UuidCreator.getRandomBased());
        this.logPrefix = "SessionState{" + sessionId + "}: ";
        this.scheduler = scheduler;
        this.liveTableMonitor = liveTableMonitor;
        this.authContext = authContext;
        log.info().append(logPrefix).append("session initialized").endl();
    }

    /**
     * This method is controlled by SessionService to update the expiration whenever the session is refreshed.
     * @param expiration the initial expiration time and session token
     */
    @VisibleForTesting
    protected void initializeExpiration(@NotNull final SessionService.TokenExpiration expiration) {
        if (expiration.session != this) {
            throw new IllegalArgumentException("mismatched session for expiration token");
        }

        if (!EXPIRATION_UPDATER.compareAndSet(this, null, expiration)) {
            throw new IllegalStateException("session already initialized");
        }

        log.info().append(logPrefix)
                .append("token initialized to '").append(expiration.token.toString())
                .append("' which expires at ").append(expiration.deadline.toString())
                .append(".").endl();
    }

    /**
     * This method is controlled by SessionService to update the expiration whenever the session is refreshed.
     * @param expiration the new expiration time and session token
     */
    @VisibleForTesting
    protected void updateExpiration(@NotNull final SessionService.TokenExpiration expiration) {
        if (expiration.session != this) {
            throw new IllegalArgumentException("mismatched session for expiration token");
        }

        SessionService.TokenExpiration prevToken = this.expiration;
        while (prevToken != null) {
            if (EXPIRATION_UPDATER.compareAndSet(this, prevToken, expiration)) {
                break;
            }
            prevToken = this.expiration;
        }

        if (prevToken == null) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }

        log.info().append(logPrefix)
                .append("token rotating to '").append(expiration.token.toString())
                .append("' which expires at ").append(expiration.deadline.toString())
                .append(".").endl();
    }

    /**
     * @return the current expiration token for this session
     */
    public SessionService.TokenExpiration getExpiration() {
        if (isExpired()) {
            return null;
        }
        return expiration;
    }

    /**
     * @return whether or not this session is expired
     */
    public boolean isExpired() {
        return expiration == null || expiration.deadline.compareTo(scheduler.currentTime()) <= 0;
    }

    /**
     * @return the auth context for this session
     */
    public AuthContext getAuthContext() {
        return authContext;
    }

    /**
     * Grab the ExportObject for the provided ticket.
     * @param ticket the export ticket
     * @return a future-like object that represents this export
     */
    public <T> ExportObject<T> getExport(final Ticket ticket) {
        return getExport(ticketToExportId(ticket));
    }

    /**
     * Grab the ExportObject for the provided id.
     * @param exportId the export handle id
     * @return a future-like object that represents this export
     */
    @SuppressWarnings("unchecked")
    public <T> ExportObject<T> getExport(final long exportId) {
        if (isExpired()) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }

        final ExportObject<T> result;

        // If this a non-export or server side export, then it must already exist or else is a user error.
        if (exportId <= NON_EXPORT_ID) {
            result = (ExportObject<T>) exportMap.get(exportId);

            if (result == null) {
                throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Export id " + exportId + " does not exist and cannot be used out-of-order!");
            }
        } else {
            result = (ExportObject<T>) exportMap.putIfAbsent(exportId, EXPORT_OBJECT_VALUE_FACTORY);
        }

        return result;
    }

    /**
     * Create and export a pre-computed element. This is typically used in scenarios where the number of exports is not
     * known in advance by the requesting client.
     *
     * @param export the result of the export
     * @param <T> the export type
     * @return the ExportObject for this item for ease of access to the export
     */
    public <T> ExportObject<T> newServerSideExport(final T export) {
        if (isExpired()) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }

        final long exportId = SERVER_EXPORT_UPDATER.getAndDecrement(this);
        //noinspection unchecked
        final ExportObject<T> result = (ExportObject<T>) exportMap.putIfAbsent(exportId, EXPORT_OBJECT_VALUE_FACTORY);
        manage(result); // since we never `setWork` the session would otherwise not manage this EXPORTED export
        result.setResult(export);
        return result;
    }

    /**
     * Create an ExportBuilder to create the export after dependencies are satisfied.
     *
     * @param ticket the grpc {@link Ticket} for this export
     * @param <T> the export type that the callable will return
     * @return an export builder
     */
    public <T> ExportBuilder<T> newExport(final Ticket ticket) {
        return newExport(ticketToExportId(ticket));
    }

    /**
     * Create an ExportBuilder to create the export after dependencies are satisfied.
     *
     * @param exportId the export id
     * @param <T> the export type that the callable will return
     * @return an export builder
     */
    public <T> ExportBuilder<T> newExport(final long exportId) {
        if (isExpired()) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }
        if (exportId <= 0) {
            throw new IllegalArgumentException("exportId's <= 0 are reserved for server allocation only");
        }
        return new ExportBuilder<>(exportId);
    }

    /**
     * Create an ExportBuilder to perform work after dependencies are satisfied that itself does not create any exports.
     * @return an export builder
     */
    public <T> ExportBuilder<T> nonExport() {
        if (isExpired()) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }
        return new ExportBuilder<>(NON_EXPORT_ID);
    }

    /**
     * Some streaming response observers are liveness artifacts that have a life cycle as determined by grpc. We also
     * manage them as part of the session state so that they are closed when the session is expired.
     * @param nonExportReferent the referent that no longer needs management
     */
    public void unmanageNonExport(final LivenessReferent nonExportReferent) {
        tryUnmanage(nonExportReferent);
    }

    /**
     * Notes that this session has expired and exports should be released.
     */
    public void onExpired() {
        // note that once we set expiration to null; we are not able to add any more objects to the exportMap
        SessionService.TokenExpiration prevToken = expiration;
        while (prevToken != null) {
            if (EXPIRATION_UPDATER.compareAndSet(this, prevToken, null)) {
                break;
            }
            prevToken = expiration;
        }
        if (prevToken == null) {
            // already expired
            return;
        }

        log.info().append(logPrefix).append("releasing outstanding exports").endl();
        synchronized (exportMap) {
            exportMap.forEach(ExportObject::cancel);
        }
        exportMap.clear();

        log.info().append(logPrefix).append("outstanding exports released").endl();
        // note that export listeners are never published beyond this class; thus unmanaging them is sufficient
        synchronized (exportListeners) {
            exportListeners.forEach(this::tryUnmanage);
        }
        exportListeners.clear();
    }

    /**
     * Destroy this session state.
     */
    @Override
    protected void destroy() {
        onExpired();
    }

    /**
     * @return true iff the provided export state is a failure state
     */
    public static boolean isExportStateFailure(final ExportNotification.State state) {
        return state == ExportNotification.State.FAILED || state == ExportNotification.State.CANCELLED || state == ExportNotification.State.DEPENDENCY_FAILED;
    }

    /**
     * @return true iff the provided export state is a terminal state
     */
    public static boolean isExportStateTerminal(final ExportNotification.State state) {
        return state == ExportNotification.State.RELEASED || isExportStateFailure(state);
    }

    /**
     * This class represents one unit of content exported in the session.
     *
     * Note: we reuse ExportObject for non-exporting tasks that have export dependencies.
     * @param <T> Is context sensitive depending on the export.
     */
    public final class ExportObject<T> extends LivenessArtifact {
        // ExportId may be 0, if this is a task that has exported dependencies, but does not export anything itself.
        // Non-exports do not publish state changes.
        private final long exportId;
        private final String logIdentity;

        // final result of export
        private volatile T result;
        private volatile ExportNotification.State state = ExportNotification.State.UNKNOWN;
        private volatile int exportListenerVersion = 0;

        // This indicates whether or not this export should use the serial execution queue.
        private boolean requiresSerialQueue;

        // This is a reference of the work to-be-done. It is non-null only during the PENDING state.
        private Callable<T> exportMain;
        // This is a reference to the error handler to call if this item enters one of the failure states.
        private ExportErrorHandler errorHandler;

        // used to keep track of which children need notification on export completion
        private List<ExportObject<?>> children = Collections.emptyList();
        // used to manage liveness of dependencies (to prevent a dependency from being released before it is used)
        private List<ExportObject<?>> parents = Collections.emptyList();

        // used to detect when this object is ready for export (is visible for atomic int field updater)
        protected volatile int dependentCount = -1;

        // used to identify and propagate error details
        private String errorId;
        private String dependentHandle;

        /**
         * @param exportId the export id for this export
         */
        private ExportObject(final long exportId) {
            this.exportId = exportId;
            this.logIdentity = exportId == NON_EXPORT_ID ? Integer.toHexString(System.identityHashCode(this)) : Long.toString(exportId);
            setState(ExportNotification.State.UNKNOWN);
        }

        /**
         * Sets the dependencies and tracks liveness dependencies.
         *
         * @param parents the dependencies that must be exported prior to invoking the exportMain callable
         */
        private synchronized void setDependencies(final List<ExportObject<?>> parents) {
            if (dependentCount != -1) {
                throw new IllegalStateException("dependencies can only be set once on an exportable object");
            }

            this.parents = parents;
            dependentCount = parents.size();
            parents.forEach(this::manage);
        }


        /**
         * Sets the dependencies and initializes the relevant data structures to include this export as a child for each.
         *
         * @param exportMain the exportMain callable to invoke when dependencies are satisfied
         * @param errorHandler the errorHandler to notify so that it may propagate errors to the requesting client
         */
        private synchronized void setWork(final Callable<T> exportMain, final ExportErrorHandler errorHandler, final boolean requiresSerialQueue) {
            if (this.exportMain != null) {
                throw new IllegalStateException("work can only be set once on an exportable object");
            }
            this.requiresSerialQueue = requiresSerialQueue;

            if (isExportStateTerminal(this.state)) {
                // nothing to do because dependency already failed; hooray??
                return;
            }

            SessionState.this.manage(this);

            this.exportMain = exportMain;
            this.errorHandler = errorHandler;

            setState(ExportNotification.State.PENDING);
            if (dependentCount <= 0 ) {
                dependentCount = 0;
                scheduleExport();
            } else {
                for (final ExportObject<?> parent : parents) {
                    // we allow parents to be null to simplify calling conventions around optional dependencies
                    if (parent == null || !parent.maybeAddDependency(this)) {
                        onResolveOne(parent);
                    }
                    // else parent will notify us on completion
                }
            }
        }

        /**
         * WARNING! This method call is only valid inside of the exportMain callable / runnable.
         *
         * @return the result of the computed export
         */
        public T get() {
            if (isExpired()) {
                throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
            }

            // Note: an export may be released while still being a dependency of queued work; so let's make sure we're still valid
            if (result == null) {
                throw new IllegalStateException("Dependent export '" + exportId + "' is " + state.name() + " and not exported");
            }

            return result;
        }

        /**
         * @return the current state of this export
         */
        public ExportNotification.State getState() {
            return state;
        }

        /**
         * @return the export id or NON_EXPORT_ID if it does not have one
         */
        public Ticket getExportId() {
            return exportIdToTicket(exportId);
        }

        /**
         * Add dependency if object export has not yet completed.
         * @param child the dependent task
         * @return true if the child was added as a dependency
         */
        private boolean maybeAddDependency(final ExportObject<?> child) {
            if (state == ExportNotification.State.EXPORTED || isExportStateTerminal(state)) {
                return false;
            }
            synchronized (this) {
                if (state == ExportNotification.State.EXPORTED || isExportStateTerminal(state)) {
                    return false;
                }

                if (children.isEmpty()) {
                    children = new ArrayList<>();
                }
                children.add(child);
                return true;
            }
        }

        /**
         * This helper notifies any export notification listeners, and propagates resolution to children that depend
         * on this export.
         *
         * @param state the new state for this export
         */
        private synchronized void setState(final ExportNotification.State state) {
            if (isExportStateTerminal(this.state)) {
                throw new IllegalStateException("cannot change state if export is already in terminal state");
            }
            this.state = state;

            // Send an export notification before possibly notifying children of our state change.
            if (exportId != NON_EXPORT_ID) {
                log.info().append(logPrefix).append("export '").append(logIdentity)
                        .append("' is ExportState.").append(state.name()).endl();

                final ExportNotification notification = makeExportNotification();
                exportListenerVersion = SessionState.this.exportListenerVersion;
                exportListeners.forEach(listener -> listener.notify(notification));
            } else {
                log.info().append(logPrefix).append("non-export '").append(logIdentity)
                        .append("' is ExportState.").append(state.name()).endl();
            }

            if (isExportStateFailure(state) && errorHandler != null) {
                safelyExecute(() -> errorHandler.onError(state, errorId, dependentHandle));
            }

            if (state == ExportNotification.State.EXPORTED || isExportStateTerminal(state)) {
                children.forEach(child -> child.onResolveOne(this));
                children = Collections.emptyList();
                parents.forEach(this::unmanage);
                parents = Collections.emptyList();
                exportMain = null;
                errorHandler = null;
            }

            if (isExportStateTerminal(state)) {
                SessionState.this.tryUnmanage(this);
            }
        }

        /**
         * Decrements parent counter and kicks off the export if that was the last dependency.
         * @param parent the parent that just resolved; it may have failed
         */
        private void onResolveOne(@Nullable final ExportObject<?> parent) {
            // am I already cancelled or failed?
            if (isExportStateTerminal(state)) {
                return;
            }

            // is this a cascading failure?
            if (parent != null && isExportStateTerminal(parent.state)) {
                synchronized (this) {
                    errorId = parent.errorId;
                    ExportNotification.State terminalState = ExportNotification.State.DEPENDENCY_FAILED;

                    if (errorId == null) {
                        final String errorDetails;
                        switch (parent.state) {
                            case RELEASED:
                                errorDetails = "dependency released by user.";
                                break;
                            case CANCELLED:
                                terminalState = ExportNotification.State.CANCELLED;
                                errorDetails = "dependency cancelled by user.";
                                break;
                            default:
                                // Note: the other error states should have non-null errorId
                                errorDetails = "dependency does not have its own error defined " +
                                        "and is in an unexpected state: " + parent.state;
                                break;
                        }

                        errorId = UuidCreator.toString(UuidCreator.getRandomBased());
                        dependentHandle = parent.logIdentity;
                        log.error().append("Internal Error '").append(errorId).append("' ").append(errorDetails).endl();
                    }

                    setState(terminalState);
                    return;
                }
            }

            final int newDepCount = DEPENDENT_COUNT_UPDATER.decrementAndGet(this);
            if (newDepCount > 0) {
                return; // either more dependencies to wait for or this export has already failed
            }
            Assert.eqZero(newDepCount, "newDepCount");

            scheduleExport();
        }

        /**
         * Schedules the export to be performed; assumes all dependencies have been resolved.
         */
        private void scheduleExport() {
            synchronized (this) {
                if (state != ExportNotification.State.PENDING) {
                    return;
                }
                setState(ExportNotification.State.QUEUED);
            }

            if (requiresSerialQueue) {
                scheduler.runSerially(this::doExport);
            } else {
                scheduler.runImmediately(this::doExport);
            }
        }

        /**
         * Performs the actual export on a scheduling thread.
         */
        private void doExport() {
            synchronized (this) {
                if (state != ExportNotification.State.QUEUED || isExpired()) {
                    return; // had a cancel race with client
                }
            }
            Exception exception = null;
            boolean shouldLog = false;
            int evaluationNumber = -1;
            QueryProcessingResults queryProcessingResults = null;
            try (final AutoCloseable ignored = LivenessScopeStack.open()) {
                queryProcessingResults = new QueryProcessingResults(
                        QueryPerformanceRecorder.getInstance());

                evaluationNumber = QueryPerformanceRecorder.getInstance().startQuery("session=" + sessionId + ",exportId=" + logIdentity);
                try {
                    setResult(exportMain.call());
                } finally {
                    shouldLog = QueryPerformanceRecorder.getInstance().endQuery();
                }
            } catch (final Exception err) {
                exception = err;
                synchronized (this) {
                    errorId = UuidCreator.toString(UuidCreator.getRandomBased());
                    log.error().append("Internal Error '").append(errorId).append("' ").append(err).endl();
                    setState(ExportNotification.State.FAILED);
                }
            } finally {
                if (exception != null && queryProcessingResults != null) {
                    queryProcessingResults.setException(exception.toString());
                }
                QueryPerformanceRecorder.resetInstance();
            }
            if ((shouldLog || exception != null) && queryProcessingResults != null) {
                final MemoryTableLoggers memLoggers = MemoryTableLoggers.getInstance();
                final QueryPerformanceLogLogger qplLogger = memLoggers.getQplLogger();
                final QueryOperationPerformanceLogLogger qoplLogger = memLoggers.getQoplLogger();
                try {
                    final QueryPerformanceNugget nugget = Require.neqNull(
                            queryProcessingResults.getRecorder().getQueryLevelPerformanceData(),
                            "queryProcessingResults.getRecorder().getQueryLevelPerformanceData()");

                    //noinspection SynchronizationOnLocalVariableOrMethodParameter
                    synchronized(qplLogger) {
                        qplLogger.log(evaluationNumber,
                                queryProcessingResults,
                                nugget);
                    }
                    final List<QueryPerformanceNugget> nuggets = queryProcessingResults.getRecorder().getOperationLevelPerformanceData();
                    //noinspection SynchronizationOnLocalVariableOrMethodParameter
                    synchronized(qoplLogger) {
                        int opNo = 0;
                        for (QueryPerformanceNugget n : nuggets) {
                            qoplLogger.log(opNo++, n);
                        }
                    }
                } catch (final Exception e) {
                    log.error().append("Failed to log query performance data: ").append(e).endl();
                }
            }
        }

        /**
         * Sets the final result for this export.
         *
         * @param result the export object
         */
        private void setResult(final T result) {
            if (this.result != null) {
                throw new IllegalStateException("cannot setResult twice!");
            }

            this.result = result;
            synchronized (this) {
                if (!isExportStateTerminal(state) && !isExpired()) {
                    if (this.result instanceof LivenessReferent) {
                        tryManage((LivenessReferent) result);
                    }
                    setState(ExportNotification.State.EXPORTED);
                }
            }
        }

        /**
         * Releases this export; it will wait for the work to complete before releasing.
         */
        public synchronized void release() {
            if (state == ExportNotification.State.EXPORTED) {
                setState(ExportNotification.State.RELEASED);
            } else if (!isExportStateTerminal(state)){
                nonExport().require(this).submit(this::release);
            }
        }

        /**
         * Releases this export; it will cancel the work and dependent exports proactively when possible.
         */
        public synchronized void cancel() {
            if (state == ExportNotification.State.EXPORTED) {
                setState(ExportNotification.State.RELEASED);
            } else if (!isExportStateTerminal(state)) {
                setState(ExportNotification.State.CANCELLED);
            }
        }

        @Override
        protected synchronized void destroy() {
            cancel();
            result = null;
        }

        /**
         * @return an export notification representing current state
         */
        private synchronized ExportNotification makeExportNotification() {
            final ExportNotification.Builder builder = ExportNotification.newBuilder()
                    .setTicket(exportIdToTicket(exportId))
                    .setExportStateValue(state.ordinal());

            if (errorId != null) {
                builder.setContext(errorId);
            }
            if (dependentHandle != null) {
                builder.setDependentHandle(dependentHandle);
            }

            return builder.build();
        }
    }

    public void addExportListener(final StreamObserver<ExportNotification> observer) {
        if (isExpired()) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }

        final ExportListener listener = new ExportListener(observer);
        manage(listener);
        exportListeners.add(listener);
        final int versionId = EXPORT_LISTENER_VERSION_UPDATER.incrementAndGet(this);

        // we must check one last time that the session has not already expired; will otherwise be cleaned up on expiration
        if (isExpired()) {
            removeExportListener(observer);
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }
        listener.initialize(versionId);
    }

    public void removeExportListener(final StreamObserver<ExportNotification> observer) {
        final Optional<ExportListener> listener = exportListeners.stream().filter(l -> l.listener == observer).findFirst();
        if (listener.isPresent()) {
            tryUnmanage(listener.get());
            exportListeners.remove(listener.get());
        }
    }

    @VisibleForTesting
    public long numExportListeners() {
        return exportListeners.size();
    }

    private class ExportListener extends LivenessArtifact {
        private volatile boolean isClosed = false;

        private final StreamObserver<ExportNotification> listener;

        private ExportListener(final StreamObserver<ExportNotification> listener) {
            this.listener = listener;
        }

        /**
         * Propagate the change to the listener.
         *
         * @param notification the notification to send
         */
        public void notify(final ExportNotification notification) {
            if (isClosed) {
                return;
            }

            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                synchronized (listener) {
                    listener.onNext(notification);
                }
            } catch (final RuntimeException | Error e) {
                log.error().append("Failed to notify listener: ").append(e).endl();
                SessionState.this.tryUnmanage(this);
            }
        }

        /**
         * Perform the refresh and send initial export state to the listener.
         */
        private void initialize(final int versionId) {
            final String id = Integer.toHexString(System.identityHashCode(this));
            log.info().append(logPrefix).append("refreshing listener ").append(id).endl();

            for (final ExportObject<?> export : exportMap) {
                if (!tryManage(export)) {
                    continue;
                }

                try {
                    if (export.exportListenerVersion >= versionId) {
                        continue;
                    }

                    // the export cannot change state while we are synchronized on it
                    //noinspection SynchronizationOnLocalVariableOrMethodParameter
                    synchronized (export) {
                        // check again because of race to the lock
                        if (export.exportListenerVersion >= versionId) {
                            continue;
                        }

                        // no need to notify on exports that can no longer be accessed
                        if (isExportStateTerminal(export.getState())) {
                            continue;
                        }

                        notify(export.makeExportNotification());
                    }
                } finally {
                    unmanage(export);
                }
            }

            synchronized (this) {
                // notify that the refresh has completed
                notify(ExportNotification.newBuilder()
                        .setTicket(exportIdToTicket(NON_EXPORT_ID))
                        .setExportState(ExportNotification.State.EXPORTED)
                        .setContext("refresh is complete")
                        .build());
                log.info().append(logPrefix).append("refresh complete for listener ").append(id).endl();
            }
        }

        @Override
        protected synchronized void destroy() {
            if (isClosed) {
                return;
            }

            isClosed = true;
            safelyExecuteLocked(listener, listener::onCompleted);
            exportListeners.remove(this);
        }
    }

    @FunctionalInterface
    public interface ExportErrorHandler {
        /**
         * Notify the handler that the final state of this export failed.
         *
         * @param resultState the final state of the export
         * @param errorContext an identifier to locate the details as to why the export failed
         * @param dependentExportId an identifier for the export id of the dependent that caused the failure if applicable
         */
        void onError(final ExportNotification.State resultState, @Nullable final String errorContext, @Nullable final String dependentExportId);
    }
    @FunctionalInterface
    public interface ExportErrorGrpcHandler {
        /**
         * This error handler receives a grpc friendly {@link StatusRuntimeException} that can be directly sent to
         * {@link io.grpc.stub.StreamObserver#onError}.
         *
         * @param notification the notification to forward to the grpc client
         */
        void onError(final StatusRuntimeException notification);
    }

    public class ExportBuilder<T> {
        private final long exportId;
        private final ExportObject<T> export;

        private boolean requiresSerialQueue;
        private ExportErrorHandler errorHandler;

        ExportBuilder(final long exportId) {
            this.exportId = exportId;

            if (exportId == NON_EXPORT_ID) {
                this.export = new ExportObject<>(NON_EXPORT_ID);
            } else {
                //noinspection unchecked
                this.export = (ExportObject<T>) exportMap.putIfAbsent(exportId, EXPORT_OBJECT_VALUE_FACTORY);
            }
        }

        /**
         * Some exports must happen serially w.r.t. other exports. For example, an export that acquires the exclusive
         * LTM lock. We enqueue these dependencies independently of the otherwise regularly concurrent exports.
         *
         * @return this builder
         */
        public ExportBuilder<T> requiresSerialQueue() {
            requiresSerialQueue = true;
            return this;
        }

        /**
         * Invoke this method to set the required dependencies for this export. A parent may be null to simplify usage
         * of optional export dependencies.
         *
         * @param dependencies the parent dependencies
         * @return this builder
         */
        public ExportBuilder<T> require(final ExportObject<?> ... dependencies) {
            export.setDependencies(Arrays.asList(dependencies));
            return this;
        }

        /**
         * Invoke this method to set the required dependencies for this export. A parent may be null to simplify usage
         * of optional export dependencies.
         *
         * @param dependencies the parent dependencies
         * @return this builder
         */
        public <S> ExportBuilder<T> require(final List<ExportObject<S>> dependencies) {
            export.setDependencies(Collections.unmodifiableList(dependencies));
            return this;
        }

        /**
         * Invoke this method to set the error handler to be notified if this export fails. Only one error handler may be set.
         *
         * @param errorHandler the error handler to be notified
         * @return this builder
         */
        public ExportBuilder<T> onError(final ExportErrorHandler errorHandler) {
            if (this.errorHandler != null) {
                throw new IllegalStateException("error handler already set");
            }
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Invoke this method to set the error handler to be notified if this export fails. Only one error handler may be set.
         * This is a convenience method for use with {@link io.grpc.stub.StreamObserver}.
         *
         * @param errorHandler the error handler to be notified
         * @return this builder
         */
        public ExportBuilder<T> onError(final ExportErrorGrpcHandler errorHandler) {
            return onError(((resultState, errorContext, dependentExportId) -> {
                final String dependentStr = dependentExportId == null ? ""
                        : (" (related parent export id: " + dependentExportId + ")");
                errorHandler.onError(StatusProto.toStatusRuntimeException(Status.newBuilder()
                        .setCode(Code.INTERNAL.getNumber())
                        .setMessage("Details Logged w/ID '" + errorContext + "'" + dependentStr)
                        .build()));
            }));
        }

        /**
         * This method is the final method for submitting an export to the session. The provided callable is enqueued on
         * the scheduler when all dependencies have been satisfied. Only the dependencies supplied to the builder are
         * guaranteed to be resolved when the exportMain is executing.
         *
         * Warning! It is the SessionState owner's responsibility to wait to release any dependency until after this
         * exportMain callable/runnable has complete.
         *
         * @param exportMain the callable that generates the export
         * @return the submitted export object
         */
        public ExportObject<T> submit(final Callable<T> exportMain) {
            export.setWork(exportMain, errorHandler, requiresSerialQueue);
            return export;
        }

        /**
         * This method is the final method for submitting an export to the session. The provided runnable is enqueued on
         * the scheduler when all dependencies have been satisfied. Only the dependencies supplied to the builder are
         * guaranteed to be resolved when the exportMain is executing.
         *
         * Warning! It is the SessionState owner's responsibility to wait to release any dependency until after this
         * exportMain callable/runnable has complete.
         *
         * @param exportMain the runnable to execute once dependencies have resolved
         * @return the submitted export object
         */
        public ExportObject<T> submit(final Runnable exportMain) {
            return submit(() -> { exportMain.run(); return null; });
        }

        /**
         * @return the export object that this builder is building
         */
        public ExportObject<T> getExport() {
            return export;
        }

        /**
         * @return the export id of this export or {@link SessionState#NON_EXPORT_ID} if is a non-export
         */
        public long getExportId() {
            return exportId;
        }
    }

    /**
     * Convenience method to convert from export id to {@link Ticket}.
     *
     * @param exportId the export id
     * @return a grpc Ticket wrapping the export id
     */
    public static Ticket exportIdToTicket(final long exportId) {
        return Ticket.newBuilder().setId(GrpcUtil.longToByteString(exportId)).build();
    }

    /**
     * Convenience method to convert from {@link Ticket} to export id.
     *
     * @param ticket the grpc Ticket
     * @return the export id that the Ticket wraps
     */
    public static long ticketToExportId(final Ticket ticket) {
        if (ticket == null || ticket.getId().size() != 8) {
            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "missing or incorrectly formatted ticket");
        }
        return GrpcUtil.byteStringToLong(ticket.getId());
    }

    // used to detect when the export object is ready for export
    @SuppressWarnings("unchecked")
    private static final AtomicIntegerFieldUpdater<ExportObject<?>> DEPENDENT_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater((Class<ExportObject<?>>)(Class<?>) ExportObject.class, "dependentCount");

    private static final KeyedLongObjectKey<ExportObject<?>> EXPORT_OBJECT_ID_KEY = new KeyedLongObjectKey.BasicStrict<ExportObject<?>>() {
        @Override
        public long getLongKey(final ExportObject<?> exportObject) {
            return exportObject.exportId;
        }
    };

    private final KeyedLongObjectHash.ValueFactory<ExportObject<?>> EXPORT_OBJECT_VALUE_FACTORY = new KeyedLongObjectHash.ValueFactory.Strict<ExportObject<?>>() {
        @Override
        public ExportObject<?> newValue(final long key) {
            // technically we're already synchronized on the exportMap; but IJ doesn't understand that
            synchronized (exportMap) {
                // there is a race since we last checked
                if (isExpired()) {
                    throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
                }

                return new ExportObject<>(key);
            }
        }
    };
}
