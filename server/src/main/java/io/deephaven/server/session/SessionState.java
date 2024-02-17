/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.rpc.Code;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.base.reference.WeakSimpleReference;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.perf.QueryState;
import io.deephaven.engine.table.impl.util.EngineMetrics;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.hash.KeyedIntObjectHash;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.hash.KeyedIntObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.util.Scheduler;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.auth.AuthContext;
import io.deephaven.util.datastructures.SimpleReferenceManager;
import io.deephaven.util.process.ProcessEnvironment;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import javax.inject.Provider;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Consumer;

import static io.deephaven.base.log.LogOutput.MILLIS_FROM_EPOCH_FORMATTER;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyComplete;
import static io.deephaven.extensions.barrage.util.GrpcUtil.safelyError;

/**
 * SessionState manages all exports for a single session.
 *
 * <p>
 * It manages exported {@link LivenessReferent}. It cascades failures to child dependencies.
 *
 * <p>
 * TODO: - cyclical dependency detection - out-of-order dependency timeout
 *
 * <p>
 * Details Regarding Data Structure of ExportObjects:
 *
 * <ul>
 * <li>The exportMap map, exportListeners list, exportListenerVersion, and export object's exportListenerVersion work
 * together to enable a listener to synchronize with outstanding exports in addition to sending the listener updates
 * while they continue to subscribe.</li>
 *
 * <li>SessionState::exportMap's purpose is to map from the export id to the export object</li>
 * <li>SessionState::exportListeners' purpose is to keep a list of active subscribers</li>
 * <li>SessionState::exportListenerVersion's purpose is to know whether or not a subscriber has already seen a
 * status</li>
 *
 * <li>A listener will receive an export notification for export id NON_EXPORT_ID (a zero) to indicate that the run has
 * completed. A listener may see an update for an export before receiving the "run has completed" message. A listener
 * should be prepared to receive duplicate/redundant updates.</li>
 * </ul>
 */
public class SessionState {
    // Some work items will be dependent on other exports, but do not export anything themselves.
    public static final int NON_EXPORT_ID = 0;

    @AssistedFactory
    public interface Factory {
        SessionState create(AuthContext authContext);
    }

    /**
     * Wrap an object in an ExportObject to make it conform to the session export API.
     *
     * @param export the object to wrap
     * @param <T> the type of the object
     * @return a sessionless export object
     */
    public static <T> ExportObject<T> wrapAsExport(final T export) {
        return new ExportObject<>(export);
    }

    /**
     * Wrap an exception in an ExportObject to make it conform to the session export API. The export behaves as if it
     * has already failed.
     *
     * @param caughtException the exception to propagate
     * @param <T> the type of the object
     * @return a sessionless export object
     */
    public static <T> ExportObject<T> wrapAsFailedExport(final Exception caughtException) {
        ExportObject<T> exportObject = new ExportObject<>(null);
        exportObject.caughtException = caughtException;
        return exportObject;
    }

    private static final Logger log = LoggerFactory.getLogger(SessionState.class);

    private final String logPrefix;
    private final Scheduler scheduler;
    private final SessionService.ErrorTransformer errorTransformer;
    private final AuthContext authContext;

    private final String sessionId;
    private volatile SessionService.TokenExpiration expiration = null;
    private static final AtomicReferenceFieldUpdater<SessionState, SessionService.TokenExpiration> EXPIRATION_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(SessionState.class, SessionService.TokenExpiration.class,
                    "expiration");

    // some types of exports have a more sound story if the server tells the client what to call it
    private volatile int nextServerAllocatedId = -1;
    private static final AtomicIntegerFieldUpdater<SessionState> SERVER_EXPORT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(SessionState.class, "nextServerAllocatedId");

    // maintains all requested exports by this client's session
    private final KeyedIntObjectHashMap<ExportObject<?>> exportMap = new KeyedIntObjectHashMap<>(EXPORT_OBJECT_ID_KEY);

    // the list of active listeners
    private final List<ExportListener> exportListeners = new CopyOnWriteArrayList<>();
    private volatile int exportListenerVersion = 0;

    // Usually, export life cycles are managed explicitly with the life cycle of the session state. However, we need
    // to be able to close non-exports that are not in the map but are otherwise satisfying outstanding gRPC requests.
    private final SimpleReferenceManager<Closeable, WeakSimpleReference<Closeable>> onCloseCallbacks =
            new SimpleReferenceManager<>(WeakSimpleReference::new, false);

    private final ExecutionContext executionContext;

    @AssistedInject
    public SessionState(
            final Scheduler scheduler,
            final SessionService.ErrorTransformer errorTransformer,
            final Provider<ExecutionContext> executionContextProvider,
            @Assisted final AuthContext authContext) {
        this.sessionId = UuidCreator.toString(UuidCreator.getRandomBased());
        this.logPrefix = "SessionState{" + sessionId + "}: ";
        this.scheduler = scheduler;
        this.errorTransformer = errorTransformer;
        this.authContext = authContext;
        this.executionContext = executionContextProvider.get().withAuthContext(authContext);
        log.debug().append(logPrefix).append("session initialized").endl();
    }

    /**
     * This method is controlled by SessionService to update the expiration whenever the session is refreshed.
     *
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

        log.debug().append(logPrefix)
                .append("token initialized to '").append(expiration.token.toString())
                .append("' which expires at ").append(MILLIS_FROM_EPOCH_FORMATTER, expiration.deadlineMillis)
                .append(".").endl();
    }

    /**
     * This method is controlled by SessionService to update the expiration whenever the session is refreshed.
     *
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
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }

        log.debug().append(logPrefix).append("token, expires at ")
                .append(MILLIS_FROM_EPOCH_FORMATTER, expiration.deadlineMillis).append(".").endl();
    }

    /**
     * @return the session id
     */
    public String getSessionId() {
        return sessionId;
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
        final SessionService.TokenExpiration currToken = expiration;
        return currToken == null || currToken.deadlineMillis <= scheduler.currentTimeMillis();
    }

    /**
     * @return the auth context for this session
     */
    public AuthContext getAuthContext() {
        return authContext;
    }

    /**
     * @return the execution context for this session
     */
    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    /**
     * Grab the ExportObject for the provided ticket.
     *
     * @param ticket the export ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a future-like object that represents this export
     */
    public <T> ExportObject<T> getExport(final Ticket ticket, final String logId) {
        return getExport(ExportTicketHelper.ticketToExportId(ticket, logId));
    }

    /**
     * Grab the ExportObject for the provided ticket.
     *
     * @param ticket the export ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a future-like object that represents this export
     */
    public <T> ExportObject<T> getExport(final Flight.Ticket ticket, final String logId) {
        return getExport(FlightExportTicketHelper.ticketToExportId(ticket, logId));
    }

    /**
     * Grab the ExportObject for the provided id.
     *
     * @param exportId the export handle id
     * @return a future-like object that represents this export
     */
    @SuppressWarnings("unchecked")
    public <T> ExportObject<T> getExport(final int exportId) {
        if (isExpired()) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }

        final ExportObject<T> result;

        if (exportId < NON_EXPORT_ID) {
            // If this a server-side export then it must already exist or else is a user error.
            result = (ExportObject<T>) exportMap.get(exportId);

            if (result == null) {
                throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                        "Export id " + exportId + " does not exist and cannot be used out-of-order!");
            }
        } else if (exportId > NON_EXPORT_ID) {
            // If this a client-side export we'll allow an out-of-order request by creating a new export object.
            result = (ExportObject<T>) exportMap.putIfAbsent(exportId, EXPORT_OBJECT_VALUE_FACTORY);
        } else {
            // If this is a non-export request, then it is a user error.
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Export id " + exportId + " refers to a non-export and cannot be requested!");
        }

        return result;
    }

    /**
     * Grab the ExportObject for the provided id if it already exists, otherwise return null.
     *
     * @param exportId the export handle id
     * @return a future-like object that represents this export
     */
    @SuppressWarnings("unchecked")
    public <T> ExportObject<T> getExportIfExists(final int exportId) {
        if (isExpired()) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }

        return (ExportObject<T>) exportMap.get(exportId);
    }

    /**
     * Grab the ExportObject for the provided id if it already exists, otherwise return null.
     *
     * @param ticket the export ticket
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @return a future-like object that represents this export
     */
    public <T> ExportObject<T> getExportIfExists(final Ticket ticket, final String logId) {
        return getExportIfExists(ExportTicketHelper.ticketToExportId(ticket, logId));
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
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }

        final int exportId = SERVER_EXPORT_UPDATER.getAndDecrement(this);

        // noinspection unchecked
        final ExportObject<T> result = (ExportObject<T>) exportMap.putIfAbsent(exportId, EXPORT_OBJECT_VALUE_FACTORY);
        result.setResult(export);
        return result;
    }

    /**
     * Create an ExportBuilder to create the export after dependencies are satisfied.
     *
     * @param ticket the grpc {@link Flight.Ticket} for this export
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param <T> the export type that the callable will return
     * @return an export builder
     */
    public <T> ExportBuilder<T> newExport(final Flight.Ticket ticket, final String logId) {
        return newExport(FlightExportTicketHelper.ticketToExportId(ticket, logId));
    }

    /**
     * Create an ExportBuilder to create the export after dependencies are satisfied.
     *
     * @param ticket the grpc {@link Ticket} for this export
     * @param logId an end-user friendly identification of the ticket should an error occur
     * @param <T> the export type that the callable will return
     * @return an export builder
     */
    public <T> ExportBuilder<T> newExport(final Ticket ticket, final String logId) {
        return newExport(ExportTicketHelper.ticketToExportId(ticket, logId));
    }

    /**
     * Create an ExportBuilder to create the export after dependencies are satisfied.
     *
     * @param exportId the export id
     * @param <T> the export type that the callable will return
     * @return an export builder
     */
    @VisibleForTesting
    public <T> ExportBuilder<T> newExport(final int exportId) {
        if (isExpired()) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }
        if (exportId <= 0) {
            throw new IllegalArgumentException("exportId's <= 0 are reserved for server allocation only");
        }
        return new ExportBuilder<>(exportId);
    }

    /**
     * Create an ExportBuilder to perform work after dependencies are satisfied that itself does not create any exports.
     *
     * @return an export builder
     */
    public <T> ExportBuilder<T> nonExport() {
        if (isExpired()) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
        }
        return new ExportBuilder<>(NON_EXPORT_ID);
    }

    /**
     * Attach an on-close callback bound to the life of the session. Note that {@link Closeable} does not require that
     * the close() method be idempotent, but when combined with {@link #removeOnCloseCallback(Closeable)}, close() will
     * only be called once from this class.
     * <p>
     * </p>
     * If called after the session has expired, this will throw, and the close() method on the provided instance will
     * not be called.
     *
     * @param onClose the callback to invoke at end-of-life
     */
    public void addOnCloseCallback(final Closeable onClose) {
        synchronized (onCloseCallbacks) {
            if (isExpired()) {
                // After the session has expired, nothing new can be added to the collection, so throw an exception (and
                // release the lock, allowing each item already in the collection to be released)
                throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
            }
            onCloseCallbacks.add(onClose);
        }
    }

    /**
     * Remove an on-close callback bound to the life of the session.
     * <p />
     * A common pattern to use this will be for an object to try to remove itself, and if it succeeds, to call its own
     * {@link Closeable#close()}. If it fails, it can expect to have close() be called automatically.
     *
     * @param onClose the callback to no longer invoke at end-of-life
     * @return true iff the callback was removed
     * @apiNote If this SessionState has already begun expiration processing, {@code onClose} will not be removed by
     *          this method. This means that if {@code onClose} was previously added and not removed, it either has
     *          already been invoked or will be invoked by the SessionState.
     */
    public boolean removeOnCloseCallback(final Closeable onClose) {
        if (isExpired()) {
            // After the session has expired, nothing can be removed from the collection.
            return false;
        }
        synchronized (onCloseCallbacks) {
            return onCloseCallbacks.remove(onClose) != null;
        }
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

        log.debug().append(logPrefix).append("releasing outstanding exports").endl();
        synchronized (exportMap) {
            exportMap.forEach(ExportObject::cancel);
            exportMap.clear();
        }

        log.debug().append(logPrefix).append("outstanding exports released").endl();
        synchronized (exportListeners) {
            exportListeners.forEach(ExportListener::onRemove);
            exportListeners.clear();
        }

        final List<Closeable> callbacksToClose;
        synchronized (onCloseCallbacks) {
            callbacksToClose = new ArrayList<>(onCloseCallbacks.size());
            onCloseCallbacks.forEach((ref, callback) -> callbacksToClose.add(callback));
            onCloseCallbacks.clear();
        }
        callbacksToClose.forEach(callback -> {
            try {
                callback.close();
            } catch (final IOException e) {
                log.error().append(logPrefix).append("error during onClose callback: ").append(e).endl();
            }
        });
    }

    /**
     * @return true iff the provided export state is a failure state
     */
    public static boolean isExportStateFailure(final ExportNotification.State state) {
        return state == ExportNotification.State.FAILED || state == ExportNotification.State.CANCELLED
                || state == ExportNotification.State.DEPENDENCY_FAILED
                || state == ExportNotification.State.DEPENDENCY_NEVER_FOUND
                || state == ExportNotification.State.DEPENDENCY_RELEASED
                || state == ExportNotification.State.DEPENDENCY_CANCELLED;
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
     * <p>
     * Note: we reuse ExportObject for non-exporting tasks that have export dependencies.
     *
     * @param <T> Is context-sensitive depending on the export.
     *
     * @apiNote ExportId may be 0, if this is a task that has exported dependencies, but does not export anything
     *          itself. Non-exports do not publish state changes.
     */
    public final static class ExportObject<T> extends LivenessArtifact {
        private final int exportId;
        private final String logIdentity;
        private final SessionService.ErrorTransformer errorTransformer;
        private final SessionState session;

        /** used to keep track of performance details either for aggregation or for the async ticket resolution */
        private QueryPerformanceRecorder queryPerformanceRecorder;

        /** final result of export */
        private volatile T result;
        private volatile ExportNotification.State state = ExportNotification.State.UNKNOWN;
        private volatile int exportListenerVersion = 0;

        /** Indicates whether this export has already been well defined. This prevents export object reuse. */
        private boolean hasHadWorkSet = false;

        /** This indicates whether or not this export should use the serial execution queue. */
        private boolean requiresSerialQueue;

        /** This is a reference of the work to-be-done. It is non-null only during the PENDING state. */
        private Callable<T> exportMain;
        /** This is a reference to the error handler to call if this item enters one of the failure states. */
        @Nullable
        private ExportErrorHandler errorHandler;
        /** This is a reference to the success handler to call if this item successfully exports. */
        @Nullable
        private Consumer<? super T> successHandler;

        /** used to keep track of which children need notification on export completion */
        private List<ExportObject<?>> children = Collections.emptyList();
        /** used to manage liveness of dependencies (to prevent a dependency from being released before it is used) */
        private List<ExportObject<?>> parents = Collections.emptyList();

        /** used to detect when this object is ready for export (is visible for atomic int field updater) */
        private volatile int dependentCount = -1;
        /** our first parent that was already released prior to having dependencies set if one exists */
        private ExportObject<?> alreadyDeadParent;

        @SuppressWarnings("unchecked")
        private static final AtomicIntegerFieldUpdater<ExportObject<?>> DEPENDENT_COUNT_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater((Class<ExportObject<?>>) (Class<?>) ExportObject.class,
                        "dependentCount");

        /** used to identify and propagate error details */
        private String errorId;
        private String failedDependencyLogIdentity;
        private Exception caughtException;

        /**
         * @param errorTransformer the error transformer to use
         * @param exportId the export id for this export
         */
        private ExportObject(
                final SessionService.ErrorTransformer errorTransformer,
                final SessionState session,
                final int exportId) {
            super(true);
            this.errorTransformer = errorTransformer;
            this.session = session;
            this.exportId = exportId;
            this.logIdentity =
                    isNonExport() ? Integer.toHexString(System.identityHashCode(this)) : Long.toString(exportId);
            setState(ExportNotification.State.UNKNOWN);

            // we retain a reference until a non-export becomes EXPORTED or a regular export becomes RELEASED
            retainReference();
        }

        /**
         * Create an ExportObject that is not tied to any session. These must be non-exports that have require no work
         * to be performed. These export objects can be used as dependencies.
         *
         * @param result the object to wrap in an export
         */
        private ExportObject(final T result) {
            super(true);
            this.errorTransformer = null;
            this.session = null;
            this.exportId = NON_EXPORT_ID;
            this.result = result;
            this.dependentCount = 0;
            this.hasHadWorkSet = true;
            this.logIdentity = Integer.toHexString(System.identityHashCode(this)) + "-sessionless";

            if (result == null) {
                maybeAssignErrorId();
                state = ExportNotification.State.FAILED;
            } else {
                state = ExportNotification.State.EXPORTED;
            }

            if (result instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(result)) {
                manage((LivenessReferent) result);
            }
        }

        private boolean isNonExport() {
            return exportId == NON_EXPORT_ID;
        }

        private synchronized void setQueryPerformanceRecorder(
                final QueryPerformanceRecorder queryPerformanceRecorder) {
            if (this.queryPerformanceRecorder != null) {
                throw new IllegalStateException(
                        "performance query recorder can only be set once on an exportable object");
            }
            this.queryPerformanceRecorder = queryPerformanceRecorder;
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
            for (final ExportObject<?> parent : parents) {
                if (parent != null && !tryManage(parent)) {
                    // we've failed; let's cleanup already managed parents
                    forceReferenceCountToZero();
                    alreadyDeadParent = parent;
                    break;
                }
            }

            if (log.isDebugEnabled()) {
                final Exception e = new RuntimeException();
                final LogEntry entry =
                        log.debug().append(e).nl().append(session.logPrefix).append("export '").append(logIdentity)
                                .append("' has ").append(dependentCount).append(" dependencies remaining: ");
                for (ExportObject<?> parent : parents) {
                    entry.nl().append('\t').append(parent.logIdentity).append(" is ").append(parent.getState().name());
                }
                entry.endl();
            }
        }

        /**
         * Sets the dependencies and initializes the relevant data structures to include this export as a child for
         * each.
         *
         * @param exportMain the exportMain callable to invoke when dependencies are satisfied
         * @param errorHandler the errorHandler to notify so that it may propagate errors to the requesting client
         */
        private synchronized void setWork(
                @NotNull final Callable<T> exportMain,
                @Nullable final ExportErrorHandler errorHandler,
                @Nullable final Consumer<? super T> successHandler,
                final boolean requiresSerialQueue) {
            if (hasHadWorkSet) {
                throw new IllegalStateException("export object can only be defined once");
            }
            hasHadWorkSet = true;
            if (queryPerformanceRecorder != null && queryPerformanceRecorder.getState() == QueryState.RUNNING) {
                // transfer ownership of the qpr to the export before it can be resumed by the scheduler
                queryPerformanceRecorder.suspendQuery();
            }
            this.requiresSerialQueue = requiresSerialQueue;

            // we defer this type of failure until setWork for consistency in error handling
            if (alreadyDeadParent != null) {
                onDependencyFailure(alreadyDeadParent);
                alreadyDeadParent = null;
            }

            if (isExportStateTerminal(state)) {
                // The following scenarios cause us to get into this state:
                // - this export object was released/cancelled
                // - the session expiration propagated to this export object
                // - a parent export was released/dead prior to `setDependencies`
                // Note that failed dependencies will be handled in the onResolveOne method below.

                // since this is the first we know of the errorHandler, it could not have been invoked yet
                if (errorHandler != null) {
                    maybeAssignErrorId();
                    errorHandler.onError(state, errorId, caughtException, failedDependencyLogIdentity);
                }
                return;
            }

            this.exportMain = exportMain;
            this.errorHandler = errorHandler;
            this.successHandler = successHandler;

            if (state != ExportNotification.State.PUBLISHING) {
                setState(ExportNotification.State.PENDING);
            } else if (dependentCount > 0) {
                throw new IllegalStateException("published exports cannot have dependencies");
            }
            if (dependentCount <= 0) {
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
         * WARNING! This method call is only safe to use in the following patterns:
         * <p>
         * 1) If an export (or non-export) {@link ExportBuilder#require}'d this export then the method is valid from
         * within the Callable/Runnable passed to {@link ExportBuilder#submit}.
         * <p>
         * 2) By first obtaining a reference to the {@link ExportObject}, and then observing its state as
         * {@link ExportNotification.State#EXPORTED}. The caller must abide by the Liveness API and dropReference.
         * <p>
         * Example:
         *
         * <pre>
         * {@code
         * <T> T getFromExport(ExportObject<T> export) {
         *     if (export.tryRetainReference()) {
         *         try {
         *             if (export.getState() == ExportNotification.State.EXPORTED) {
         *                 return export.get();
         *             }
         *         } finally {
         *             export.dropReference();
         *         }
         *     }
         *     return null;
         * }
         * }
         * </pre>
         *
         * @return the result of the computed export
         */
        public T get() {
            if (session != null && session.isExpired()) {
                throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
            }
            final T localResult = result;
            // Note: an export may be released while still being a dependency of queued work; so let's make sure we're
            // still valid
            if (localResult == null) {
                throw new IllegalStateException(
                        "Dependent export '" + exportId + "' is null and in state " + state.name());
            }
            return localResult;
        }

        /**
         * @return the current state of this export
         */
        public ExportNotification.State getState() {
            return state;
        }

        /**
         * @return the ticket for this export; note if this is a non-export the returned ticket will not resolve to
         *         anything and is considered an invalid ticket
         */
        public Ticket getExportId() {
            return ExportTicketHelper.wrapExportIdInTicket(exportId);
        }

        /**
         * Add dependency if object export has not yet completed.
         *
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
         * This helper notifies any export notification listeners, and propagates resolution to children that depend on
         * this export.
         *
         * @param state the new state for this export
         */
        private synchronized void setState(final ExportNotification.State state) {
            if ((this.state == ExportNotification.State.EXPORTED && isNonExport())
                    || isExportStateTerminal(this.state)) {
                throw new IllegalStateException("cannot change state if export is already in terminal state");
            }
            if (this.state != ExportNotification.State.UNKNOWN && this.state.getNumber() >= state.getNumber()) {
                throw new IllegalStateException("export object state changes must advance toward a terminal state");
            }
            this.state = state;

            // Send an export notification before possibly notifying children of our state change.
            if (exportId != NON_EXPORT_ID) {
                log.debug().append(session.logPrefix).append("export '").append(logIdentity)
                        .append("' is ExportState.").append(state.name()).endl();

                final ExportNotification notification = makeExportNotification();
                exportListenerVersion = session.exportListenerVersion;
                session.exportListeners.forEach(listener -> listener.notify(notification));
            } else {
                log.debug().append(session == null ? "Session " : session.logPrefix)
                        .append("non-export '").append(logIdentity).append("' is ExportState.")
                        .append(state.name()).endl();
            }

            if (isExportStateFailure(state) && errorHandler != null) {
                maybeAssignErrorId();
                try {
                    final Exception toReport;
                    if (caughtException != null && errorTransformer != null) {
                        toReport = errorTransformer.transform(caughtException);
                    } else {
                        toReport = caughtException;
                    }

                    errorHandler.onError(state, errorId, toReport, failedDependencyLogIdentity);
                } catch (final Throwable err) {
                    // this is a serious error; crash the jvm to ensure that we don't miss it
                    log.error().append("Unexpected error while reporting ExportObject failure: ").append(err).endl();
                    ProcessEnvironment.getGlobalFatalErrorReporter().reportAsync(
                            "Unexpected error while reporting ExportObject failure", err);
                }
            }

            final boolean isNowExported = state == ExportNotification.State.EXPORTED;
            if (isNowExported && successHandler != null) {
                try {
                    successHandler.accept(result);
                } catch (final Throwable err) {
                    // this is a serious error; crash the jvm to ensure that we don't miss it
                    log.error().append("Unexpected error while reporting ExportObject success: ").append(err).endl();
                    ProcessEnvironment.getGlobalFatalErrorReporter().reportAsync(
                            "Unexpected error while reporting ExportObject success", err);
                }
            }

            if (isNowExported || isExportStateTerminal(state)) {
                children.forEach(child -> child.onResolveOne(this));
                children = Collections.emptyList();
                parents.stream().filter(Objects::nonNull).forEach(this::tryUnmanage);
                parents = Collections.emptyList();
                exportMain = null;
                errorHandler = null;
                successHandler = null;
            }

            if ((isNowExported && isNonExport()) || isExportStateTerminal(state)) {
                dropReference();
            }
        }

        /**
         * Decrements parent counter and kicks off the export if that was the last dependency.
         *
         * @param parent the parent that just resolved; it may have failed
         */
        private void onResolveOne(@Nullable final ExportObject<?> parent) {
            // am I already cancelled or failed?
            if (isExportStateTerminal(state)) {
                return;
            }

            // Is this a cascading failure? Note that we manage the parents in `setDependencies` which
            // keeps the parent results live until this child been exported. This means that the parent is allowed to
            // be in a RELEASED state, but is not allowed to be in a failure state.
            if (parent != null && isExportStateFailure(parent.state)) {
                onDependencyFailure(parent);
                return;
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
                if (state != ExportNotification.State.PENDING && state != ExportNotification.State.PUBLISHING) {
                    return;
                }
                setState(ExportNotification.State.QUEUED);
            }

            if (requiresSerialQueue) {
                session.scheduler.runSerially(this::doExport);
            } else {
                session.scheduler.runImmediately(this::doExport);
            }
        }

        /**
         * Performs the actual export on a scheduling thread.
         */
        private void doExport() {
            final Callable<T> capturedExport;
            synchronized (this) {
                capturedExport = exportMain;
                // check for some sort of cancel race with client
                if (state != ExportNotification.State.QUEUED
                        || session.isExpired()
                        || capturedExport == null
                        || !tryRetainReference()) {
                    if (!isExportStateTerminal(state)) {
                        setState(ExportNotification.State.CANCELLED);
                    } else if (errorHandler != null) {
                        // noinspection ThrowableNotThrown
                        Assert.statementNeverExecuted("in terminal state but error handler is not null");
                    }
                    return;
                }
                dropReference();
                setState(ExportNotification.State.RUNNING);
            }

            T localResult = null;
            boolean shouldLog = false;
            final QueryPerformanceRecorder exportRecorder;
            try (final SafeCloseable ignored1 = session.executionContext.open();
                    final SafeCloseable ignored2 = LivenessScopeStack.open()) {

                final String queryId;
                if (isNonExport()) {
                    queryId = "nonExport=" + logIdentity;
                } else {
                    queryId = "exportId=" + logIdentity;
                }

                final boolean isResume = queryPerformanceRecorder != null
                        && queryPerformanceRecorder.getState() == QueryState.SUSPENDED;
                exportRecorder = Objects.requireNonNullElseGet(queryPerformanceRecorder,
                        () -> QueryPerformanceRecorder.newQuery("ExportObject#doWork(" + queryId + ")",
                                session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY));

                try (final SafeCloseable ignored3 = isResume
                        ? exportRecorder.resumeQuery()
                        : exportRecorder.startQuery()) {
                    try {
                        localResult = capturedExport.call();
                    } catch (final Exception err) {
                        caughtException = err;
                    }
                    shouldLog = exportRecorder.endQuery();
                } catch (final Exception err) {
                    // end query will throw if the export runner left the QPR in a bad state
                    if (caughtException == null) {
                        caughtException = err;
                    }
                }

                if (caughtException != null) {
                    synchronized (this) {
                        if (!isExportStateTerminal(state)) {
                            maybeAssignErrorId();
                            if (!(caughtException instanceof StatusRuntimeException)) {
                                log.error().append("Internal Error '").append(errorId).append("' ")
                                        .append(caughtException).endl();
                            }
                            setState(ExportNotification.State.FAILED);
                        }
                    }
                }
                if (shouldLog || caughtException != null) {
                    EngineMetrics.getInstance().logQueryProcessingResults(exportRecorder, caughtException);
                }
                if (caughtException == null) {
                    // must set result after ending the query so that onSuccess may resume / finalize a parent query
                    setResult(localResult);
                }
            }
        }

        private void maybeAssignErrorId() {
            if (errorId == null) {
                errorId = UuidCreator.toString(UuidCreator.getRandomBased());
            }
        }

        private synchronized void onDependencyFailure(final ExportObject<?> parent) {
            errorId = parent.errorId;
            if (parent.caughtException instanceof StatusRuntimeException) {
                caughtException = parent.caughtException;
            }
            ExportNotification.State terminalState = ExportNotification.State.DEPENDENCY_FAILED;

            if (errorId == null) {
                final String errorDetails;
                switch (parent.state) {
                    case RELEASED:
                        terminalState = ExportNotification.State.DEPENDENCY_RELEASED;
                        errorDetails = "dependency released by user.";
                        break;
                    case CANCELLED:
                        terminalState = ExportNotification.State.DEPENDENCY_CANCELLED;
                        errorDetails = "dependency cancelled by user.";
                        break;
                    default:
                        // Note: the other error states should have non-null errorId
                        errorDetails = "dependency does not have its own error defined " +
                                "and is in an unexpected state: " + parent.state;
                        break;
                }

                maybeAssignErrorId();
                failedDependencyLogIdentity = parent.logIdentity;
                if (!(caughtException instanceof StatusRuntimeException)) {
                    log.error().append("Internal Error '").append(errorId).append("' ").append(errorDetails)
                            .endl();
                }
            }

            setState(terminalState);
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

            // result is cleared on destroy; so don't set if it won't be called
            if (!tryRetainReference()) {
                return;
            }

            try {
                synchronized (this) {
                    // client may race a cancel with setResult
                    if (!isExportStateTerminal(state)) {
                        this.result = result;
                        if (result instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(result)) {
                            manage((LivenessReferent) result);
                        }
                        setState(ExportNotification.State.EXPORTED);
                    }
                }
            } finally {
                dropReference();
            }
        }

        /**
         * Releases this export; it will wait for the work to complete before releasing.
         */
        public synchronized void release() {
            if (session == null) {
                throw new UnsupportedOperationException("Session-less exports cannot be released");
            }
            if (state == ExportNotification.State.EXPORTED) {
                if (isNonExport()) {
                    return;
                }
                setState(ExportNotification.State.RELEASED);
            } else if (!isExportStateTerminal(state)) {
                session.nonExport().require(this).submit(this::release);
            }
        }

        /**
         * Releases this export; it will cancel the work and dependent exports proactively when possible.
         */
        public synchronized void cancel() {
            if (session == null) {
                throw new UnsupportedOperationException("Session-less exports cannot be cancelled");
            }
            if (state == ExportNotification.State.EXPORTED) {
                if (isNonExport()) {
                    return;
                }
                setState(ExportNotification.State.RELEASED);
            } else if (!isExportStateTerminal(state)) {
                setState(ExportNotification.State.CANCELLED);
            }
        }

        @Override
        protected synchronized void destroy() {
            super.destroy();
            result = null;
            // keep SREs since error propagation won't reference a real errorId on the server
            if (!(caughtException instanceof StatusRuntimeException)) {
                caughtException = null;
            }
        }

        /**
         * @return an export notification representing current state
         */
        private synchronized ExportNotification makeExportNotification() {
            final ExportNotification.Builder builder = ExportNotification.newBuilder()
                    .setTicket(ExportTicketHelper.wrapExportIdInTicket(exportId))
                    .setExportState(state);

            if (errorId != null) {
                builder.setContext(errorId);
            }
            if (failedDependencyLogIdentity != null) {
                builder.setDependentHandle(failedDependencyLogIdentity);
            }

            return builder.build();
        }
    }

    public void addExportListener(final StreamObserver<ExportNotification> observer) {
        final int versionId;
        final ExportListener listener;
        synchronized (exportListeners) {
            if (isExpired()) {
                throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
            }

            listener = new ExportListener(observer);
            exportListeners.add(listener);
            versionId = ++exportListenerVersion;
        }

        listener.initialize(versionId);
    }

    /**
     * Remove an on-close callback bound to the life of the session.
     *
     * @param observer the observer to no longer be subscribed
     * @return The item if it was removed, else null
     */
    public StreamObserver<ExportNotification> removeExportListener(final StreamObserver<ExportNotification> observer) {
        final MutableObject<ExportListener> wrappedListener = new MutableObject<>();
        final boolean found = exportListeners.removeIf(wrap -> {
            if (wrappedListener.getValue() != null) {
                return false;
            }

            final boolean matches = wrap.listener == observer;
            if (matches) {
                wrappedListener.setValue(wrap);
            }
            return matches;
        });

        if (found) {
            wrappedListener.getValue().onRemove();
        }

        return found ? observer : null;
    }

    @VisibleForTesting
    public long numExportListeners() {
        return exportListeners.size();
    }

    private class ExportListener {
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
            } catch (final RuntimeException e) {
                log.error().append("Failed to notify listener: ").append(e).endl();
                removeExportListener(listener);
            }
        }

        /**
         * Perform the run and send initial export state to the listener.
         */
        private void initialize(final int versionId) {
            final String id = Integer.toHexString(System.identityHashCode(this));
            log.debug().append(logPrefix).append("refreshing listener ").append(id).endl();

            for (final ExportObject<?> export : exportMap) {
                if (!export.tryRetainReference()) {
                    continue;
                }

                try {
                    if (export.exportListenerVersion >= versionId) {
                        continue;
                    }

                    // the export cannot change state while we are synchronized on it
                    // noinspection SynchronizationOnLocalVariableOrMethodParameter
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
                    export.dropReference();
                }
            }

            // notify that the run has completed
            notify(ExportNotification.newBuilder()
                    .setTicket(ExportTicketHelper.wrapExportIdInTicket(NON_EXPORT_ID))
                    .setExportState(ExportNotification.State.EXPORTED)
                    .setContext("run is complete")
                    .build());
            log.debug().append(logPrefix).append("run complete for listener ").append(id).endl();
        }

        protected void onRemove() {
            synchronized (this) {
                if (isClosed) {
                    return;
                }
                isClosed = true;
            }

            safelyComplete(listener);
        }
    }

    @FunctionalInterface
    public interface ExportErrorHandler {
        /**
         * Notify the handler that the final state of this export failed.
         *
         * @param resultState the final state of the export
         * @param errorContext an identifier to locate the details as to why the export failed
         * @param dependentExportId an identifier for the export id of the dependent that caused the failure if
         *        applicable
         */
        void onError(final ExportNotification.State resultState,
                final String errorContext,
                @Nullable final Exception cause,
                @Nullable final String dependentExportId);
    }
    @FunctionalInterface
    public interface ExportErrorGrpcHandler {
        /**
         * This error handler receives a grpc friendly {@link StatusRuntimeException} that can be directly sent to
         * {@link StreamObserver#onError}.
         *
         * @param notification the notification to forward to the grpc client
         */
        void onError(final StatusRuntimeException notification);
    }

    public class ExportBuilder<T> {
        private final int exportId;
        private final ExportObject<T> export;

        private boolean requiresSerialQueue;
        private ExportErrorHandler errorHandler;
        private Consumer<? super T> successHandler;

        ExportBuilder(final int exportId) {
            this.exportId = exportId;

            if (exportId == NON_EXPORT_ID) {
                this.export = new ExportObject<>(SessionState.this.errorTransformer, SessionState.this, NON_EXPORT_ID);
            } else {
                // noinspection unchecked
                this.export = (ExportObject<T>) exportMap.putIfAbsent(exportId, EXPORT_OBJECT_VALUE_FACTORY);
            }
        }

        /**
         * Set the performance recorder to resume when running this export.
         *
         * @param queryPerformanceRecorder the performance recorder
         * @return this builder
         */
        public ExportBuilder<T> queryPerformanceRecorder(
                @NotNull final QueryPerformanceRecorder queryPerformanceRecorder) {
            export.setQueryPerformanceRecorder(queryPerformanceRecorder);
            return this;
        }

        /**
         * Some exports must happen serially w.r.t. other exports. For example, an export that acquires the exclusive
         * UGP lock. We enqueue these dependencies independently of the otherwise regularly concurrent exports.
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
        public ExportBuilder<T> require(final ExportObject<?>... dependencies) {
            export.setDependencies(List.of(dependencies));
            return this;
        }

        /**
         * Invoke this method to set the required dependencies for this export. A parent may be null to simplify usage
         * of optional export dependencies.
         *
         * @param dependencies the parent dependencies
         * @return this builder
         */
        public ExportBuilder<T> require(final List<? extends ExportObject<?>> dependencies) {
            export.setDependencies(List.copyOf(dependencies));
            return this;
        }

        /**
         * Invoke this method to set the error handler to be notified if this export fails. Only one error handler may
         * be set. Exactly one of the onError and onSuccess handlers will be invoked.
         * <p>
         * Not synchronized, it is expected that the provided callback handles thread safety itself.
         *
         * @param errorHandler the error handler to be notified
         * @return this builder
         */
        public ExportBuilder<T> onError(final ExportErrorHandler errorHandler) {
            if (this.errorHandler != null) {
                throw new IllegalStateException("error handler already set");
            } else if (export.hasHadWorkSet) {
                throw new IllegalStateException("error handler must be set before work is submitted");
            }
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Invoke this method to set the error handler to be notified if this export fails. Only one error handler may
         * be set. Exactly one of the onError and onSuccess handlers will be invoked.
         * <p>
         * Not synchronized, it is expected that the provided callback handles thread safety itself.
         *
         * @param errorHandler the error handler to be notified
         * @return this builder
         */
        public ExportBuilder<T> onErrorHandler(final ExportErrorGrpcHandler errorHandler) {
            return onError(((resultState, errorContext, cause, dependentExportId) -> {
                if (cause instanceof StatusRuntimeException) {
                    errorHandler.onError((StatusRuntimeException) cause);
                    return;
                }

                final String dependentStr = dependentExportId == null ? ""
                        : (" (related parent export id: " + dependentExportId + ")");
                if (cause == null) {
                    if (resultState == ExportNotification.State.CANCELLED) {
                        errorHandler.onError(Exceptions.statusRuntimeException(Code.CANCELLED,
                                "Export is cancelled" + dependentStr));
                    } else {
                        errorHandler.onError(Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                                "Export in state " + resultState + dependentStr));
                    }
                } else {
                    errorHandler.onError(Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                            "Details Logged w/ID '" + errorContext + "'" + dependentStr));
                }
            }));
        }

        /**
         * Invoke this method to set the error handler to be notified if this export fails. Only one error handler may
         * be set. This is a convenience method for use with {@link StreamObserver}. Exactly one of the onError and
         * onSuccess handlers will be invoked.
         * <p>
         * Invoking onError will be synchronized on the StreamObserver instance, so callers can rely on that mechanism
         * to deal with more than one thread trying to write to the stream.
         *
         * @param streamObserver the streamObserver to be notified of any error
         * @return this builder
         */
        public ExportBuilder<T> onError(StreamObserver<?> streamObserver) {
            return onErrorHandler(statusRuntimeException -> {
                safelyError(streamObserver, statusRuntimeException);
            });
        }

        /**
         * Invoke this method to set the onSuccess handler to be notified if this export succeeds. Only one success
         * handler may be set. Exactly one of the onError and onSuccess handlers will be invoked.
         * <p>
         * Not synchronized, it is expected that the provided callback handles thread safety itself.
         *
         * @param successHandler the onSuccess handler to be notified
         * @return this builder
         */
        public ExportBuilder<T> onSuccess(final Consumer<? super T> successHandler) {
            if (this.successHandler != null) {
                throw new IllegalStateException("success handler already set");
            } else if (export.hasHadWorkSet) {
                throw new IllegalStateException("success handler must be set before work is submitted");
            }
            this.successHandler = successHandler;
            return this;
        }

        /**
         * Invoke this method to set the onSuccess handler to be notified if this export succeeds. Only one success
         * handler may be set. Exactly one of the onError and onSuccess handlers will be invoked.
         * <p>
         * Not synchronized, it is expected that the provided callback handles thread safety itself.
         *
         * @param successHandler the onSuccess handler to be notified
         * @return this builder
         */
        public ExportBuilder<T> onSuccess(final Runnable successHandler) {
            return onSuccess(ignored -> successHandler.run());
        }

        /**
         * This method is the final method for submitting an export to the session. The provided callable is enqueued on
         * the scheduler when all dependencies have been satisfied. Only the dependencies supplied to the builder are
         * guaranteed to be resolved when the exportMain is executing.
         * <p>
         * Warning! It is the SessionState owner's responsibility to wait to release any dependency until after this
         * exportMain callable/runnable has complete.
         *
         * @param exportMain the callable that generates the export
         * @return the submitted export object
         */
        public ExportObject<T> submit(final Callable<T> exportMain) {
            export.setWork(exportMain, errorHandler, successHandler, requiresSerialQueue);
            return export;
        }

        /**
         * This method is the final method for submitting an export to the session. The provided runnable is enqueued on
         * the scheduler when all dependencies have been satisfied. Only the dependencies supplied to the builder are
         * guaranteed to be resolved when the exportMain is executing.
         * <p>
         * Warning! It is the SessionState owner's responsibility to wait to release any dependency until after this
         * exportMain callable/runnable has complete.
         *
         * @param exportMain the runnable to execute once dependencies have resolved
         * @return the submitted export object
         */
        public ExportObject<T> submit(final Runnable exportMain) {
            return submit(() -> {
                exportMain.run();
                return null;
            });
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
        public int getExportId() {
            return exportId;
        }
    }

    private static final KeyedIntObjectKey<ExportObject<?>> EXPORT_OBJECT_ID_KEY =
            new KeyedIntObjectKey.BasicStrict<ExportObject<?>>() {
                @Override
                public int getIntKey(final ExportObject<?> exportObject) {
                    return exportObject.exportId;
                }
            };

    private final KeyedIntObjectHash.ValueFactory<ExportObject<?>> EXPORT_OBJECT_VALUE_FACTORY =
            new KeyedIntObjectHash.ValueFactory.Strict<ExportObject<?>>() {
                @Override
                public ExportObject<?> newValue(final int key) {
                    if (isExpired()) {
                        throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "session has expired");
                    }

                    return new ExportObject<>(SessionState.this.errorTransformer, SessionState.this, key);
                }
            };
}
