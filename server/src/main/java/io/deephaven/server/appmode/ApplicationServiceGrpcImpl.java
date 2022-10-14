/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.appmode;

import com.google.rpc.Code;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc;
import io.deephaven.proto.backplane.grpc.FieldInfo;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;
import io.deephaven.server.object.TypeLookup;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.DateTimeUtils;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

@Singleton
public class ApplicationServiceGrpcImpl extends ApplicationServiceGrpc.ApplicationServiceImplBase
        implements ScriptSession.Listener, ApplicationState.Listener {
    private static final Logger log = LoggerFactory.getLogger(ApplicationServiceGrpcImpl.class);

    private static final String QUERY_SCOPE_DESCRIPTION = "query scope variable";

    private final Scheduler scheduler;
    private final SessionService sessionService;
    private final TypeLookup typeLookup;

    /** The list of Field listeners */
    private final Set<Subscription> subscriptions = new LinkedHashSet<>();

    /** A schedulable job that flushes pending field changes to all listeners. */
    private final FieldUpdatePropagationJob propagationJob = new FieldUpdatePropagationJob();

    /** The state, as known by subscriptions */
    private final Map<AppFieldId, FieldInfo> known = new LinkedHashMap<>();

    /** The accumulated state changes, as known by us and not yet sent to subscriptions */
    private final Map<AppFieldId, State> accumulated = new LinkedHashMap<>();

    @Inject
    public ApplicationServiceGrpcImpl(
            final Scheduler scheduler,
            final SessionService sessionService,
            final TypeLookup typeLookup) {
        this.scheduler = scheduler;
        this.sessionService = sessionService;
        this.typeLookup = typeLookup;
    }

    @Override
    public synchronized void onScopeChanges(final ScriptSession scriptSession, final ScriptSession.Changes changes) {
        if (ConsoleServiceGrpcImpl.REMOTE_CONSOLE_DISABLED || changes.isEmpty()) {
            return;
        }
        for (Entry<String, String> e : changes.removed.entrySet()) {
            remove(AppFieldId.fromScopeName(e.getKey()));
        }
        for (Entry<String, String> e : changes.updated.entrySet()) {
            update(AppFieldId.fromScopeName(e.getKey()), QUERY_SCOPE_DESCRIPTION, e.getValue());
        }
        for (Entry<String, String> e : changes.created.entrySet()) {
            create(AppFieldId.fromScopeName(e.getKey()), QUERY_SCOPE_DESCRIPTION, e.getValue());
        }
        schedulePropagationOrClearIncrementalState();
    }

    @Override
    public synchronized void onRemoveField(ApplicationState app, Field<?> oldField) {
        remove(AppFieldId.from(app, oldField.name()));
        schedulePropagationOrClearIncrementalState();
    }

    @Override
    public synchronized void onNewField(final ApplicationState app, final Field<?> field) {
        final AppFieldId id = AppFieldId.from(app, field.name());
        final String type = typeLookup.type(field.value()).orElse(null);
        create(id, field.description().orElse(null), type);
        schedulePropagationOrClearIncrementalState();
    }

    private void schedulePropagationOrClearIncrementalState() {
        if (!subscriptions.isEmpty()) {
            propagationJob.markUpdates();
        } else {
            // Run on current thread instead of scheduler
            propagateUpdates();
        }
    }

    private synchronized void propagateUpdates() {
        propagationJob.markRunning();
        final Updater updater = new Updater();
        for (State state : accumulated.values()) {
            state.append(updater);
        }
        accumulated.clear();
        if (!updater.isEmpty() && !subscriptions.isEmpty()) {
            final FieldsChangeUpdate update = updater.build();
            // Send updates to all subscriptions, if they fail to handle the update, cancel the subscription
            List<Subscription> toCancel = new ArrayList<>(subscriptions);
            toCancel.removeIf(s -> s.send(update));
            toCancel.forEach(Subscription::onCancel);
        }
    }

    @Override
    public synchronized void listFields(ListFieldsRequest request,
            StreamObserver<FieldsChangeUpdate> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final Subscription subscription = new Subscription(session, responseObserver);

            final FieldsChangeUpdate.Builder responseBuilder = FieldsChangeUpdate.newBuilder();
            for (FieldInfo fieldInfo : known.values()) {
                responseBuilder.addCreated(fieldInfo);
            }
            if (subscription.send(responseBuilder.build())) {
                subscriptions.add(subscription);
            } else {
                subscription.onCancel();
            }
        });
    }

    synchronized void remove(Subscription sub) {
        if (subscriptions.remove(sub)) {
            sub.notifyObserverAborted();
        }
    }

    private static TypedTicket typedTicket(AppFieldId id, String type) {
        final TypedTicket.Builder ticket = TypedTicket.newBuilder().setTicket(id.getTicket());
        if (type != null) {
            ticket.setType(type);
        }
        return ticket.build();
    }

    private class FieldUpdatePropagationJob implements Runnable {
        /** This interval is used as a debounce to prevent spamming field changes from a broken application. */
        private static final long UPDATE_INTERVAL_MS = 250;

        // guarded by parent sync
        private long lastScheduledMillis = 0;
        private boolean isScheduled = false;

        @Override
        public void run() {
            try {
                propagateUpdates();
            } catch (final Throwable t) {
                log.error(t).append("failed to propagate field changes").endl();
            }
        }

        // must be sync wrt parent
        private void markRunning() {
            isScheduled = false;
        }

        // must be sync wrt parent
        private boolean markUpdates() {
            // Note: we don't have to worry about the potential for a dirty state while we are propagating updates since
            // the propagation of updates and changing of fields is synchronized wrt parent.
            if (isScheduled) {
                return false;
            }
            isScheduled = true;
            final long now = scheduler.currentTimeMillis();
            final long nextMin = lastScheduledMillis + UPDATE_INTERVAL_MS;
            if (lastScheduledMillis > 0 && now >= nextMin) {
                lastScheduledMillis = now;
                scheduler.runImmediately(this);
            } else {
                lastScheduledMillis = nextMin;
                scheduler.runAfterDelay(nextMin - now, this);
            }
            return true;
        }
    }

    /**
     * Subscription is a small helper class that kills the listener's subscription when its session expires.
     *
     * @implNote gRPC observers are not thread safe; we must synchronize around observer communication
     */
    private class Subscription implements Closeable {
        private final SessionState session;

        // guarded by parent sync
        private final StreamObserver<FieldsChangeUpdate> observer;

        public Subscription(final SessionState session, final StreamObserver<FieldsChangeUpdate> observer) {
            this.session = session;
            this.observer = observer;
            if (observer instanceof ServerCallStreamObserver) {
                final ServerCallStreamObserver<FieldsChangeUpdate> serverCall =
                        (ServerCallStreamObserver<FieldsChangeUpdate>) observer;
                serverCall.setOnCancelHandler(this::onCancel);
            }
            session.addOnCloseCallback(this);
        }

        void onCancel() {
            if (session.removeOnCloseCallback(this)) {
                close();
            }
        }

        @Override
        public void close() {
            remove(this);
        }

        /**
         * Sends an update to the subscribed client. Returns true if successful - if false, the client is no longer
         * listening and this subscription should be canceled after iteration.
         *
         * @param changes the updates to inform the client of
         * @return true if the message was sent, false if an error occurred and the subscription should be canceled
         */
        private boolean send(FieldsChangeUpdate changes) {
            try {
                observer.onNext(changes);
            } catch (RuntimeException ignored) {
                return false;
            }
            return true;
        }

        // must be sync wrt parent
        private void notifyObserverAborted() {
            GrpcUtil.safelyExecute(
                    () -> observer.onError(GrpcUtil.statusRuntimeException(Code.ABORTED, "subscription cancelled")));
        }
    }

    private void create(AppFieldId id, String description, String type) {
        accumulated(id).create(description, type);
    }

    private void update(AppFieldId id, String description, String type) {
        accumulated(id).update(description, type);
    }

    private void remove(AppFieldId id) {
        accumulated(id).remove();
    }

    private State accumulated(AppFieldId id) {
        return accumulated.computeIfAbsent(id, this::newState);
    }

    private State newState(AppFieldId id) {
        final FieldInfo existingInfo = known.get(id);
        return existingInfo == null ? State.emptyState(id) : State.existingState(id, existingInfo);
    }

    private enum CUR {
        NOOP, CREATED, UPDATED, REMOVED
    }

    private static class State {

        public static State emptyState(AppFieldId id) {
            return new State(id, null);
        }

        public static State existingState(AppFieldId id, FieldInfo existing) {
            return new State(id, Objects.requireNonNull(existing));
        }

        private final AppFieldId id;
        private final FieldInfo existing;
        private String description;
        private String type;

        // If existing == null, may only be CREATED or NOOP.
        // If existing != null, may only be REMOVED or UPDATED.
        private CUR out;

        private State(AppFieldId id, FieldInfo existing) {
            this.id = Objects.requireNonNull(id);
            this.existing = existing;
            this.out = existing == null ? CUR.NOOP : CUR.UPDATED;
        }

        public void create(String description, String type) {
            if (existing == null) {
                transition(CUR.NOOP, CUR.CREATED);
            } else {
                transition(CUR.REMOVED, CUR.UPDATED);
            }
            this.description = description;
            this.type = type;
        }

        public void update(String description, String type) {
            if (existing == null) {
                check(CUR.CREATED);
            } else {
                check(CUR.UPDATED);
            }
            this.description = description;
            this.type = type;
        }

        public void remove() {
            if (existing == null) {
                transition(CUR.CREATED, CUR.NOOP);
            } else {
                transition(CUR.UPDATED, CUR.REMOVED);
            }
            // If we send a remove, we'll be basing it off of our existing info
            this.description = null;
            this.type = null;
        }

        public void append(Updater updater) {
            switch (out) {
                case NOOP:
                    break;
                case CREATED:
                    updater.onCreated(id, fieldInfo());
                    break;
                case UPDATED:
                    updater.onUpdated(id, fieldInfo());
                    break;
                case REMOVED:
                    updater.onRemoved(id, Objects.requireNonNull(existing));
                    break;
                default:
                    throw new IllegalStateException("Unexpected state " + out);
            }
        }

        private void transition(CUR from, CUR to) {
            if (out != from) {
                throw new IllegalStateException(
                        String.format("Expected transition from=%s to=%s, actual=%s", from, to, out));
            }
            out = to;
        }

        private void check(CUR expected) {
            if (out != expected) {
                throw new IllegalStateException(String.format("Expected state=%s, actual=%s", expected, out));
            }
        }

        private FieldInfo fieldInfo() {
            return FieldInfo.newBuilder()
                    .setTypedTicket(typedTicket(id, type))
                    .setFieldName(id.fieldName)
                    .setFieldDescription(description == null ? "" : description)
                    .setApplicationId(id.applicationId())
                    .setApplicationName(id.applicationName())
                    .build();
        }
    }

    /**
     * Modifies {@code known} state while also building {@link FieldsChangeUpdate}.
     */
    private class Updater {
        private final FieldsChangeUpdate.Builder builder = FieldsChangeUpdate.newBuilder();
        private boolean isEmpty = true;

        boolean isEmpty() {
            return isEmpty;
        }

        void onCreated(AppFieldId id, FieldInfo info) {
            builder.addCreated(info);
            known.put(id, info);
            isEmpty = false;
        }

        void onUpdated(AppFieldId id, FieldInfo info) {
            builder.addUpdated(info);
            known.put(id, info);
            isEmpty = false;
        }

        void onRemoved(AppFieldId id, FieldInfo info) {
            builder.addRemoved(info);
            known.remove(id);
            isEmpty = false;
        }

        FieldsChangeUpdate build() {
            return builder.build();
        }
    }
}
