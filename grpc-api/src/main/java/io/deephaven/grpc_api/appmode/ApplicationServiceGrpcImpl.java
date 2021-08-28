package io.deephaven.grpc_api.appmode;

import com.google.rpc.Code;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.CustomField;
import io.deephaven.appmode.Field;
import io.deephaven.db.plot.FigureWidget;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.ScriptSession;
import io.deephaven.db.util.liveness.LivenessArtifact;
import io.deephaven.db.util.liveness.LivenessManager;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.v2.DynamicNode;
import io.deephaven.grpc_api.barrage.util.BarrageSchemaUtil;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api.util.Scheduler;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc;
import io.deephaven.proto.backplane.grpc.CustomInfo;
import io.deephaven.proto.backplane.grpc.FieldInfo;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.FigureInfo;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.TableInfo;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Singleton
public class ApplicationServiceGrpcImpl extends ApplicationServiceGrpc.ApplicationServiceImplBase
        implements ScriptSession.Listener, ApplicationState.Listener {
    private static final Logger log = LoggerFactory.getLogger(ApplicationServiceGrpcImpl.class);

    private final AppMode mode;
    private final Scheduler scheduler;
    private final SessionService sessionService;
    private final LivenessTracker tracker = new LivenessTracker();

    /** The list of Field listeners */
    private final Set<Subscription> subscriptions = new LinkedHashSet<>();

    /** A schedulable job that flushes pending field changes to all listeners. */
    private final FieldUpdatePropagationJob propagationJob = new FieldUpdatePropagationJob();

    /** Which fields have been updated since we last propagated? */
    private final Map<AppFieldId, Field<?>> addedFields = new HashMap<>();
    /** Which fields have been removed since we last propagated? */
    private final Set<AppFieldId> removedFields = new HashSet<>();
    /** Which fields have been updated since we last propagated? */
    private final Set<AppFieldId> updatedFields = new HashSet<>();
    /** Which [remaining] fields have we seen? */
    private final Map<AppFieldId, Field<?>> knownFieldMap = new HashMap<>();

    @Inject
    public ApplicationServiceGrpcImpl(final AppMode mode,
            final Scheduler scheduler,
            final SessionService sessionService) {
        this.mode = mode;
        this.scheduler = scheduler;
        this.sessionService = sessionService;
    }

    @Override
    public synchronized void onScopeChanges(final ScriptSession scriptSession, final ScriptSession.Changes changes) {
        if (!mode.hasVisibilityToConsoleExports() || changes.isEmpty()) {
            return;
        }

        changes.removed.keySet().stream().map(AppFieldId::fromScopeName).forEach(id -> {
            updatedFields.remove(id);
            Field<?> oldField = addedFields.remove(id);
            if (oldField != null) {
                tracker.maybeUnmanage(oldField.value());
            } else {
                removedFields.add(id);
            }
        });

        for (final String name : changes.updated.keySet()) {
            final AppFieldId id = AppFieldId.fromScopeName(name);

            boolean recentField = false;
            ScopeField field = (ScopeField) addedFields.get(id);
            if (field == null) {
                field = (ScopeField) knownFieldMap.get(id);
            } else {
                recentField = true;
            }

            // Note the order w.r.t. the tracker is intentional to avoid dropping ref count to zero
            Object newValue = scriptSession.unwrapObject(scriptSession.getVariable(name));
            Object oldValue = field.value();

            if (newValue != oldValue) {
                tracker.maybeManage(newValue);
                tracker.maybeUnmanage(oldValue);
            }

            field.value = newValue;
            if (!recentField) {
                updatedFields.add(id);
            }
        }

        for (final String name : changes.created.keySet()) {
            final AppFieldId id = AppFieldId.fromScopeName(name);
            final Object value = scriptSession.unwrapObject(scriptSession.getVariable(name));
            final ScopeField field = new ScopeField(name, value);
            final FieldInfo fieldInfo = getFieldInfo(id, field);
            if (fieldInfo == null) {
                // The script session should not have told us about this variable...
                throw new IllegalStateException(
                        String.format("Field information could not be generated for scope variable '%s'", name));
            }
            tracker.maybeManage(field.value);
            final Field<?> oldField = addedFields.put(id, field);
            if (oldField != null) {
                throw new IllegalStateException(
                        String.format("Script session notified of new field but was already existing '%s'", name));
            }
        }

        schedulePropagationOrClearIncrementalState();
    }

    @Override
    public synchronized void onRemoveField(ApplicationState app, Field<?> oldField) {
        if (!mode.hasVisibilityToAppExports()) {
            return;
        }

        final AppFieldId id = AppFieldId.from(app, oldField.name());
        Field<?> recentlyAdded = addedFields.remove(id);
        if (recentlyAdded != null) {
            tracker.maybeUnmanage(recentlyAdded.value());
            return;
        }
        updatedFields.remove(id);
        removedFields.add(id);

        schedulePropagationOrClearIncrementalState();
    }

    @Override
    public synchronized void onNewField(final ApplicationState app, final Field<?> field) {
        if (!mode.hasVisibilityToAppExports()) {
            return;
        }

        final AppFieldId id = AppFieldId.from(app, field.name());
        final FieldInfo fieldInfo = getFieldInfo(id, field);
        if (fieldInfo == null) {
            throw new IllegalStateException(String.format("Field information could not be generated for field '%s/%s'",
                    app.id(), field.name()));
        }

        tracker.maybeManage(field.value());

        final Field<?> knownField = knownFieldMap.get(id);
        if (knownField != null && !removedFields.contains(id)) {
            updatedFields.add(id);
            tracker.maybeUnmanage(knownField.value());
            knownFieldMap.put(id, field);
        } else {
            final Field<?> recentlyAdded = addedFields.put(id, field);
            if (recentlyAdded != null) {
                tracker.maybeUnmanage(recentlyAdded.value());
            }
        }

        schedulePropagationOrClearIncrementalState();
    }

    private void schedulePropagationOrClearIncrementalState() {
        if (!subscriptions.isEmpty()) {
            propagationJob.markUpdates();
        } else {
            // don't have to wait for the propagation job to accept these fields into the known field map
            knownFieldMap.keySet().removeAll(removedFields);
            knownFieldMap.putAll(addedFields);

            // let's not duplicate information when a client does actually join
            addedFields.clear();
            removedFields.clear();
            updatedFields.clear();
        }
    }

    private synchronized void propagateUpdates() {
        propagationJob.markRunning();
        final FieldsChangeUpdate.Builder builder = FieldsChangeUpdate.newBuilder();

        // We only unmanage when we can no longer send it to a new observer.
        removedFields.forEach(id -> {
            final Field<?> oldField = knownFieldMap.get(id);
            if (oldField == null) {
                log.error().append("Removing old field but field not known; fieldId = ").append(id.toString()).endl();
            } else {
                tracker.maybeUnmanage(oldField.value());
                builder.addRemoved(getRemovedFieldInfo(id));
            }
        });
        removedFields.clear();

        // We manage all referents when they are added.
        addedFields.forEach((id, field) -> {
            knownFieldMap.put(id, field);
            builder.addCreated(getFieldInfo(id, field));
        });
        addedFields.clear();

        // Updated fields are managed/unmanaged during notification of update.
        updatedFields.forEach(id -> builder.addUpdated(getFieldInfo(id, knownFieldMap.get(id))));
        updatedFields.clear();
        final FieldsChangeUpdate update = builder.build();

        subscriptions.forEach(sub -> sub.send(update));
    }

    @Override
    public synchronized void listFields(ListFieldsRequest request,
            StreamObserver<FieldsChangeUpdate> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final Subscription subscription = new Subscription(session, responseObserver);

            final FieldsChangeUpdate.Builder responseBuilder = FieldsChangeUpdate.newBuilder();
            knownFieldMap.forEach((appFieldId, field) -> responseBuilder.addCreated(getFieldInfo(appFieldId, field)));

            if (subscription.send(responseBuilder.build())) {
                subscriptions.add(subscription);
            }
        });
    }

    synchronized void remove(Subscription sub) {
        if (subscriptions.remove(sub)) {
            sub.notifyObserverAborted();
        }
    }

    private static FieldInfo getRemovedFieldInfo(final AppFieldId id) {
        return FieldInfo.newBuilder()
                .setTicket(id.getTicket())
                .setFieldName(id.fieldName)
                .setApplicationId(id.applicationId())
                .setApplicationName(id.applicationName())
                .build();
    }

    private static FieldInfo getFieldInfo(final AppFieldId id, final Field<?> field) {
        if (field instanceof CustomField) {
            return getCustomFieldInfo(id, (CustomField<?>) field);
        }
        return getStandardFieldInfo(id, field);
    }

    private static FieldInfo getCustomFieldInfo(final AppFieldId id, final CustomField<?> field) {
        return FieldInfo.newBuilder()
                .setTicket(id.getTicket())
                .setFieldName(id.fieldName)
                .setFieldType(FieldInfo.FieldType.newBuilder()
                        .setCustom(CustomInfo.newBuilder()
                                .setType(field.type())
                                .build())
                        .build())
                .setFieldDescription(field.description().orElse(""))
                .setApplicationId(id.applicationId())
                .setApplicationName(id.applicationName())
                .build();
    }

    private static FieldInfo getStandardFieldInfo(final AppFieldId id, final Field<?> field) {
        // Note that this method accepts any Field and not just StandardField
        final FieldInfo.FieldType fieldType = fetchFieldType(field.value());

        if (fieldType == null) {
            throw new IllegalArgumentException("Application Field is not of standard type; use CustomField instead");
        }

        return FieldInfo.newBuilder()
                .setTicket(id.getTicket())
                .setFieldName(id.fieldName)
                .setFieldType(fieldType)
                .setFieldDescription(field.description().orElse(""))
                .setApplicationId(id.applicationId())
                .setApplicationName(id.applicationName())
                .build();
    }

    private static FieldInfo.FieldType fetchFieldType(final Object obj) {
        if (obj instanceof Table) {
            final Table table = (Table) obj;
            return FieldInfo.FieldType.newBuilder().setTable(TableInfo.newBuilder()
                    .setSchemaHeader(BarrageSchemaUtil.schemaBytesFromTable(table))
                    .setIsStatic(!table.isLive())
                    .setSize(table.size())
                    .build()).build();
        }
        if (obj instanceof FigureWidget) {
            return FieldInfo.FieldType.newBuilder().setFigure(FigureInfo.getDefaultInstance()).build();
        }

        return null;
    }

    private static class ScopeField implements Field<Object> {
        final String name;
        Object value;

        ScopeField(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Object value() {
            return value;
        }

        @Override
        public Optional<String> description() {
            return Optional.of("query scope variable");
        }
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
            if (!isScheduled) {
                throw new IllegalStateException("Job is running without being scheduled");
            }
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
            final long now = scheduler.currentTime().getMillis();
            final long nextMin = lastScheduledMillis + UPDATE_INTERVAL_MS;
            if (lastScheduledMillis > 0 && now >= nextMin) {
                lastScheduledMillis = now;
                scheduler.runImmediately(this);
            } else {
                lastScheduledMillis = nextMin;
                scheduler.runAtTime(DBTimeUtils.millisToTime(nextMin), this);
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

        // must be sync wrt parent
        private boolean send(FieldsChangeUpdate changes) {
            try {
                observer.onNext(changes);
            } catch (RuntimeException ignored) {
                onCancel();
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


    private static class LivenessTracker extends LivenessArtifact {
        private <T> void maybeManage(T object) {
            if (object instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(object)) {
                manage((LivenessReferent) object);
            }
        }

        private <T> void maybeUnmanage(T object) {
            if (object instanceof LivenessReferent && DynamicNode.notDynamicOrIsRefreshing(object)) {
                unmanage((LivenessReferent) object);
            }
        }
    }
}
