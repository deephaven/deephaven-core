package io.deephaven.grpc_api.appmode;

import com.google.rpc.Code;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.CustomField;
import io.deephaven.appmode.Field;
import io.deephaven.db.plot.FigureWidget;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.ScriptSession;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
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

    /** The list of Field listeners */
    private final Set<Subscription> subscriptions = new LinkedHashSet<>();

    /** A schedulable job that flushes pending field changes to all listeners. */
    private final FieldUpdatePropagationJob propagationJob = new FieldUpdatePropagationJob();

    /** Which fields have been updated since we last propagated? */
    private final Set<AppFieldId> addedFields = new HashSet<>();
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
            if (!addedFields.remove(id)) {
                removedFields.add(id);
            }
            knownFieldMap.remove(id);
        });

        for (final String name : changes.updated.keySet()) {
            final AppFieldId id = AppFieldId.fromScopeName(name);
            final ScopeField field = (ScopeField) knownFieldMap.get(id);
            field.value = scriptSession.unwrapObject(scriptSession.getVariable(name));
            updatedFields.add(id);
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
            addedFields.add(id);
            knownFieldMap.put(id, field);
        }

        schedulePropagationOrClearIncrementalState();
    }

    @Override
    public synchronized void onRemoveField(ApplicationState app, String name) {
        if (!mode.hasVisibilityToAppExports()) {
            return;
        }

        final AppFieldId id = AppFieldId.from(app, name);
        if (knownFieldMap.remove(id) != null) {
            updatedFields.remove(id);
            if (!addedFields.remove(id)) {
                removedFields.add(id);
            }
        } else {
            log.warn().append("Removing unknown field: ").append(id.toString()).endl();
        }

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

        final Field<?> oldValue = knownFieldMap.put(id, field);
        if (oldValue == null) {
            addedFields.add(id);
        } else {
            updatedFields.add(id);
        }

        schedulePropagationOrClearIncrementalState();
    }

    private void schedulePropagationOrClearIncrementalState() {
        if (!subscriptions.isEmpty()) {
            propagationJob.markUpdates();
        } else {
            addedFields.clear();
            removedFields.clear();
            updatedFields.clear();
        }
    }

    private synchronized void propagateUpdates() {
        propagationJob.markRunning();
        final FieldsChangeUpdate.Builder builder = FieldsChangeUpdate.newBuilder();
        addedFields.forEach(id -> builder.addCreated(getFieldInfo(id, knownFieldMap.get(id))));
        addedFields.clear();

        removedFields.forEach(id -> builder.addRemoved(getRemovedFieldInfo(id)));
        removedFields.clear();

        updatedFields.forEach(id -> builder.addUpdated(getFieldInfo(id, knownFieldMap.get(id))));
        updatedFields.clear();
        final FieldsChangeUpdate update = builder.build();

        log.info().append("fields updated: ").append(update.toString()).endl();
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

        synchronized boolean send(FieldsChangeUpdate changes) {
            try {
                observer.onNext(changes);
            } catch (RuntimeException ignored) {
                onCancel();
                return false;
            }
            return true;
        }

        void onCancel() {
            if (session.removeOnCloseCallback(this)) {
                close();
            }
        }

        @Override
        public synchronized void close() {
            synchronized (ApplicationServiceGrpcImpl.this) {
                subscriptions.remove(this);
            }
            GrpcUtil.safelyExecute(
                    () -> observer.onError(GrpcUtil.statusRuntimeException(Code.ABORTED, "subscription cancelled")));
        }
    }
}
