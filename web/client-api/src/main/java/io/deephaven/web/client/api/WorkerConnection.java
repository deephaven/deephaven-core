//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.common.io.BaseEncoding;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import com.vertispan.tsdefs.annotations.TsIgnore;
import elemental2.core.JsSet;
import elemental2.core.JsWeakMap;
import elemental2.core.TypedArray;
import elemental2.core.Uint8Array;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.flightjs.protocol.BrowserFlight;
import io.deephaven.flightjs.protocol.BrowserFlightServiceGrpc;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc;
import io.deephaven.proto.backplane.grpc.ApplyPreviewColumnsRequest;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc;
import io.deephaven.proto.backplane.grpc.ConfigValue;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse;
import io.deephaven.proto.backplane.grpc.EmptyTableRequest;
import io.deephaven.proto.backplane.grpc.ExportRequest;
import io.deephaven.proto.backplane.grpc.ExportResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse;
import io.deephaven.proto.backplane.grpc.FetchTableRequest;
import io.deephaven.proto.backplane.grpc.FieldInfo;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HierarchicalTableServiceGrpc;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.MergeTablesRequest;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc;
import io.deephaven.proto.backplane.grpc.PartitionedTableServiceGrpc;
import io.deephaven.proto.backplane.grpc.PublishRequest;
import io.deephaven.proto.backplane.grpc.PublishResponse;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.StorageServiceGrpc;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.TerminationNotificationRequest;
import io.deephaven.proto.backplane.grpc.TerminationNotificationResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionData;
import io.deephaven.proto.backplane.script.grpc.LogSubscriptionRequest;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.stream.AuthenticationInterceptor;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.barrage.stream.TrailersCapturingInterceptor;
import io.deephaven.web.client.api.batch.RequestBatcher;
import io.deephaven.web.client.api.batch.TableConfig;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import io.deephaven.web.client.api.impl.TicketAndPromise;
import io.deephaven.web.client.api.lifecycle.HasLifecycle;
import io.deephaven.web.client.api.parse.JsDataHandler;
import io.deephaven.web.client.api.state.StateCache;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.widget.JsWidget;
import io.deephaven.web.client.api.widget.JsWidgetExportedObject;
import io.deephaven.web.client.api.widget.plot.JsFigure;
import io.deephaven.web.client.fu.JsItr;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.ide.SharedExportBytesUnion;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.client.state.HasTableBinding;
import io.deephaven.web.client.state.TableReviver;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsNullable;
import jsinterop.base.JsPropertyMap;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.gwtproject.nio.TypedArrayHelper;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Non-exported class, manages the connection to a given worker server. Exported types like QueryInfo and Table will
 * refer to this, and allow us to try to keep track of how many open tables there are, so we can close the connection if
 * not in use.
 *
 * Might make more sense to be part of QueryInfo, but this way we can WeakMap instances, check periodically if any
 * QueryInfos are left alive or event handlers still exist, and close connections that seem unused.
 *
 * Except for the delegated call from QueryInfo.getTable, none of these calls will be possible in Connecting or
 * Disconnected state if done right. Failed state is possible, and we will want to think more about handling, possible
 * re-Promise-ing all of the things, or just return stale values if we have them.
 *
 * Responsible for reconnecting to the query server when required - when that server disappears, and at least one table
 * is left un-closed.
 */
@TsIgnore
public class WorkerConnection {

    private Double scheduledAuthUpdate;
    // default to 10s, the actual value is almost certainly higher than that
    private double sessionTimeoutMs = 10_000;

    /**
     * States the connection can be in. If non-requested disconnect occurs, transition to reconnecting. If reconnect
     * fails, move to failed, and do not attempt again.
     *
     * If an error happens on the websocket connection, we'll get a close event also - since we also use onError to
     * handle failed work, and will just try one reconnect per close event.
     *
     * Reconnecting requires waiting for the worker to return to "Running" state, requesting a new auth token, and then
     * initiating that connection.
     *
     * Mostly informational, useful for debugging and error messages.
     */
    private enum State {
        Connecting, Connected,
        /**
         * Indicates that this worker was deliberately disconnected, should be reconnected again if needed.
         */
        Disconnected, Failed, Reconnecting
    }

    private final QueryConnectable<?> info;
    private final Tickets tickets;
    private final ReconnectState newSessionReconnect;
    private final TableReviver reviver;
    // un-finished fetch operations - these can fail on connection issues, won't be attempted again
    private List<Callback<Void, String>> onOpen = new ArrayList<>();

    private State state;
    private SessionServiceGrpc.SessionServiceStub sessionServiceClient;
    private TableServiceGrpc.TableServiceStub tableServiceClient;
    private ConsoleServiceGrpc.ConsoleServiceStub consoleServiceClient;
    private ApplicationServiceGrpc.ApplicationServiceStub applicationServiceClient;
    private FlightServiceGrpc.FlightServiceStub flightServiceClient;
    private BrowserFlightServiceGrpc.BrowserFlightServiceStub browserFlightServiceClient;
    private InputTableServiceGrpc.InputTableServiceStub inputTableServiceClient;
    private ObjectServiceGrpc.ObjectServiceStub objectServiceClient;
    private PartitionedTableServiceGrpc.PartitionedTableServiceStub partitionedTableServiceClient;
    private StorageServiceGrpc.StorageServiceStub storageServiceClient;
    private ConfigServiceGrpc.ConfigServiceStub configServiceClient;
    private HierarchicalTableServiceGrpc.HierarchicalTableServiceStub hierarchicalTableServiceClient;

    private final StateCache cache = new StateCache();
    private final JsWeakMap<HasTableBinding, RequestBatcher> batchers = new JsWeakMap<>();
    private JsWeakMap<TableTicket, JsConsumer<TableTicket>> handleCallbacks = new JsWeakMap<>();
    private JsWeakMap<TableTicket, JsConsumer<InitialTableDefinition>> definitionCallbacks = new JsWeakMap<>();
    private final Set<ClientTableState> flushable = new HashSet<>();
    private final JsSet<JsConsumer<LogItem>> logCallbacks = new JsSet<>();

    private ResponseStreamWrapper<ExportedTableUpdateMessage> exportNotifications;

    private JsSet<HasLifecycle> simpleReconnectableInstances = new JsSet<>();

    private List<LogItem> pastLogs = new ArrayList<>();
    private JsConsumer<LogItem> recordLog = pastLogs::add;
    private ResponseStreamWrapper<LogSubscriptionData> logStream;

    private Context.CancellableContext terminationStream;

    private final JsSet<JsConsumer<JsVariableChanges>> fieldUpdatesCallback = new JsSet<>();
    private Map<String, JsVariableDefinition> knownFields = new HashMap<>();
    private ResponseStreamWrapper<FieldsChangeUpdate> fieldsChangeUpdateStream;

    private ConfigurationConstantsResponse constants;

    public WorkerConnection(QueryConnectable<?> info) {
        this.info = info;

        this.tickets = new Tickets();
        state = State.Connecting;
        this.reviver = new TableReviver(this);

        sessionServiceClient = info.createStub(SessionServiceGrpc::newStub);
        tableServiceClient = info.createStub(TableServiceGrpc::newStub);
        consoleServiceClient = info.createStub(ConsoleServiceGrpc::newStub);
        flightServiceClient = info.createStub(FlightServiceGrpc::newStub);
        applicationServiceClient = info.createStub(ApplicationServiceGrpc::newStub);
        browserFlightServiceClient = info.createStub(BrowserFlightServiceGrpc::newStub);
        inputTableServiceClient = info.createStub(InputTableServiceGrpc::newStub);
        objectServiceClient = info.createStub(ObjectServiceGrpc::newStub);
        partitionedTableServiceClient = info.createStub(PartitionedTableServiceGrpc::newStub);
        storageServiceClient = info.createStub(StorageServiceGrpc::newStub);
        configServiceClient = info.createStub(ConfigServiceGrpc::newStub);
        hierarchicalTableServiceClient = info.createStub(HierarchicalTableServiceGrpc::newStub);

        newSessionReconnect = new ReconnectState(this::connectToWorker);

        // start connection
        newSessionReconnect.initialConnection();
    }

    /**
     * Creates a new session based on the current auth info, and attempts to re-create all tables and other objects that
     * were currently open.
     *
     * First we assume that the auth token provider is valid, and ask for a new token to provide to the worker.
     *
     * Given that token, we create a new session on the worker server.
     *
     * When a table is first fetched, it might fail - the worker connection will keep trying to connect even if the
     * failure is in one of the above steps. A later attempt to fetch that table may succeed however.
     *
     * Once the table has been successfully fetched, after each reconnect until the table is close()d we'll attempt to
     * restore the table by re-fetching the table, then reapplying all operations on it.
     */
    private void connectToWorker() {
        info.onReady()
                .then(queryWorkerRunning -> authUpdate()).then(newSession -> {
                    // subscribe to fatal errors
                    subscribeToTerminationNotification();

                    state = State.Connected;

                    JsLog.debug("Connected to worker, ensuring all states are refreshed");
                    // mark that we succeeded
                    newSessionReconnect.success();

                    if (newSession) {
                        // assert false : "Can't yet rebuild connections with new auth, please log in again";
                        // nuke pending callbacks, we'll remake them
                        handleCallbacks = new JsWeakMap<>();
                        definitionCallbacks = new JsWeakMap<>();

                        // for each cts in the cache, get all with active subs
                        ClientTableState[] hasActiveSubs = cache.getAllStates().stream()
                                .peek(cts -> {
                                    cts.getHandle().setConnected(false);
                                    cts.forActiveLifecycles(item -> {
                                        assert !(item instanceof JsTable) ||
                                                ((JsTable) item).state() == cts
                                                : "Invalid table state " + item + " does not point to state " + cts;
                                        item.suppressEvents();
                                    });
                                })
                                .filter(cts -> !cts.isEmpty())
                                .peek(cts -> {
                                    cts.forActiveTables(t -> {
                                        assert t.state().isAncestor(cts)
                                                : "Invalid binding " + t + " (" + t.state() + ") does not contain "
                                                        + cts;
                                    });
                                })
                                .toArray(ClientTableState[]::new);
                        // clear caches
                        // TODO verify we still need to do this, it seems like it might signify a leak
                        List<ClientTableState> inactiveStatesToRemove = cache.getAllStates().stream()
                                .filter(ClientTableState::isEmpty)
                                .collect(Collectors.toList());
                        inactiveStatesToRemove.forEach(cache::release);

                        flushable.clear();

                        reviver.revive(hasActiveSubs);

                        simpleReconnectableInstances.forEach((item, index, arr) -> item.refetch());
                    } else {
                        // wire up figures to attempt to reconnect when ready
                        simpleReconnectableInstances.forEach((item, index, arr) -> {
                            item.reconnect();
                            return null;
                        });

                        // only notify that we're back, no need to re-create or re-fetch anything
                        ClientTableState[] hasActiveSubs = cache.getAllStates().stream()
                                .filter(cts -> !cts.isEmpty())
                                .toArray(ClientTableState[]::new);

                        reviver.revive(hasActiveSubs);
                    }

                    info.connected();

                    // if any tables have been requested, make sure they start working now that we are connected
                    onOpen.forEach(c -> c.onSuccess(null));
                    onOpen.clear();

                    startExportNotificationsStream();

                    maybeRestartLogStream();

                    return Promise.resolve((Object) null);
                }, fail -> {
                    // Connection was explicitly closed. We don't want to change
                    // the status unless a `forceReconnect` is called.
                    if (state == State.Disconnected) {
                        return null;
                    }

                    // this is non-recoverable, connection/auth/registration failed, but we'll let it start again when
                    // state changes
                    state = State.Failed;
                    JsLog.debug("Failed to connect to worker.");

                    final String failure = String.valueOf(fail);

                    // notify all pending fetches that they failed
                    onOpen.forEach(c -> c.onFailure(failure));
                    onOpen.clear();

                    // if (server != null) {
                    // // explicitly disconnect from the query worker
                    // server.close();
                    // }

                    // signal that we should try again
                    connectionLost();

                    // inform the UI that it failed to connect
                    info.failureHandled("Failed to connect: " + failure);
                    return null;
                });
    }


    private boolean checkStatus(Throwable fail) {
        if (fail instanceof StatusRuntimeException) {
            // Not using Status.fromThrowable here to preserve trailers
            return checkStatus((StatusRuntimeException) fail);
        }
        return checkStatus(Status.fromThrowable(fail).asRuntimeException());
    }

    public boolean checkStatus(Status status) {
        if (status.isOk()) {
            return false;
        }
        return checkStatus(status.asRuntimeException(TrailersCapturingInterceptor.getTrailersFromContext()));
    }

    public boolean checkStatus(StatusRuntimeException status) {
        if (state == State.Disconnected) {
            return false;
        }
        if (status.getStatus().isOk()) {
            // success, ignore
            return true;
        } else if (status.getStatus().getCode() == Status.Code.UNAUTHENTICATED) {
            // fire deprecated event for now
            info.notifyConnectionError(status);

            // signal that the user needs to re-authenticate, make a new session
            // TODO (deephaven-core#3501) in theory we could make a new session for some auth types
            info.fireCriticalEvent(CoreClient.EVENT_RECONNECT_AUTH_FAILED);
        } else if (isTransportError(status.getStatus())) {
            // fire deprecated event for now
            info.notifyConnectionError(status);

            // signal that there has been a connection failure of some kind and attempt to reconnect
            info.fireEvent(CoreClient.EVENT_DISCONNECT);

            // Try again after a backoff, this may happen several times
            connectionLost();
        } // others probably are meaningful to the caller
        return false;
    }

    public static boolean isTransportError(Status status) {
        Status.Code code = status.getCode();
        return code == Status.Code.UNAVAILABLE || code == Status.Code.UNKNOWN || code == Status.Code.INTERNAL;
    }

    private void maybeRestartLogStream() {
        if (logCallbacks.size == 0) {
            return;
        }
        if (logStream != null) {
            logStream.cancel();
        }
        LogSubscriptionRequest.Builder logSubscriptionRequest = LogSubscriptionRequest.newBuilder();
        if (pastLogs.size() > 0) {
            // only ask for messages seen after the last message we recieved
            logSubscriptionRequest
                    .setLastSeenLogTimestamp(pastLogs.get(pastLogs.size() - 1).getMicrosLong());
        }
        logStream = ResponseStreamWrapper
                .of(observer -> consoleServiceClient.subscribeToLogs(logSubscriptionRequest.build(), observer));
        logStream.onData(data -> {
            LogItem logItem = new LogItem();
            logItem.setLogLevel(data.getLogLevel());
            logItem.setMessage(data.getMessage());
            logItem.setMicros(data.getMicros());

            for (JsConsumer<LogItem> callback : JsItr.iterate(logCallbacks.keys())) {
                callback.apply(logItem);
            }
        });
        logStream.onEnd(this::checkStatus);
    }

    private void startExportNotificationsStream() {
        if (exportNotifications != null) {
            exportNotifications.cancel();
        }
        exportNotifications = ResponseStreamWrapper
                .of(observer -> tableServiceClient
                        .exportedTableUpdates(ExportedTableUpdatesRequest.getDefaultInstance(), observer));
        exportNotifications.onData(update -> {
            if (!update.getUpdateFailureMessage().isEmpty()) {
                cache.get(new TableTicket(update.getExportId())).ifPresent(state1 -> {
                    state1.setResolution(ClientTableState.ResolutionState.FAILED, update.getUpdateFailureMessage());
                });
            } else {
                exportedTableUpdateMessage(new TableTicket(update.getExportId()),
                        update.getSize());
            }
        });

        // any export notification error is bad news
        exportNotifications.onStatus(this::checkStatus);
    }

    /**
     * Manages auth token update and rotation. This calls CetConfigurationConstants on a regular interval and signals to
     * the caller if a new session was created based on the auth details.
     *
     * @return a promise for when this auth is completed, with a value of true if a new session was created
     */
    private Promise<Boolean> authUpdate() {
        if (scheduledAuthUpdate != null) {
            DomGlobal.clearTimeout(scheduledAuthUpdate);
            scheduledAuthUpdate = null;
        }
        return Callbacks.<ConfigurationConstantsResponse>grpcUnaryPromiseWrapped(c -> {
            configServiceClient().getConfigurationConstants(ConfigurationConstantsRequest.getDefaultInstance(), c);
        }).then(response -> {
            // Read the timeout from the server, we'll refresh at less than that
            constants = response.message();
            ConfigValue sessionDuration = constants.getConfigValuesMap().get("http.session.durationMs");
            if (sessionDuration != null && sessionDuration.hasStringValue()) {
                sessionTimeoutMs = Double.parseDouble(sessionDuration.getStringValue());
            }

            // schedule an update based on our currently configured delay
            scheduledAuthUpdate = DomGlobal.setTimeout(ignore -> {
                authUpdate();
            }, sessionTimeoutMs / 2);
            return Promise.resolve(AuthenticationInterceptor.SESSION_CREATED.get(response.context()));
        }, err -> {
            if (err instanceof StatusRuntimeException ex) {
                if (ex.getStatus().getCode() == Status.Code.UNAUTHENTICATED) {
                    // Fire an event for the UI to attempt to re-auth
                    info.fireCriticalEvent(CoreClient.EVENT_RECONNECT_AUTH_FAILED);
                    return Promise.reject("Authentication failed, please reconnect");
                }
                checkStatus(ex);
                if (ex.getMessage() != null && !ex.getMessage().isEmpty()) {
                    return Promise.reject(ex.getMessage());
                } else {
                    return Promise
                            .reject("Error occurred while authenticating, gRPC status "
                                    + ex.getStatus().getCode().name());
                }
            } else {
                return Promise.reject(err);
            }
        });
    }

    private void subscribeToTerminationNotification() {
        terminationStream = Context.current().withCancellation();
        terminationStream.run(
                () -> sessionServiceClient.terminationNotification(TerminationNotificationRequest.getDefaultInstance(),
                        new StreamObserver<TerminationNotificationResponse>() {
                            @Override
                            public void onNext(TerminationNotificationResponse success) {
                                // welp; the server is gone -- let everyone know
                                connectionLost();

                                info.notifyServerShutdown(success);
                            }

                            @Override
                            public void onError(Throwable fail) {
                                // Errors are treated like connection issues, won't signal any shutdown
                                if (checkStatus(fail)) {
                                    // restart the termination notification
                                    subscribeToTerminationNotification();
                                } else {
                                    info.notifyConnectionError(fail);
                                    connectionLost();
                                }
                            }

                            @Override
                            public void onCompleted() {

                            }
                        }));
    }

    // @Override
    public void exportedTableUpdateMessage(TableTicket clientId, long size) {
        cache.get(clientId).ifPresent(state -> {
            state.setSize(size);
        });
    }

    // @Override

    public void connectionLost() {
        // notify all active tables and widgets that the connection is closed
        // TODO(deephaven-core#3604) when a new session is created, refetch all widgets and use that to drive reconnect
        simpleReconnectableInstances.forEach((item, index, array) -> {
            try {
                item.disconnected();
            } catch (Exception e) {
                JsLog.warn("Error in firing Figure.EVENT_DISCONNECT event", e);
            }
            return null;
        });
        info.disconnected();
        for (ClientTableState cts : cache.getAllStates()) {
            cts.forActiveLifecycles(HasLifecycle::disconnected);
        }


        if (state == State.Disconnected) {
            // deliberately closed, don't try to reopen at this time
            JsLog.debug("WorkerConnection.onClose Disconnected, not trying to reopen");
            return;
        }

        // try again
        JsLog.debug("WorkerConnection.onClose, trying to reconnect");

        state = State.Reconnecting;
        newSessionReconnect.failed();

        // fail outstanding promises, if any
        onOpen.forEach(c -> c.onFailure("Connection to server closed"));
        onOpen.clear();
    }

    @JsMethod
    public void forceReconnect() {
        JsLog.debug("pending: ", definitionCallbacks, handleCallbacks);

        // just in case it wasn't already running, mark us as reconnecting
        state = State.Reconnecting;
        newSessionReconnect.failed();
    }

    @JsMethod
    public void forceClose() {
        // explicitly mark as disconnected so reconnect isn't attempted
        state = State.Disconnected;

        // forcibly clean up the log stream and its listeners
        if (logStream != null) {
            logStream.cancel();
            logStream = null;
        }
        pastLogs.clear();
        logCallbacks.clear();

        // Stop server streams, will not reconnect
        if (terminationStream != null) {
            terminationStream.cancel(null);
            terminationStream = null;
        }
        if (exportNotifications != null) {
            exportNotifications.cancel();
            exportNotifications = null;
        }

        newSessionReconnect.disconnected();
        if (scheduledAuthUpdate != null) {
            DomGlobal.clearTimeout(scheduledAuthUpdate);
            scheduledAuthUpdate = null;
        }

        // Allow this to be disabled in case races are possible that break this.
        // Flag is temporary, as long as we're sure there are no ill effects from the feature.
        ConfigValue disableCloseOnDisconnect = getServerConfigValue("web.disableCloseSessionOnDisconnect");
        if (disableCloseOnDisconnect == null || !disableCloseOnDisconnect.hasStringValue()) {
            sessionServiceClient.closeSession(HandshakeRequest.getDefaultInstance(), Callbacks.ignore());
        }
        info.logout();
    }

    public Promise<JsVariableDefinition> getVariableDefinition(String name, String type) {
        LazyPromise<JsVariableDefinition> promise = new LazyPromise<>();

        final class Listener implements Consumer<JsVariableChanges> {
            final JsRunnable subscription;

            Listener() {
                subscription = subscribeToFieldUpdates(this::accept);
            }

            @Override
            public void accept(JsVariableChanges changes) {
                JsVariableDefinition foundField = changes.getCreated()
                        .find((field, p1, p2) -> field.getTitle().equals(name)
                                && field.getType().equalsIgnoreCase(type));

                if (foundField == null) {
                    foundField = changes.getUpdated().find((field, p1, p2) -> field.getTitle().equals(name)
                            && field.getType().equalsIgnoreCase(type));
                }

                if (foundField != null) {
                    subscription.run();
                    promise.succeed(foundField);
                }
            }
        }

        Listener listener = new Listener();

        return promise
                .timeout(10_000)
                .asPromise()
                .then(Promise::resolve, fail -> {
                    listener.subscription.run();
                    // noinspection unchecked, rawtypes
                    return (Promise<JsVariableDefinition>) (Promise) Promise
                            .reject(fail);
                });
    }

    public Promise<JsTable> getTable(JsVariableDefinition varDef, @Nullable Boolean applyPreviewColumns) {
        if (applyPreviewColumns != null && applyPreviewColumns) {
            JsLog.warn("applyPreviewColumns is deprecated, and will be removed in a future release");
        }
        return whenServerReady("get a table").then(serve -> {
            JsLog.debug("innerGetTable", varDef.getTitle(), " started");
            return newState((c, cts) -> {
                JsLog.debug("performing fetch for ", varDef.getTitle(), " / ", cts,
                        " (" + cts.getHandle() + ")");
                // Only apply preview if specifically requested
                if (applyPreviewColumns != null && applyPreviewColumns) {
                    ApplyPreviewColumnsRequest req = ApplyPreviewColumnsRequest.newBuilder()
                            .setSourceId(Tickets.createTableRef(varDef))
                            .setResultId(cts.getHandle().makeTicket())
                            .build();
                    tableServiceClient.applyPreviewColumns(req, c);
                } else {
                    FetchTableRequest req = FetchTableRequest.newBuilder()
                            .setSourceId(Tickets.createTableRef(varDef))
                            .setResultId(cts.getHandle().makeTicket())
                            .build();
                    tableServiceClient.fetchTable(req, c);
                }
            }, "fetch table " + varDef.getTitle())
                    .refetch()
                    .then(cts -> {
                        JsLog.debug("innerGetTable", varDef.getTitle(), " succeeded ", cts);
                        JsTable table = new JsTable(this, cts);
                        return Promise.resolve(table);
                    });
        });
    }

    public Promise<?> getObject(JsVariableDefinition definition) {
        if (JsVariableType.TABLE.equalsIgnoreCase(definition.getType())) {
            return getTable(definition, null);
        } else if (JsVariableType.FIGURE.equalsIgnoreCase(definition.getType())) {
            return getFigure(definition);
        } else if (JsVariableType.PANDAS.equalsIgnoreCase(definition.getType())) {
            return getWidget(definition)
                    .then(JsWidget::refetch)
                    .then(widget -> {
                        widget.close();
                        return widget.getExportedObjects()[0].fetch();
                    });
        } else if (JsVariableType.PARTITIONEDTABLE.equalsIgnoreCase(definition.getType())) {
            return getPartitionedTable(definition);
        } else if (JsVariableType.HIERARCHICALTABLE.equalsIgnoreCase(definition.getType())) {
            return getHierarchicalTable(definition);
        } else {
            warnLegacyTicketTypes(definition.getType());
            return getWidget(definition).then(JsWidget::refetch);
        }
    }

    public Promise<?> getJsObject(JsPropertyMap<Object> definitionObject) {
        if (definitionObject instanceof JsVariableDefinition) {
            return getObject((JsVariableDefinition) definitionObject);
        }

        if (!definitionObject.has("type")) {
            throw new IllegalArgumentException("no type field; could not getObject");
        }
        String type = definitionObject.getAsAny("type").asString();

        boolean hasName = definitionObject.has("name");
        boolean hasId = definitionObject.has("id");
        if (hasName && hasId) {
            throw new IllegalArgumentException("has both name and id field; could not getObject");
        } else if (hasName) {
            String name = definitionObject.getAsAny("name").asString();
            return getVariableDefinition(name, type)
                    .then(this::getObject);
        } else if (hasId) {
            String id = definitionObject.getAsAny("id").asString();
            return getObject(new JsVariableDefinition(type, null, id, null));
        } else {
            throw new IllegalArgumentException("no name/id field; could not construct getObject");
        }
    }

    public Promise<?> getObject(TypedTicket typedTicket) {
        if (JsVariableType.TABLE.equalsIgnoreCase(typedTicket.getType())) {
            throw new IllegalArgumentException("wrong way to get a table from a ticket");
        } else if (JsVariableType.FIGURE.equalsIgnoreCase(typedTicket.getType())) {
            return new JsFigure(this, () -> {
                JsWidget widget = new JsWidget(this, typedTicket);
                return widget.refetch().then(ignore -> Promise.resolve(makeFigureFetchResponse(widget)));
            }).refetch();
        } else if (JsVariableType.PANDAS.equalsIgnoreCase(typedTicket.getType())) {
            return getWidget(typedTicket)
                    .then(JsWidget::refetch)
                    .then(widget -> {
                        widget.close();
                        return widget.getExportedObjects()[0].fetch();
                    });
        } else if (JsVariableType.PARTITIONEDTABLE.equalsIgnoreCase(typedTicket.getType())) {
            return new JsPartitionedTable(this, new JsWidget(this, typedTicket)).refetch();
        } else if (JsVariableType.HIERARCHICALTABLE.equalsIgnoreCase(typedTicket.getType())) {
            return new JsWidget(this, typedTicket).refetch().then(w -> Promise.resolve(new JsTreeTable(this, w)));
        } else {
            warnLegacyTicketTypes(typedTicket.getType());
            return getWidget(typedTicket).then(JsWidget::refetch);
        }
    }

    private static void warnLegacyTicketTypes(String ticketType) {
        if (JsVariableType.TABLEMAP.equalsIgnoreCase(ticketType)) {
            JsLog.warn(
                    "TableMap is now known as PartitionedTable, fetching as a plain widget. To fetch as a PartitionedTable use that as the type.");
        }
        if (JsVariableType.TREETABLE.equalsIgnoreCase(ticketType)) {
            JsLog.warn(
                    "TreeTable is now HierarchicalTable, fetching as a plain widget. To fetch as a HierarchicalTable use that as this type.");
        }
    }

    public Promise<SharedExportBytesUnion> shareObject(ServerObject object, SharedExportBytesUnion sharedTicketBytes) {
        if (object.getConnection() != this) {
            return Promise.reject("Cannot share an object that comes from another server instance");
        }
        Ticket ticket = sharedTicketFromStringOrBytes(sharedTicketBytes);
        PublishRequest request = PublishRequest.newBuilder()
                .setSourceId(object.typedTicket().getTicket())
                .setResultId(ticket).build();

        return Callbacks.<PublishResponse>grpcUnaryPromise(c -> {
            sessionServiceClient().publishFromTicket(request, c);
        }).then(ignore -> Promise.resolve(sharedTicketBytes));
    }

    private Ticket sharedTicketFromStringOrBytes(SharedExportBytesUnion sharedTicketBytes) {
        final TypedArray.SetArrayUnionType array;
        if (sharedTicketBytes.isString()) {
            byte[] arr = sharedTicketBytes.asString().getBytes(StandardCharsets.UTF_8);
            array = TypedArray.SetArrayUnionType.of(arr);
        } else {
            Uint8Array bytes = sharedTicketBytes.asUint8Array();
            array = TypedArray.SetArrayUnionType.of(bytes);
        }
        return tickets.sharedTicket(array);
    }

    public Promise<?> getSharedObject(SharedExportBytesUnion sharedExportBytes, String type) {
        if (type.equalsIgnoreCase(JsVariableType.TABLE)) {
            return newState((callback, newState) -> {
                Ticket ticket = newState.getHandle().makeTicket();

                ExportRequest request = ExportRequest.newBuilder()
                        .setSourceId(sharedTicketFromStringOrBytes(sharedExportBytes))
                        .setResultId(ticket)
                        .build();

                Callbacks.<ExportResponse>grpcUnaryPromise(c -> {
                    sessionServiceClient().exportFromTicket(request, c);
                }).then(ignore -> {
                    tableServiceClient().getExportedTableCreationResponse(ticket, callback);
                    return null;
                }, err -> {
                    if (err instanceof Throwable t) {
                        callback.onError(t);
                    } else {
                        callback.onError(new RuntimeException(String.valueOf(err)));
                    }
                    return null;
                });

            }, "getSharedObject")
                    .refetch()
                    .then(state -> Promise.resolve(new JsTable(this, state)));
        }

        TypedTicket result = TypedTicket.newBuilder()
                .setTicket(getTickets().newExportTicket())
                .setType(type)
                .build();


        ExportRequest request = ExportRequest.newBuilder()
                .setResultId(result.getTicket())
                .setSourceId(sharedTicketFromStringOrBytes(sharedExportBytes))
                .build();


        return Callbacks.<ExportResponse>grpcUnaryPromise(c -> {
            sessionServiceClient().exportFromTicket(request, c);
        }).then(ignore -> getObject(result));
    }

    @SuppressWarnings("ConstantConditions")
    public JsRunnable subscribeToFieldUpdates(JsConsumer<JsVariableChanges> callback) {
        fieldUpdatesCallback.add(callback);
        if (fieldUpdatesCallback.size == 1) {
            fieldsChangeUpdateStream =
                    ResponseStreamWrapper.of(observer -> applicationServiceClient
                            .listFields(ListFieldsRequest.getDefaultInstance(), observer));
            fieldsChangeUpdateStream.onData(data -> {
                final JsVariableDefinition[] created = new JsVariableDefinition[0];
                final JsVariableDefinition[] updated = new JsVariableDefinition[0];
                final JsVariableDefinition[] removed = new JsVariableDefinition[0];

                List<FieldInfo> removedFI = data.getRemovedList();
                for (int i = 0; i < removedFI.size(); ++i) {
                    String removedId = BaseEncoding.base64()
                            .encode(removedFI.get(i).getTypedTicket().getTicket().getTicket().toByteArray());
                    JsVariableDefinition result = knownFields.get(removedId);
                    removed[removed.length] = result;
                    knownFields.remove(removedId);
                }
                List<FieldInfo> createdFI = data.getCreatedList();
                for (int i = 0; i < createdFI.size(); ++i) {
                    JsVariableDefinition result = new JsVariableDefinition(createdFI.get(i));
                    created[created.length] = result;
                    knownFields.put(result.getId(), result);
                }
                List<FieldInfo> updatedFI = data.getUpdatedList();
                for (int i = 0; i < updatedFI.size(); ++i) {
                    JsVariableDefinition result = new JsVariableDefinition(updatedFI.get(i));
                    updated[updated.length] = result;
                    knownFields.put(result.getId(), result);
                }

                // Ensure that if a new subscription is in line to receive its initial update, we need to defer
                // the updates until after it receives its initial state.
                LazyPromise
                        .runLater(() -> notifyFieldsChangeListeners(new JsVariableChanges(created, updated, removed)));
            });
            fieldsChangeUpdateStream.onEnd(this::checkStatus);
        } else {
            final JsVariableDefinition[] empty = new JsVariableDefinition[0];
            final JsVariableChanges update = new JsVariableChanges(knownFields.values().toArray(empty), empty, empty);
            LazyPromise.runLater(() -> {
                callback.apply(update);
            });
        }
        return () -> {
            fieldUpdatesCallback.delete(callback);
            if (fieldUpdatesCallback.size == 0) {
                knownFields.clear();
                if (fieldsChangeUpdateStream != null) {
                    fieldsChangeUpdateStream.cancel();
                    fieldsChangeUpdateStream = null;
                }
            }
        };
    }

    private void notifyFieldsChangeListeners(JsVariableChanges update) {
        for (JsConsumer<JsVariableChanges> callback : JsItr.iterate(fieldUpdatesCallback.keys())) {
            callback.apply(update);
        }
    }

    public Promise<Object> whenServerReady(String operationName) {
        switch (state) {
            case Disconnected:
                throw new IllegalStateException("Can't " + operationName + " while connection is closed");
            case Failed:
                state = State.Reconnecting;
                newSessionReconnect.initialConnection();
                // deliberate fall-through
            case Connecting:
            case Reconnecting:
                // Create a new promise around a callback, add that to the list of callbacks to complete when
                // connection is complete
                return Callbacks.<Void, String>promise(info, c -> onOpen.add(c)).then(ignore -> Promise.resolve(this));
            case Connected:
                // Already connected, continue
                return Promise.resolve(this);
            default:
                // not possible, means null state
                return Promise.reject("Can't " + operationName + " while connection is in state " + state);
        }
    }

    private TicketAndPromise<?> exportScopeTicket(JsVariableDefinition varDef) {
        Ticket ticket = getTickets().newExportTicket();
        return new TicketAndPromise<>(ticket, whenServerReady("exportScopeTicket").then(server -> {
            ExportRequest req = ExportRequest.newBuilder()
                    .setSourceId(createTypedTicket(varDef).getTicket())
                    .setResultId(ticket)
                    .build();
            return Callbacks.<ExportResponse>grpcUnaryPromise(
                    c -> sessionServiceClient().exportFromTicket(req, c));
        }), this);
    }

    public Promise<JsPartitionedTable> getPartitionedTable(JsVariableDefinition varDef) {
        return whenServerReady("get a partitioned table")
                .then(server -> getWidget(varDef))
                .then(widget -> new JsPartitionedTable(this, widget).refetch());
    }

    public Promise<JsTreeTable> getHierarchicalTable(JsVariableDefinition varDef) {
        return getWidget(varDef).then(JsWidget::refetch).then(w -> Promise.resolve(new JsTreeTable(this, w)));
    }

    public Promise<JsFigure> getFigure(JsVariableDefinition varDef) {
        if (!varDef.getType().equalsIgnoreCase("Figure")) {
            throw new IllegalArgumentException("Can't load as a figure: " + varDef.getType());
        }
        return whenServerReady("get a figure")
                .then(server -> new JsFigure(this,
                        () -> {
                            return getWidget(varDef).then(JsWidget::refetch).then(widget -> {
                                FetchObjectResponse result = makeFigureFetchResponse(widget);
                                widget.close();
                                return Promise.resolve(result);
                            });
                        }).refetch());
    }

    private static FetchObjectResponse makeFigureFetchResponse(JsWidget widget) {
        return FetchObjectResponse.newBuilder()
                .setData(widget.getData())
                .setType(widget.getType())
                .addAllTypedExportIds(Arrays.stream(widget.getExportedObjects())
                        .map(JsWidgetExportedObject::typedTicket).collect(Collectors.toList()))
                .build();
    }

    private TypedTicket createTypedTicket(JsVariableDefinition varDef) {
        return TypedTicket.newBuilder()
                .setTicket(Tickets.createTicket(varDef))
                .setType(varDef.getType())
                .build();
    }

    public Promise<JsWidget> getWidget(JsVariableDefinition varDef) {
        return exportScopeTicket(varDef)
                .race(ticket -> {
                    TypedTicket typedTicket = TypedTicket.newBuilder()
                            .setType(varDef.getType())
                            .setTicket(ticket)
                            .build();
                    return getWidget(typedTicket);
                }).promise();
    }

    public Promise<JsWidget> getWidget(TypedTicket typedTicket) {
        return whenServerReady("get a widget")
                .then(response -> Promise.resolve(new JsWidget(this, typedTicket)));
    }

    public void registerSimpleReconnectable(HasLifecycle figure) {
        this.simpleReconnectableInstances.add(figure);
    }

    public void unregisterSimpleReconnectable(HasLifecycle figure) {
        this.simpleReconnectableInstances.delete(figure);
    }


    public TableServiceGrpc.TableServiceStub tableServiceClient() {
        return tableServiceClient;
    }

    public ConsoleServiceGrpc.ConsoleServiceStub consoleServiceClient() {
        return consoleServiceClient;
    }

    public SessionServiceGrpc.SessionServiceStub sessionServiceClient() {
        return sessionServiceClient;
    }

    public FlightServiceGrpc.FlightServiceStub flightServiceClient() {
        return flightServiceClient;
    }

    public BrowserFlightServiceGrpc.BrowserFlightServiceStub browserFlightServiceClient() {
        return browserFlightServiceClient;
    }

    public InputTableServiceGrpc.InputTableServiceStub inputTableServiceClient() {
        return inputTableServiceClient;
    }

    public ObjectServiceGrpc.ObjectServiceStub objectServiceClient() {
        return objectServiceClient;
    }

    public PartitionedTableServiceGrpc.PartitionedTableServiceStub partitionedTableServiceClient() {
        return partitionedTableServiceClient;
    }

    public StorageServiceGrpc.StorageServiceStub storageServiceClient() {
        return storageServiceClient;
    }

    public ConfigServiceGrpc.ConfigServiceStub configServiceClient() {
        return configServiceClient;
    }

    public HierarchicalTableServiceGrpc.HierarchicalTableServiceStub hierarchicalTableServiceClient() {
        return hierarchicalTableServiceClient;
    }

    public <ReqT, RespT> BiDiStream.Factory<ReqT, RespT> streamFactory() {
        return new BiDiStream.Factory<>(info.supportsClientStreaming(), tickets::newTicketInt);
    }

    public Promise<JsTable> newTable(String[] columnNames, String[] types, Object[][] data, String userTimeZone) {
        // Store the ref to the data using an array we can clear out, so the data is garbage collected later
        // This means the table can only be created once, but that's probably what we want in this case anyway
        final Object[][][] dataRef = new Object[][][] {data};
        return newState((c, cts) -> {
            final Object[][] d = dataRef[0];
            if (d == null) {
                c.onError(new IllegalStateException("Data already released, cannot re-create table"));
                return;
            }
            dataRef[0] = null;

            // make a schema that we can embed in the first DoPut message
            FlatBufferBuilder schema = new FlatBufferBuilder(1024);

            // while we're examining columns, build the copiers for data
            List<JsDataHandler> columns = new ArrayList<>();

            int[] fields = new int[columnNames.length];
            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];
                String columnType = types[i];

                JsDataHandler writer = JsDataHandler.getHandler(columnType);
                columns.add(writer);

                int nameOffset = schema.createString(columnName);
                int typeOffset = writer.writeType(schema);
                int metadataOffset = Field.createCustomMetadataVector(schema, new int[] {
                        KeyValue.createKeyValue(schema, schema.createString("deephaven:type"),
                                schema.createString(writer.deephavenType()))
                });

                Field.startField(schema);
                Field.addName(schema, nameOffset);
                Field.addNullable(schema, true);

                Field.addTypeType(schema, writer.typeType());
                Field.addType(schema, typeOffset);
                Field.addCustomMetadata(schema, metadataOffset);

                fields[i] = Field.endField(schema);
            }
            int fieldsOffset = Schema.createFieldsVector(schema, fields);

            Schema.startSchema(schema);
            Schema.addFields(schema, fieldsOffset);

            // wrap in a message and send as the first payload
            Flight.FlightData.Builder schemaMessage = Flight.FlightData.newBuilder();
            ByteBuffer schemaMessagePayload =
                    createMessage(schema, MessageHeader.Schema, Schema.endSchema(schema), 0, 0);
            schemaMessage.setDataHeader(ByteStringAccess.wrap(schemaMessagePayload.duplicate()));

            schemaMessage.setAppMetadata(ByteStringAccess.wrap(WebBarrageUtils.emptyMessage()));
            schemaMessage.setFlightDescriptor(cts.getHandle().makeFlightDescriptor());

            // we wait for any errors in this response to pass to the caller, but success is determined by the eventual
            // table's creation, which can race this
            BiDiStream<Flight.FlightData, Flight.PutResult> stream =
                    this.<Flight.FlightData, Flight.PutResult>streamFactory().<BrowserFlight.BrowserNextResponse>create(
                            callback -> flightServiceClient.doPut(callback),
                            (first, callback) -> browserFlightServiceClient.openDoPut(first, callback),
                            (next, callback) -> browserFlightServiceClient.nextDoPut(next, callback));
            stream.send(schemaMessage.build());

            stream.onEnd(status -> {
                if (status.isOk()) {
                    ByteBuffer schemaPlusHeader = ByteBuffer.allocate(schemaMessagePayload.remaining() + 8);
                    schemaPlusHeader.order(ByteOrder.LITTLE_ENDIAN);
                    schemaPlusHeader.putInt(-1);
                    schemaPlusHeader.putInt(schemaMessagePayload.remaining());
                    schemaPlusHeader.put(schemaMessagePayload);
                    schemaPlusHeader.flip();
                    ExportedTableCreationResponse syntheticResponse = ExportedTableCreationResponse.newBuilder()
                            .setSchemaHeader(ByteStringAccess.wrap(schemaPlusHeader))
                            .setSize(data[0].length)
                            .setIsStatic(true)
                            .setSuccess(true)
                            .setResultId(cts.getHandle().makeTableReference())
                            .build();

                    c.onNext(syntheticResponse);
                    c.onCompleted();
                } else {
                    c.onError(status.asRuntimeException(TrailersCapturingInterceptor.getTrailersFromContext()));
                }
            });
            Flight.FlightData.Builder bodyMessage = Flight.FlightData.newBuilder();
            bodyMessage.setAppMetadata(ByteStringAccess.wrap(WebBarrageUtils.emptyMessage()));

            FlatBufferBuilder bodyData = new FlatBufferBuilder(1024);

            // iterate each column, building buffers and fieldnodes, as well as building the actual payload
            List<Uint8Array> buffers = new ArrayList<>();
            List<JsDataHandler.Node> nodes = new ArrayList<>();
            JsDataHandler.ParseContext context = new JsDataHandler.ParseContext();
            if (userTimeZone != null) {
                context.timeZone = JsTimeZone.getTimeZone(userTimeZone);
            }
            for (int i = 0; i < data.length; i++) {
                columns.get(i).write(data[i], context, nodes::add, buffers::add);
            }

            // write down the buffers for the RecordBatch
            RecordBatch.startBuffersVector(bodyData, buffers.size());
            int length = 0;// record the size, we need to be sure all buffers are padded to full width
            for (Uint8Array arr : buffers) {
                assert arr.byteLength % 8 == 0;
                length += arr.byteLength;
            }
            int cumulativeOffset = length;
            for (int i = buffers.size() - 1; i >= 0; i--) {
                Uint8Array buffer = buffers.get(i);
                cumulativeOffset -= buffer.byteLength;
                Buffer.createBuffer(bodyData, cumulativeOffset, buffer.byteLength);
            }
            assert cumulativeOffset == 0;
            int buffersOffset = bodyData.endVector();

            RecordBatch.startNodesVector(bodyData, nodes.size());
            for (int i = nodes.size() - 1; i >= 0; i--) {
                JsDataHandler.Node node = nodes.get(i);
                FieldNode.createFieldNode(bodyData, node.length(), node.nullCount());
            }
            int nodesOffset = bodyData.endVector();

            RecordBatch.startRecordBatch(bodyData);

            RecordBatch.addBuffers(bodyData, buffersOffset);
            RecordBatch.addNodes(bodyData, nodesOffset);
            RecordBatch.addLength(bodyData, data[0].length);

            int recordBatchOffset = RecordBatch.endRecordBatch(bodyData);
            bodyMessage.setDataHeader(ByteStringAccess
                    .wrap(createMessage(bodyData, MessageHeader.RecordBatch, recordBatchOffset, length, 0)));
            bodyMessage.setDataBody(ByteStringAccess.wrap(padAndConcat(buffers, length)));

            stream.send(bodyMessage.build());
            stream.end();
        }, "creating new table")
                .refetch()
                .then(cts -> Promise.resolve(new JsTable(this, cts)));
    }

    private ByteBuffer padAndConcat(List<Uint8Array> buffers, int length) {
        Uint8Array all = new Uint8Array(buffers.stream().mapToInt(b -> b.byteLength).sum());
        int currentPosition = 0;
        for (int i = 0; i < buffers.size(); i++) {
            Uint8Array buffer = buffers.get(i);
            all.set(buffer, currentPosition);
            currentPosition += buffer.byteLength;
        }
        assert length == currentPosition;
        return TypedArrayHelper.wrap(all);
    }

    private static ByteBuffer createMessage(FlatBufferBuilder payload, byte messageHeaderType, int messageHeaderOffset,
            int bodyLength, int customMetadataOffset) {
        payload.finish(Message.createMessage(payload, MetadataVersion.V5, messageHeaderType, messageHeaderOffset,
                bodyLength, customMetadataOffset));
        return payload.dataBuffer();
    }

    public Promise<JsTable> mergeTables(JsTable[] tables, HasEventHandling failHandler) {
        return newState((c, cts) -> {
            final List<TableReference> tableHandles = new ArrayList<>();
            for (int i = 0; i < tables.length; i++) {
                final JsTable table = tables[i];
                if (!table.getConnection().equals(this)) {
                    throw new IllegalStateException("Table in merge is not on the worker for this connection");
                }
                tableHandles.add(TableReference.newBuilder()
                        .setTicket(tables[i].getHandle().makeTicket())
                        .build());
            }
            JsLog.debug("Merging tables: ", LazyString.of(cts.getHandle()), " for ", cts.getHandle().isResolved(),
                    cts.getResolution());
            MergeTablesRequest requestMessage = MergeTablesRequest.newBuilder()
                    .setResultId(cts.getHandle().makeTicket())
                    .addAllSourceIds(tableHandles)
                    .build();
            tableServiceClient.mergeTables(requestMessage, c);
        }, "merging tables")
                .refetch()
                .then(cts -> Promise.resolve(new JsTable(this, cts)));
    }

    /**
     * Provides a reference to any table that happens to be created using that handle
     */
    private JsTable getFirstByHandle(TableTicket handle) {
        final Optional<ClientTableState> table = cache.get(handle);
        if (table.isPresent()) {
            final ClientTableState state = table.get();
            if (!state.getBoundTables().isEmpty()) {
                return state.getBoundTables().first();
            }
        }
        return null;
    }

    public RequestBatcher getBatcher(JsTable table) {
        // LATER: consider a global client.batch(()=>{}) method which causes all table statements to be batched
        // together.
        // We will build this architecture to support this, without wiring it up just yet
        RequestBatcher batcher = batchers.get(table);
        if (batcher == null || batcher.isSent()) {
            final RequestBatcher myBatcher = new RequestBatcher(table, this);
            batchers.set(table, myBatcher);
            myBatcher.onSend(r -> {
                // clear out our map references if we're the last batcher to finish running for the given table.
                if (batchers.get(table) == myBatcher) {
                    batchers.delete(table);
                }
            });
            return myBatcher;
        }
        return batcher;
    }

    public ClientTableState newStateFromUnsolicitedTable(ExportedTableCreationResponse unsolicitedTable,
            String fetchSummary) {
        TableTicket tableTicket = new TableTicket(unsolicitedTable.getResultId().getTicket());
        JsTableFetch failFetch = (callback, newState) -> {
            throw new IllegalStateException(
                    "Cannot reconnect, must recreate the unsolicited table on the server: " + fetchSummary);
        };
        return cache.create(tableTicket, handle -> {
            ClientTableState cts = new ClientTableState(this, handle, failFetch, fetchSummary);
            cts.applyTableCreationResponse(unsolicitedTable);
            return cts;
        });
    }

    public ClientTableState newState(JsTableFetch fetcher, String fetchSummary) {
        return cache.create(tickets.newTableTicket(),
                handle -> new ClientTableState(this, handle, fetcher, fetchSummary));
    }

    public ClientTableState newState(ClientTableState from, TableConfig to) {
        return newState(from, to, tickets.newTableTicket());
    }

    public ClientTableState newState(ClientTableState from, TableConfig to, TableTicket handle) {
        return cache.create(handle, h -> from.newState(h, to));
    }

    public StateCache getCache() {
        return cache;
    }

    /**
     * Schedules a deferred command to check the given state for active tables.
     */
    public void scheduleCheck(ClientTableState state) {
        if (flushable.isEmpty()) {
            LazyPromise.runLater(this::flush);
        }
        flushable.add(state);
    }

    public void releaseHandle(TableTicket handle) {
        releaseTicket(handle.makeTicket());
    }

    /**
     * Releases the ticket, indicating no client using this session will reference it any more.
     *
     * @param ticket the ticket to release
     */
    public void releaseTicket(Ticket ticket) {
        // TODO verify cleanup core#223
        ReleaseRequest releaseRequest = ReleaseRequest.newBuilder()
                .setId(ticket)
                .build();
        sessionServiceClient.release(releaseRequest, Callbacks.ignore());
    }

    private void flush() {
        ArrayList<ClientTableState> statesToFlush = new ArrayList<>(flushable);
        flushable.clear();

        for (ClientTableState state : statesToFlush) {
            if (state.isEmpty()) {
                // completely empty; perform release
                final ClientTableState.ResolutionState previousState = state.getResolution();
                state.setResolution(ClientTableState.ResolutionState.RELEASED);
                if (previousState != ClientTableState.ResolutionState.RELEASED) {
                    cache.release(state);

                    JsLog.debug("Releasing state", state, LazyString.of(state.getHandle()));
                    // don't send a release message to the server if the table isn't really there
                    if (state.getHandle().isConnected()) {
                        releaseHandle(state.getHandle());
                    }
                }
            }
        }
    }

    public TableReviver getReviver() {
        return reviver;
    }

    public boolean isUsable() {
        switch (state) {
            case Connected:
            case Connecting:
                // Ignore Reconnecting, this is here for tree tables to decide whether to poll or not;
                // if we really are disconnected, tree tables should wait until we are reconnected to poll again.
                return true;

        }
        return false;
    }

    public Tickets getTickets() {
        return tickets;
    }

    public ConfigValue getServerConfigValue(String key) {
        return constants.getConfigValuesMap().get(key);
    }

    public void onOpen(BiConsumer<Void, String> callback) {
        switch (state) {
            case Connected:
                LazyPromise.runLater(() -> callback.accept(null, null));
                break;
            case Disconnected:
                throw new IllegalStateException("Can't add onOpen callback when connection is closed");
            case Failed:
                state = State.Reconnecting;
                newSessionReconnect.initialConnection();
                // intentional fall-through
            default:
                onOpen.add(Callbacks.of(callback));
        }
    }

    public JsRunnable subscribeToLogs(JsConsumer<LogItem> callback) {
        boolean mustSub = logCallbacks.size == 0;
        logCallbacks.add(callback);
        if (mustSub) {
            logCallbacks.add(recordLog);
            maybeRestartLogStream();
        } else {
            pastLogs.forEach(callback::apply);
        }
        return () -> {
            logCallbacks.delete(callback);
            if (logCallbacks.size == 1) {
                logCallbacks.delete(recordLog);
                assert logCallbacks.size == 0;
                pastLogs.clear();
                if (logStream != null) {
                    logStream.cancel();
                    logStream = null;
                }
            }
        };
    }

    @JsMethod
    public String dump(@JsOptional @JsNullable String graphName) {
        if (graphName == null) {
            graphName = "states";
        }
        StringBuilder graph = new StringBuilder("digraph " + graphName + " {\n");

        // write dummy null state for later use - represents this worker
        graph.append("  null [label=\"fetch from server\" shape=plaintext]\n");

        // collect the parent/child relationships
        Map<ClientTableState, List<ClientTableState>> statesAndParents = cache.getAllStates().stream()
                .collect(Collectors.groupingBy(ClientTableState::getPrevious));

        // append all handles, operations, and how they were performed
        appendStatesToDump(null, statesAndParents, graph);

        // insert all tables and the state they are currently using
        cache.getAllStates().forEach(cts -> {
            cts.getActiveBindings().forEach(binding -> {
                int tableId = binding.getTable().getSubscriptionId();
                graph.append("  table").append(tableId).append("[shape=box];\n");
                graph.append("  table").append(tableId).append(" -> handle")
                        .append(binding.getTable().getHandle().hashCode()).append("[color=blue];\n");
                if (binding.getRollback() != null) {
                    graph.append("  handle").append(binding.getState().getHandle().hashCode()).append(" -> handle")
                            .append(binding.getRollback().getState().getHandle().hashCode())
                            .append(" [style=dotted, label=rollback];\n");
                }
            });
        });

        return graph.append("}").toString();
    }

    private void appendStatesToDump(ClientTableState parent,
            Map<ClientTableState, List<ClientTableState>> statesAndParents, StringBuilder graph) {
        List<ClientTableState> childStates = statesAndParents.get(parent);
        if (childStates == null) {
            return;
        }
        for (ClientTableState clientTableState : childStates) {
            if (parent == null) {
                graph.append("  null");
            } else {
                graph.append("  handle").append(parent.getHandle().hashCode());
            }
            graph.append(" -> handle").append(clientTableState.getHandle().hashCode()).append("[label=\"")
                    .append(clientTableState.getFetchSummary().replaceAll("\"", "")).append("\"];\n");
            appendStatesToDump(clientTableState, statesAndParents, graph);
        }
    }

    public Promise<JsTable> emptyTable(double size) {
        return whenServerReady("create emptyTable").then(server -> newState((c, cts) -> {
            EmptyTableRequest emptyTableRequest = EmptyTableRequest.newBuilder()
                    .setResultId(cts.getHandle().makeTicket())
                    .setSize((long) size)
                    .build();
            tableServiceClient.emptyTable(emptyTableRequest, c);
        }, "emptyTable(" + size + ")").refetch())
                .then(cts -> Promise.resolve(new JsTable(this, cts)));
    }

    public Promise<JsTable> timeTable(double periodNanos, DateWrapper startTime) {
        final long startTimeNanos = startTime == null ? -1 : startTime.getWrapped();
        return whenServerReady("create timetable").then(server -> newState((c, cts) -> {
            TimeTableRequest timeTableRequest = TimeTableRequest.newBuilder()
                    .setResultId(cts.getHandle().makeTicket())
                    .setPeriodNanos((long) periodNanos)
                    .setStartTimeNanos(startTimeNanos)
                    .build();
            tableServiceClient.timeTable(timeTableRequest, c);
        }, "create timetable(" + periodNanos + ", " + startTime + ")").refetch())
                .then(cts -> Promise.resolve(new JsTable(this, cts)));
    }
}
