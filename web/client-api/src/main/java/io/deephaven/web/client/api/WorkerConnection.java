//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.flatbuffers.FlatBufferBuilder;
import com.vertispan.tsdefs.annotations.TsIgnore;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.JsSet;
import elemental2.core.JsWeakMap;
import elemental2.core.Uint8Array;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb_service.BrowserFlightServiceClient;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb_service.FlightServiceClient;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Code;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.UnaryOutput;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.FieldInfo;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.FieldsChangeUpdate;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.ListFieldsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb_service.ApplicationServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigValue;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb_service.ConfigService;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb_service.ConfigServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.LogSubscriptionData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.LogSubscriptionRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb_service.ConsoleServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb_service.HierarchicalTableServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb_service.InputTableServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb.FetchObjectResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.object_pb_service.ObjectServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.partitionedtable_pb_service.PartitionedTableServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.ExportRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.ExportResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.ReleaseRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.TerminationNotificationRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb_service.SessionServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb_service.UnaryResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.storage_pb_service.StorageServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ApplyPreviewColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.EmptyTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableUpdateMessage;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableUpdatesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FetchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.MergeTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TableReference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TimeTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb_service.TableServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import io.deephaven.web.client.api.barrage.WebBarrageUtils;
import io.deephaven.web.client.api.barrage.def.InitialTableDefinition;
import io.deephaven.web.client.api.barrage.stream.BiDiStream;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.batch.RequestBatcher;
import io.deephaven.web.client.api.batch.TableConfig;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.api.console.JsVariableType;
import io.deephaven.web.client.api.grpc.UnaryWithHeaders;
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
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.client.state.HasTableBinding;
import io.deephaven.web.client.state.TableReviver;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.base.Js;
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

import javax.annotation.Nullable;
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

import static io.deephaven.web.client.api.CoreClient.EVENT_REFRESH_TOKEN_UPDATED;
import static io.deephaven.web.client.api.barrage.WebGrpcUtils.CLIENT_OPTIONS;

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
    private static final String FLIGHT_AUTH_HEADER_NAME = "authorization";

    // All calls to the server should share this metadata instance, or copy from it if they need something custom
    private final BrowserHeaders metadata = new BrowserHeaders();

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
    private final ClientConfiguration config;
    private final ReconnectState newSessionReconnect;
    private final TableReviver reviver;
    // un-finished fetch operations - these can fail on connection issues, won't be attempted again
    private List<Callback<Void, String>> onOpen = new ArrayList<>();

    private State state;
    private SessionServiceClient sessionServiceClient;
    private TableServiceClient tableServiceClient;
    private ConsoleServiceClient consoleServiceClient;
    private ApplicationServiceClient applicationServiceClient;
    private FlightServiceClient flightServiceClient;
    private BrowserFlightServiceClient browserFlightServiceClient;
    private InputTableServiceClient inputTableServiceClient;
    private ObjectServiceClient objectServiceClient;
    private PartitionedTableServiceClient partitionedTableServiceClient;
    private StorageServiceClient storageServiceClient;
    private ConfigServiceClient configServiceClient;
    private HierarchicalTableServiceClient hierarchicalTableServiceClient;

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

    private UnaryResponse terminationStream;

    private final JsSet<JsConsumer<JsVariableChanges>> fieldUpdatesCallback = new JsSet<>();
    private Map<String, JsVariableDefinition> knownFields = new HashMap<>();
    private ResponseStreamWrapper<FieldsChangeUpdate> fieldsChangeUpdateStream;

    private ConfigurationConstantsResponse constants;

    public WorkerConnection(QueryConnectable<?> info) {
        this.info = info;
        this.config = new ClientConfiguration();
        state = State.Connecting;
        this.reviver = new TableReviver(this);
        sessionServiceClient = new SessionServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        tableServiceClient = new TableServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        consoleServiceClient = new ConsoleServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        flightServiceClient = new FlightServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        applicationServiceClient =
                new ApplicationServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        browserFlightServiceClient =
                new BrowserFlightServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        inputTableServiceClient =
                new InputTableServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        objectServiceClient = new ObjectServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        partitionedTableServiceClient =
                new PartitionedTableServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        storageServiceClient = new StorageServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        configServiceClient = new ConfigServiceClient(info.getServerUrl(), CLIENT_OPTIONS);
        hierarchicalTableServiceClient =
                new HierarchicalTableServiceClient(info.getServerUrl(), CLIENT_OPTIONS);

        // builder.setConnectionErrorHandler(msg -> info.failureHandled(String.valueOf(msg)));

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
                .then(queryWorkerRunning -> {
                    if (metadata().has(FLIGHT_AUTH_HEADER_NAME)) {
                        return authUpdate().then(ignore -> Promise.resolve(Boolean.FALSE));
                    }
                    return Promise.all(
                            info.getConnectToken().then(authToken -> {
                                // set the proposed initial token and make the first call
                                metadata.set(FLIGHT_AUTH_HEADER_NAME,
                                        (authToken.getType() + " " + authToken.getValue()).trim());
                                return Promise.resolve(authToken);
                            }),
                            info.getConnectOptions().then(options -> {
                                // set other specified headers, if any
                                JsObject.keys(options.headers).forEach((key, index) -> {
                                    metadata.set(key, options.headers.get(key));
                                    return null;
                                });
                                return Promise.resolve(options);
                            })).then(ignore -> authUpdate()).then(ignore -> Promise.resolve(Boolean.TRUE));
                }).then(newSession -> {
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

                        reviver.revive(metadata, hasActiveSubs);

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

                        reviver.revive(metadata, hasActiveSubs);
                    }

                    info.connected();

                    // if any tables have been requested, make sure they start working now that we are connected
                    onOpen.forEach(c -> c.onSuccess(null));
                    onOpen.clear();

                    startExportNotificationsStream();

                    maybeRestartLogStream();

                    return Promise.resolve((Object) null);
                }, fail -> {
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


    private boolean checkStatus(ResponseStreamWrapper.ServiceError fail) {
        return checkStatus(ResponseStreamWrapper.Status.of(fail.getCode(), fail.getMessage(), fail.getMetadata()));
    }

    public boolean checkStatus(ResponseStreamWrapper.Status status) {
        if (state == State.Disconnected) {
            return false;
        }
        if (status.isOk()) {
            // success, ignore
            return true;
        } else if (status.getCode() == Code.Unauthenticated) {
            // fire deprecated event for now
            info.notifyConnectionError(status);

            // signal that the user needs to re-authenticate, make a new session
            // TODO (deephaven-core#3501) in theory we could make a new session for some auth types
            info.fireCriticalEvent(CoreClient.EVENT_RECONNECT_AUTH_FAILED);
        } else if (status.isTransportError()) {
            // fire deprecated event for now
            info.notifyConnectionError(status);

            // signal that there has been a connection failure of some kind and attempt to reconnect
            info.fireEvent(CoreClient.EVENT_DISCONNECT);

            // Try again after a backoff, this may happen several times
            connectionLost();
        } // others probably are meaningful to the caller
        return false;
    }

    private void maybeRestartLogStream() {
        if (logCallbacks.size == 0) {
            return;
        }
        if (logStream != null) {
            logStream.cancel();
        }
        LogSubscriptionRequest logSubscriptionRequest = new LogSubscriptionRequest();
        if (pastLogs.size() > 0) {
            // only ask for messages seen after the last message we recieved
            logSubscriptionRequest
                    .setLastSeenLogTimestamp(String.valueOf((long) pastLogs.get(pastLogs.size() - 1).getMicros()));
        }
        logStream = ResponseStreamWrapper
                .of(consoleServiceClient.subscribeToLogs(logSubscriptionRequest, metadata));
        logStream.onData(data -> {
            LogItem logItem = new LogItem();
            logItem.setLogLevel(data.getLogLevel());
            logItem.setMessage(data.getMessage());
            logItem.setMicros((double) java.lang.Long.parseLong(data.getMicros()));

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
                .of(tableServiceClient.exportedTableUpdates(new ExportedTableUpdatesRequest(), metadata()));
        exportNotifications.onData(update -> {
            if (update.getUpdateFailureMessage() != null && !update.getUpdateFailureMessage().isEmpty()) {
                cache.get(new TableTicket(update.getExportId().getTicket_asU8())).ifPresent(state1 -> {
                    state1.setResolution(ClientTableState.ResolutionState.FAILED, update.getUpdateFailureMessage());
                });
            } else {
                exportedTableUpdateMessage(new TableTicket(update.getExportId().getTicket_asU8()),
                        java.lang.Long.parseLong(update.getSize()));
            }
        });

        // any export notification error is bad news
        exportNotifications.onStatus(this::checkStatus);
    }

    /**
     * Manages auth token update and rotation. A typical grpc/grpc-web client would support something like an
     * interceptor to be able to tweak requests and responses slightly, but our client doesn't have an easy way to add
     * something like that. Instead, this client will continue to call FlightService/Handshake at the specified
     * interval, with empty payloads.
     *
     * @return a promise for when this auth is completed
     */
    private Promise<Void> authUpdate() {
        if (scheduledAuthUpdate != null) {
            DomGlobal.clearTimeout(scheduledAuthUpdate);
            scheduledAuthUpdate = null;
        }
        return UnaryWithHeaders.<ConfigurationConstantsRequest, ConfigurationConstantsResponse>call(
                this, ConfigService.GetConfigurationConstants, new ConfigurationConstantsRequest())
                .then(result -> {
                    BrowserHeaders headers = result.getHeaders();
                    // unchecked cast is required here due to "aliasing" in ts/webpack resulting in BrowserHeaders !=
                    // Metadata
                    JsArray<String> authorization =
                            Js.<BrowserHeaders>uncheckedCast(headers).get(FLIGHT_AUTH_HEADER_NAME);
                    if (authorization.length > 0) {
                        JsArray<String> existing = metadata().get(FLIGHT_AUTH_HEADER_NAME);
                        if (!existing.getAt(0).equals(authorization.getAt(0))) {
                            // use this new token
                            metadata().set(FLIGHT_AUTH_HEADER_NAME, authorization);
                            CustomEventInit<JsRefreshToken> init = CustomEventInit.create();
                            init.setDetail(new JsRefreshToken(authorization.getAt(0), sessionTimeoutMs));
                            info.fireEvent(EVENT_REFRESH_TOKEN_UPDATED, init);
                        }
                    }

                    // Read the timeout from the server, we'll refresh at less than that
                    constants = result.getMessage();
                    ConfigValue sessionDuration = constants.getConfigValuesMap().get("http.session.durationMs");
                    if (sessionDuration != null && sessionDuration.hasStringValue()) {
                        sessionTimeoutMs = Double.parseDouble(sessionDuration.getStringValue());
                    }

                    // schedule an update based on our currently configured delay
                    scheduledAuthUpdate = DomGlobal.setTimeout(ignore -> {
                        authUpdate();
                    }, sessionTimeoutMs / 2);

                    return Promise.resolve((Void) null);
                }).catch_(err -> {
                    UnaryOutput<?> result = (UnaryOutput<?>) err;
                    if (result.getStatus() == Code.Unauthenticated) {
                        // explicitly clear out any metadata for authentication, and signal that auth failed
                        metadata.delete(FLIGHT_AUTH_HEADER_NAME);

                        // Fire an event for the UI to attempt to re-auth
                        info.fireCriticalEvent(CoreClient.EVENT_RECONNECT_AUTH_FAILED);

                        // We return here rather than continue and call checkStatus()
                        return Promise.reject("Authentication failed, please reconnect");
                    }
                    checkStatus(ResponseStreamWrapper.Status.of(result.getStatus(), result.getStatusMessage(),
                            result.getTrailers()));
                    if (result.getStatusMessage() != null && !result.getStatusMessage().isEmpty()) {
                        return Promise.reject(result.getStatusMessage());
                    } else {
                        return Promise.reject("Error occurred while authenticating, gRPC status " + result.getStatus());
                    }
                });
    }

    private void subscribeToTerminationNotification() {
        terminationStream =
                sessionServiceClient.terminationNotification(new TerminationNotificationRequest(), metadata(),
                        (fail, success) -> {
                            if (state == State.Disconnected) {
                                // already disconnected, no need to respond
                                return;
                            }
                            if (fail != null) {
                                // Errors are treated like connection issues, won't signal any shutdown
                                if (checkStatus((ResponseStreamWrapper.ServiceError) fail)) {
                                    // restart the termination notification
                                    subscribeToTerminationNotification();
                                } else {
                                    info.notifyConnectionError(Js.cast(fail));
                                    connectionLost();
                                }
                                return;
                            }
                            assert success != null;

                            // welp; the server is gone -- let everyone know
                            connectionLost();

                            info.notifyServerShutdown(success);
                        });
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
            terminationStream.cancel();
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
    }

    public void setSessionTimeoutMs(double sessionTimeoutMs) {
        this.sessionTimeoutMs = sessionTimeoutMs;
    }

    // @Override
    public void onError(Throwable throwable) {
        info.failureHandled(throwable.toString());
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
        return whenServerReady("get a table").then(serve -> {
            JsLog.debug("innerGetTable", varDef.getTitle(), " started");
            return newState(info,
                    (c, cts, metadata) -> {
                        JsLog.debug("performing fetch for ", varDef.getTitle(), " / ", cts,
                                " (" + LazyString.of(cts::getHandle), ")");
                        // TODO (deephaven-core#188): eliminate this branch by applying preview cols before subscribing
                        if (applyPreviewColumns == null || applyPreviewColumns) {
                            ApplyPreviewColumnsRequest req = new ApplyPreviewColumnsRequest();
                            req.setSourceId(TableTicket.createTableRef(varDef));
                            req.setResultId(cts.getHandle().makeTicket());
                            tableServiceClient.applyPreviewColumns(req, metadata, c::apply);
                        } else {
                            FetchTableRequest req = new FetchTableRequest();
                            req.setSourceId(TableTicket.createTableRef(varDef));
                            req.setResultId(cts.getHandle().makeTicket());
                            tableServiceClient.fetchTable(req, metadata, c::apply);
                        }
                    }, "fetch table " + varDef.getTitle()).then(cts -> {
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
            return new JsFigure(this, c -> {
                JsWidget widget = new JsWidget(this, typedTicket);
                widget.refetch().then(ignore -> {
                    c.apply(null, makeFigureFetchResponse(widget));
                    return null;
                });
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

    @JsMethod
    @SuppressWarnings("ConstantConditions")
    public JsRunnable subscribeToFieldUpdates(JsConsumer<JsVariableChanges> callback) {
        fieldUpdatesCallback.add(callback);
        if (fieldUpdatesCallback.size == 1) {
            fieldsChangeUpdateStream =
                    ResponseStreamWrapper.of(applicationServiceClient.listFields(new ListFieldsRequest(), metadata));
            fieldsChangeUpdateStream.onData(data -> {
                final JsVariableDefinition[] created = new JsVariableDefinition[0];
                final JsVariableDefinition[] updated = new JsVariableDefinition[0];
                final JsVariableDefinition[] removed = new JsVariableDefinition[0];

                JsArray<FieldInfo> removedFI = data.getRemovedList();
                for (int i = 0; i < removedFI.length; ++i) {
                    String removedId = removedFI.getAt(i).getTypedTicket().getTicket().getTicket_asB64();
                    JsVariableDefinition result = knownFields.get(removedId);
                    removed[removed.length] = result;
                    knownFields.remove(removedId);
                }
                JsArray<FieldInfo> createdFI = data.getCreatedList();
                for (int i = 0; i < createdFI.length; ++i) {
                    JsVariableDefinition result = new JsVariableDefinition(createdFI.getAt(i));
                    created[created.length] = result;
                    knownFields.put(result.getId(), result);
                }
                JsArray<FieldInfo> updatedFI = data.getUpdatedList();
                for (int i = 0; i < updatedFI.length; ++i) {
                    JsVariableDefinition result = new JsVariableDefinition(updatedFI.getAt(i));
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
            case Failed:
            case Disconnected:
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
        Ticket ticket = getConfig().newTicket();
        return new TicketAndPromise<>(ticket, whenServerReady("exportScopeTicket").then(server -> {
            ExportRequest req = new ExportRequest();
            req.setSourceId(createTypedTicket(varDef).getTicket());
            req.setResultId(ticket);
            return Callbacks.<ExportResponse, Object>grpcUnaryPromise(
                    c -> sessionServiceClient().exportFromTicket(req, metadata(), c::apply));
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
                        c -> {
                            getWidget(varDef).then(JsWidget::refetch).then(widget -> {
                                c.apply(null, makeFigureFetchResponse(widget));
                                widget.close();
                                return null;
                            }, error -> {
                                c.apply(error, null);
                                return null;
                            });
                        }).refetch());
    }

    private static FetchObjectResponse makeFigureFetchResponse(JsWidget widget) {
        FetchObjectResponse legacyResponse = new FetchObjectResponse();
        legacyResponse.setData(widget.getDataAsU8());
        legacyResponse.setType(widget.getType());
        legacyResponse.setTypedExportIdsList(Arrays.stream(widget.getExportedObjects())
                .map(JsWidgetExportedObject::typedTicket).toArray(TypedTicket[]::new));
        return legacyResponse;
    }

    private TypedTicket createTypedTicket(JsVariableDefinition varDef) {
        TypedTicket typedTicket = new TypedTicket();
        typedTicket.setTicket(TableTicket.createTicket(varDef));
        typedTicket.setType(varDef.getType());
        return typedTicket;
    }

    public Promise<JsWidget> getWidget(JsVariableDefinition varDef) {
        return exportScopeTicket(varDef)
                .race(ticket -> {
                    TypedTicket typedTicket = new TypedTicket();
                    typedTicket.setType(varDef.getType());
                    typedTicket.setTicket(ticket);
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


    public TableServiceClient tableServiceClient() {
        return tableServiceClient;
    }

    public ConsoleServiceClient consoleServiceClient() {
        return consoleServiceClient;
    }

    public SessionServiceClient sessionServiceClient() {
        return sessionServiceClient;
    }

    public FlightServiceClient flightServiceClient() {
        return flightServiceClient;
    }

    public BrowserFlightServiceClient browserFlightServiceClient() {
        return browserFlightServiceClient;
    }

    public InputTableServiceClient inputTableServiceClient() {
        return inputTableServiceClient;
    }

    public ObjectServiceClient objectServiceClient() {
        return objectServiceClient;
    }

    public PartitionedTableServiceClient partitionedTableServiceClient() {
        return partitionedTableServiceClient;
    }

    public StorageServiceClient storageServiceClient() {
        return storageServiceClient;
    }

    public ConfigServiceClient configServiceClient() {
        return configServiceClient;
    }

    public HierarchicalTableServiceClient hierarchicalTableServiceClient() {
        return hierarchicalTableServiceClient;
    }

    public BrowserHeaders metadata() {
        return metadata;
    }

    public <ReqT, RespT> BiDiStream.Factory<ReqT, RespT> streamFactory() {
        return new BiDiStream.Factory<>(this::metadata, config::newTicketInt);
    }

    public Promise<JsTable> newTable(String[] columnNames, String[] types, Object[][] data, String userTimeZone,
            HasEventHandling failHandler) {
        // Store the ref to the data using an array we can clear out, so the data is garbage collected later
        // This means the table can only be created once, but that's probably what we want in this case anyway
        final Object[][][] dataRef = new Object[][][] {data};
        return newState(failHandler, (c, cts, metadata) -> {
            final Object[][] d = dataRef[0];
            if (d == null) {
                c.apply("Data already released, cannot re-create table", null);
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
            FlightData schemaMessage = new FlightData();
            Uint8Array schemaMessagePayload =
                    createMessage(schema, MessageHeader.Schema, Schema.endSchema(schema), 0, 0);
            schemaMessage.setDataHeader(schemaMessagePayload);

            schemaMessage.setAppMetadata(WebBarrageUtils.emptyMessage());
            schemaMessage.setFlightDescriptor(cts.getHandle().makeFlightDescriptor());

            // we wait for any errors in this response to pass to the caller, but success is determined by the eventual
            // table's creation, which can race this
            BiDiStream<FlightData, FlightData> stream = this.<FlightData, FlightData>streamFactory().create(
                    headers -> flightServiceClient.doPut(headers),
                    (first, headers) -> browserFlightServiceClient.openDoPut(first, headers),
                    (next, headers, callback) -> browserFlightServiceClient.nextDoPut(next, headers, callback::apply),
                    new FlightData());
            stream.send(schemaMessage);

            stream.onEnd(status -> {
                if (status.isOk()) {
                    ExportedTableCreationResponse syntheticResponse = new ExportedTableCreationResponse();
                    Uint8Array schemaPlusHeader = new Uint8Array(schemaMessagePayload.length + 8);
                    schemaPlusHeader.set(schemaMessagePayload, 8);
                    syntheticResponse.setSchemaHeader(schemaPlusHeader);
                    syntheticResponse.setSize(data[0].length + "");
                    syntheticResponse.setIsStatic(true);
                    syntheticResponse.setSuccess(true);
                    syntheticResponse.setResultId(cts.getHandle().makeTableReference());

                    c.apply(null, syntheticResponse);
                } else {
                    c.apply(status.getDetails(), null);
                }
            });
            FlightData bodyMessage = new FlightData();
            bodyMessage.setAppMetadata(WebBarrageUtils.emptyMessage());

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
            bodyMessage.setDataHeader(createMessage(bodyData, MessageHeader.RecordBatch, recordBatchOffset, length, 0));
            bodyMessage.setDataBody(padAndConcat(buffers, length));

            stream.send(bodyMessage);
            stream.end();
        }, "creating new table").then(cts -> Promise.resolve(new JsTable(this, cts)));
    }

    private Uint8Array padAndConcat(List<Uint8Array> buffers, int length) {
        Uint8Array all = new Uint8Array(buffers.stream().mapToInt(b -> b.byteLength).sum());
        int currentPosition = 0;
        for (int i = 0; i < buffers.size(); i++) {
            Uint8Array buffer = buffers.get(i);
            all.set(buffer, currentPosition);
            currentPosition += buffer.byteLength;
        }
        assert length == currentPosition;
        return all;
    }

    private static Uint8Array createMessage(FlatBufferBuilder payload, byte messageHeaderType, int messageHeaderOffset,
            int bodyLength, int customMetadataOffset) {
        payload.finish(Message.createMessage(payload, MetadataVersion.V5, messageHeaderType, messageHeaderOffset,
                bodyLength, customMetadataOffset));
        return WebBarrageUtils.bbToUint8ArrayView(payload.dataBuffer());
    }

    public Promise<JsTable> mergeTables(JsTable[] tables, HasEventHandling failHandler) {
        return newState(failHandler, (c, cts, metadata) -> {
            final TableReference[] tableHandles = new TableReference[tables.length];
            for (int i = 0; i < tables.length; i++) {
                final JsTable table = tables[i];
                if (!table.getConnection().equals(this)) {
                    throw new IllegalStateException("Table in merge is not on the worker for this connection");
                }
                tableHandles[i] = new TableReference();
                tableHandles[i].setTicket(tables[i].getHandle().makeTicket());
            }
            JsLog.debug("Merging tables: ", LazyString.of(cts.getHandle()), " for ", cts.getHandle().isResolved(),
                    cts.getResolution());
            MergeTablesRequest requestMessage = new MergeTablesRequest();
            requestMessage.setResultId(cts.getHandle().makeTicket());
            requestMessage.setSourceIdsList(tableHandles);
            tableServiceClient.mergeTables(requestMessage, metadata, c::apply);
        }, "merging tables").then(cts -> Promise.resolve(new JsTable(this, cts)));
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

    private TableTicket newHandle() {
        return new TableTicket(config.newTicketRaw());
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
        TableTicket tableTicket = new TableTicket(unsolicitedTable.getResultId().getTicket().getTicket_asU8());
        JsTableFetch failFetch = (callback, newState, metadata1) -> {
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
        return cache.create(newHandle(), handle -> new ClientTableState(this, handle, fetcher, fetchSummary));
    }

    /**
     *
     * @param fetcher The lambda to perform the fetch of the table's definition.
     * @return A promise that will resolve when the ClientTableState is RUNNING (and fail if anything goes awry).
     *
     *         TODO: consider a fetch timeout.
     */
    public Promise<ClientTableState> newState(HasEventHandling failHandler, JsTableFetch fetcher, String fetchSummary) {
        final TableTicket handle = newHandle();
        final ClientTableState s = cache.create(handle, h -> new ClientTableState(this, h, fetcher, fetchSummary));
        return s.refetch(failHandler, metadata);
    }

    public ClientTableState newState(ClientTableState from, TableConfig to) {
        return newState(from, to, newHandle());
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
        ReleaseRequest releaseRequest = new ReleaseRequest();
        releaseRequest.setId(ticket);
        sessionServiceClient.release(releaseRequest, metadata, null);
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

    public ClientConfiguration getConfig() {
        return config;
    }

    public ConfigValue getServerConfigValue(String key) {
        return constants.getConfigValuesMap().get(key);
    }

    public void onOpen(BiConsumer<Void, String> callback) {
        switch (state) {
            case Connected:
                LazyPromise.runLater(() -> callback.accept(null, null));
                break;
            case Failed:
            case Disconnected:
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
    public String dump(@JsOptional String graphName) {
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
        return whenServerReady("create emptyTable").then(server -> newState(info, (c, cts, metadata) -> {
            EmptyTableRequest emptyTableRequest = new EmptyTableRequest();
            emptyTableRequest.setResultId(cts.getHandle().makeTicket());
            emptyTableRequest.setSize(size + "");
            tableServiceClient.emptyTable(emptyTableRequest, metadata, c::apply);
        }, "emptyTable(" + size + ")")).then(cts -> Promise.resolve(new JsTable(this, cts)));
    }

    public Promise<JsTable> timeTable(double periodNanos, DateWrapper startTime) {
        final long startTimeNanos = startTime == null ? -1 : startTime.getWrapped();
        return whenServerReady("create timetable").then(server -> newState(info, (c, cts, metadata) -> {
            TimeTableRequest timeTableRequest = new TimeTableRequest();
            timeTableRequest.setResultId(cts.getHandle().makeTicket());
            timeTableRequest.setPeriodNanos(periodNanos + "");
            timeTableRequest.setStartTimeNanos(startTimeNanos + "");
            tableServiceClient.timeTable(timeTableRequest, metadata, c::apply);
        }, "create timetable(" + periodNanos + ", " + startTime + ")"))
                .then(cts -> Promise.resolve(new JsTable(this, cts)));
    }
}
