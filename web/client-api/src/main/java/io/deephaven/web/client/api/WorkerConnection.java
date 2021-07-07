package io.deephaven.web.client.api;

import elemental2.core.JsSet;
import elemental2.core.JsWeakMap;
import elemental2.core.Uint8Array;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.Grpc;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageRecordBatch;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.message_generated.io.deephaven.barrage.flatbuf.Message;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.message_generated.io.deephaven.barrage.flatbuf.MessageHeader;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb.BarrageData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb.SubscriptionRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb_service.BarrageServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.LogSubscriptionData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.LogSubscriptionRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb_service.ConsoleServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.HandshakeRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.HandshakeResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb_service.SessionServiceClient;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.EmptyTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.MergeTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TableReference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TimeTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb_service.TableServiceClient;
import io.deephaven.web.client.api.batch.RequestBatcher;
import io.deephaven.web.client.api.batch.TableConfig;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.api.csv.CsvTypeParser;
import io.deephaven.web.client.api.lifecycle.HasLifecycle;
import io.deephaven.web.client.api.state.StateCache;
import io.deephaven.web.client.api.tree.JsTreeTable;
import io.deephaven.web.client.api.widget.plot.JsFigure;
import io.deephaven.web.client.fu.JsItr;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.state.ClientTableState;
import io.deephaven.web.client.state.HasTableBinding;
import io.deephaven.web.client.state.TableReviver;
import io.deephaven.web.shared.data.*;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.ide.VariableType;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.deephaven.web.client.api.barrage.BarrageUtils.*;

/**
 * Non-exported class, manages the connection to a given worker server. Exported types like QueryInfo and Table
 * will refer to this, and allow us to try to keep track of how many open tables there are, so we can close the
 * connection if not in use.
 *
 * Might make more sense to be part of QueryInfo, but this way we can WeakMap instances, check periodically if
 * any QueryInfos are left alive or event handlers still exist, and close connections that seem unused.
 *
 * Except for the delegated call from QueryInfo.getTable, none of these calls will be possible in Connecting
 * or Disconnected state if done right. Failed state is possible, and we will want to think more about handling,
 * possible re-Promise-ing all of the things, or just return stale values if we have them.
 *
 * Responsible for reconnecting to the query server when required - when that server disappears, and at least one
 * table is left un-closed.
 */
public class WorkerConnection {

    static {
        //TODO configurable, let us support this even when ssl?
        if (DomGlobal.window.location.getProtocol().equals("http:")) {
            Grpc.setDefaultTransport.onInvoke(Grpc.WebsocketTransport.onInvoke());
        }
    }
    private String sessionToken;

    // All calls to the server should share this metadata instance, or copy from it if they need something custom
    private BrowserHeaders metadata = new BrowserHeaders();

    /**
     * States the connection can be in. If non-requested disconnect occurs, transition to
     * reconnecting. If reconnect fails, move to failed, and do not attempt again.
     *
     * If an error happens on the websocket connection, we'll get a close event also - since
     * we also use onError to handle failed work, and will just try one reconnect per close
     * event.
     *
     * Reconnecting requires waiting for the worker to return to "Running" state, requesting
     * a new auth token, and then initiating that connection.
     *
     * Mostly informational, useful for debugging and error messages.
     */
    private enum State {
        Connecting,
        Connected,
        /**
         * Indicates that this worker was deliberately disconnected, should be reconnected again if needed.
         */
        Disconnected,
        Failed,
        Reconnecting
    }

    private final QueryConnectable<?> info;
    private final ClientConfiguration config;
    private final ReconnectState newSessionReconnect;
    private final TableReviver reviver;
    // un-finished fetch operations - these can fail on connection issues, won't be attempted again
    private List<Callback<Void, String>> onOpen = new ArrayList<>();

    private State state;
    private double killTimerCancelation;
    private SessionServiceClient sessionServiceClient;
    private TableServiceClient tableServiceClient;
    private BarrageServiceClient barrageApiClient;
    private ConsoleServiceClient consoleServiceClient;

    private final StateCache cache = new StateCache();
    private final JsWeakMap<HasTableBinding, RequestBatcher> batchers = new JsWeakMap<>();
    private JsWeakMap<TableTicket, JsConsumer<TableTicket>> handleCallbacks = new JsWeakMap<>();
    private JsWeakMap<TableTicket, JsConsumer<InitialTableDefinition>> definitionCallbacks = new JsWeakMap<>();
    private final Set<ClientTableState> flushable = new HashSet<>();
    private final JsSet<JsConsumer<LogItem>> logCallbacks = new JsSet<>();

    private final Map<ClientTableState, ResponseStreamWrapper<BarrageData>> subscriptionStreams = new HashMap<>();

    private Map<TableMapHandle, TableMap> tableMaps = new HashMap<>();

    private JsSet<JsFigure> figures = new JsSet<>();

    private List<LogItem> pastLogs = new ArrayList<>();
    private JsConsumer<LogItem> recordLog = pastLogs::add;
    private ResponseStreamWrapper<LogSubscriptionData> logStream;

    public WorkerConnection(QueryConnectable<?> info, Supplier<Promise<ConnectToken>> authTokenPromiseSupplier) {
        this.info = info;
        this.config = new ClientConfiguration();
        state = State.Connecting;
        this.reviver = new TableReviver(this);
        boolean debugGrpc = false;
        sessionServiceClient = new SessionServiceClient(info.getServerUrl(), JsPropertyMap.of("debug", debugGrpc));
        tableServiceClient = new TableServiceClient(info.getServerUrl(), JsPropertyMap.of("debug", debugGrpc));
        barrageApiClient = new BarrageServiceClient(info.getServerUrl(), JsPropertyMap.of("debug", debugGrpc));
        consoleServiceClient = new ConsoleServiceClient(info.getServerUrl(), JsPropertyMap.of("debug", debugGrpc));

//        builder.setConnectionErrorHandler(msg -> info.failureHandled(String.valueOf(msg)));

        newSessionReconnect = new ReconnectState(() -> {
            connectToWorker(authTokenPromiseSupplier);
        });

        // start connection
        newSessionReconnect.initialConnection();
    }

    /**
     * Creates a new session based on the current auth info, and attempts to
     * re-create all tables and other objects that were currently open.
     *
     * First we assume that the auth token provider is valid, and ask for a new
     * token to provide to the worker.
     *
     * Given that token, we create a new session on the worker server.
     *
     * When a table is first fetched, it might fail - the worker connection
     * will keep trying to connect even if the failure is in one of the above
     * steps. A later attempt to fetch that table may succeed however.
     *
     * Once the table has been successfully fetched, after each reconnect until
     * the table is close()d we'll attempt to restore the table by re-fetching
     * the table, then reapplying all operations on it.
     */
    private void connectToWorker(Supplier<Promise<ConnectToken>> authTokenPromiseSupplier) {
        info.running()
                .then(queryWorkerRunning -> {
                    // get the auth token
                    return authTokenPromiseSupplier.get();
                }).then(authToken -> {
            // create a new session
            HandshakeRequest handshakeRequest = new HandshakeRequest();
            if (authToken != null) {
                Uint8Array token = new Uint8Array(authToken.getBytes().length);
                handshakeRequest.setPayload(token);
            }
            handshakeRequest.setAuthProtocol(1);

            return Callbacks.<HandshakeResponse, Object>grpcUnaryPromise(c -> sessionServiceClient.newSession(handshakeRequest, (BrowserHeaders) null, c::apply));
        }).then(handshakeResponse -> {
            // start the reauth cycle
            authUpdate(handshakeResponse);

            state = State.Connected;

            JsLog.debug("Connected to worker, ensuring all states are refreshed");
            //mark that we succeeded
            newSessionReconnect.success();

            // nuke pending callbacks, we'll remake them
            handleCallbacks = new JsWeakMap<>();
            definitionCallbacks = new JsWeakMap<>();


            // for each cts in the cache, get all with active subs
            ClientTableState[] hasActiveSubs = cache.getAllStates().stream()
                    .peek(cts -> {
                        cts.getHandle().setConnected(false);
                        cts.setSubscribed(false);
                        cts.forActiveLifecycles(item-> {
                            assert !(item instanceof JsTable) ||
                                   ((JsTable)item).state() == cts : "Invalid table state " + item + " does not point to state " + cts;
                            item.suppressEvents();
                        });
                    })
                    .filter(cts -> !cts.isEmpty())
                    .peek(cts -> {
                        cts.forActiveTables(t-> {
                            assert t.state().isAncestor(cts) : "Invalid binding " + t + " (" + t.state() + ") does not contain " + cts;
                        });
                    })
                    .toArray(ClientTableState[]::new);
            // clear caches
            List<ClientTableState> inactiveStatesToRemove = cache.getAllStates().stream()
                    .filter(ClientTableState::isEmpty)
                    .collect(Collectors.toList());
            inactiveStatesToRemove.forEach(cache::release);

            flushable.clear();

            reviver.revive(metadata, hasActiveSubs);

            tableMaps.forEach((handle, tableMap) -> tableMap.refetch());
            figures.forEach((p0, p1, p2) -> p0.refetch());

            info.connected();

            //if any tables have been requested, make sure they start working now that we are connected
            onOpen.forEach(c -> c.onSuccess(null));
            onOpen.clear();

//                // start a heartbeat to check if connection is properly alive
//                ping(success.getAuthSessionToken());

            return Promise.resolve(handshakeResponse);
        }, fail -> {
            // this is non-recoverable, connection/auth/registration failed, but we'll let it start again when state changes
            state = State.Failed;
            JsLog.debug("Failed to connect to worker.");

            final String failure = fail.toString();

            // notify all pending fetches that they failed
            onOpen.forEach(c -> c.onFailure(failure));
            onOpen.clear();

//            if (server != null) {
//                // explicitly disconnect from the query worker
//                server.close();
//            }

            // signal that we should try again
            newSessionReconnect.failed();

            // inform the UI that it failed to connect
            info.failureHandled("Failed to connect: " + failure);
            return null;
        });
    }

    private void authUpdate(HandshakeResponse handshakeResponse) {
        // store the token and schedule refresh calls to keep it alive
        sessionToken = new String(Js.uncheckedCast(handshakeResponse.getSessionToken_asU8()), Charset.forName("UTF-8"));
        String sessionHeaderName = new String(Js.uncheckedCast(handshakeResponse.getMetadataHeader_asU8()), Charset.forName("UTF-8"));
        metadata.set(sessionHeaderName, sessionToken);

        //TODO maybe accept server advice on refresh rates, or just do our own thing
        DomGlobal.setTimeout((ignore) -> {
            HandshakeRequest req = new HandshakeRequest();
            req.setAuthProtocol(0);
            req.setPayload(handshakeResponse.getSessionToken_asU8());
            sessionServiceClient.refreshSessionToken(req, metadata, (fail, success) -> {
                if (fail != null) {
                    //TODO set a flag so others know not to try until we re-trigger initial auth
                    //TODO re-trigger auth
                    return;
                }
                // mark the new token, schedule a new check
                authUpdate(success);
            });
        }, 5000);
    }

    private void notifyLog(LogItem log) {
        for (JsConsumer<LogItem> callback : JsItr.iterate(logCallbacks.keys())) {
            callback.apply(log);
        }
    }

//    @Override
    public void initialSnapshot(TableTicket handle, TableSnapshot snapshot) {
        LazyPromise.runLater(()->{
            //notify table that it has a snapshot available to replace viewport rows
            //TODO looping in this way is not ideal, means that we're roughly O(n*m), where
            //     n is the number of rows, and m the number of tables with viewports.
            //     Instead, we should track all rows here in WorkerConnection, and then
            //     tell every table who might be interested about the rows it is interested in.
            if (!cache.get(handle).isPresent()) {
                JsLog.debug("Discarding snapshot for ", handle, " : ", snapshot);
            }
            cache.get(handle).ifPresent(s->{
                s.setSize(snapshot.getTableSize());
                s.forActiveTables(table -> {
                    table.handleSnapshot(handle, snapshot);
                });
            });
        });
    }

//    @Override
    public void incrementalUpdates(TableTicket tableHandle, DeltaUpdates updates) {
        LazyPromise.runLater(()-> {
            //notify table that it has individual row updates
            final Optional<ClientTableState> cts = cache.get(tableHandle);
            if (!cts.isPresent()) {
                JsLog.debug("Discarding delta for disconnected state ", tableHandle, " : ", updates);
            }
            JsLog.debug("Delta received", tableHandle, updates);
            cts.ifPresent(s-> {
                if (!s.isSubscribed()) {
                    JsLog.debug("Discarding delta for unsubscribed table", tableHandle, updates);
                    return;
                }
                s.handleDelta(updates);
            });
        });
    }

//    @Override
    public void exportedTableUpdateMessage(TableTicket clientId, long size) {
        cache.get(clientId).ifPresent(state -> {
            if (!state.isSubscribed()) {
                // not presently subscribed so this is the only way to be informed of size changes
                state.setSize(size);
            }
        });
    }

//    @Override
    public void exportedTableUpdateMessageError(TableTicket clientId, String errorMessage) {
        cache.get(clientId).ifPresent(state -> {
            state.forActiveTables(t -> t.failureHandled(errorMessage));
        });
    }

//    @Override
    public void onOpen() {
        // never actually called - this instance isn't configured to be the "client" in the connection until auth
        // has succeeded.
        assert false : "WorkerConnection.onOpen() should not be invoked directly, check the stack trace to see how this was triggered";
    }

//    @Override
    public void onClose(int code, String message) {
        // notify all active tables, tablemaps, and figures that the connection is closed
        tableMaps.values().forEach(tableMap -> {
            try {
                tableMap.fireEvent(TableMap.EVENT_DISCONNECT);
                tableMap.suppressEvents();
            } catch (Exception e) {
                JsLog.warn("Error in firing TableMap.EVENT_DISCONNECT event", e);
            }
        });
        figures.forEach((p0, p1, p2) -> {
            try {
                p0.fireEvent(JsFigure.EVENT_DISCONNECT);
                p0.suppressEvents();
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
        onOpen.forEach(c -> c.onFailure("Connection to server closed (" + code + "): " + message));
        onOpen.clear();
    }

    //TODO fold this into the auth reconnect and "my stream puked" check"
//    @Override
//    public void ping(final String lastKnownSessionToken) {
//        // note that lastKnownSessionToken may be null when client manually tries to ping
//
//        if (state == State.Disconnected) {
//            // deliberately closed, stop the ping/pong
//            JsLog.debug("WorkerConnection.ping Disconnected, ignoring");
//            return;
//        }
//
//        // cancel the last timeout check, and schedule a new one
//        DomGlobal.clearTimeout(killTimerCancelation);
//        final double now = Duration.currentTimeMillis();
//        killTimerCancelation = DomGlobal.setTimeout(ignore -> {
//            boolean keepWaiting = isDevMode() && (Duration.currentTimeMillis() - now > 45_000);
//            if (keepWaiting) {
//                // it took quite a bit more than 30s, user was probably stuck in debugger,
//                // or laptop was shut down in some way.  Ping again.
//                ping(null);
//            } else {
//                JsLog.debug("Haven't heard from the server in 30s, reconnecting...");
//                forceReconnect();
//            }
//        }, 30_000);
//
//        // wait 5s, and tell the server that we're here to continue the cycle
//        DomGlobal.setTimeout(ignore -> server.pong(), 5000);
//    }

    @JsMethod
    public void forceReconnect() {
        JsLog.debug("pending: ", definitionCallbacks, handleCallbacks);

        // stop the current connection
//        if (server != null) {
//            server.close();
//        }
        // just in case it wasn't already running, mark us as reconnecting
        state = State.Reconnecting;
        newSessionReconnect.failed();
    }

    @JsMethod
    public void forceClose() {
        //explicitly mark as disconnected so reconnect isn't attempted
        state = State.Disconnected;
//        if (server != null) {
//            server.close();
//        }
        newSessionReconnect.disconnected();
        DomGlobal.clearTimeout(killTimerCancelation);
    }

//    @Override
    public void onError(Throwable throwable) {
        info.failureHandled(throwable.toString());
    }

    public Promise<JsTable> getTable(String tableName) {
        return getTable(tableName, null);
    }

    public Promise<JsTable> getTable(String tableName, Ticket script) {
        return whenServerReady("get a table").then(serve -> {
            JsLog.debug("innerGetTable", tableName, " started");
            return newState(info,
                (c, cts, metadata) -> {
                    JsLog.debug("performing fetch for ", tableName, " / ", cts, " (" + LazyString.of(cts::getHandle), ",", script, ")");
                    assert script != null : "no global scope support at this time";
                    FetchTableRequest fetch = new FetchTableRequest();
                    fetch.setConsoleId(script);
                    fetch.setTableName(tableName);
                    fetch.setTableId(cts.getHandle().makeTicket());
                    consoleServiceClient.fetchTable(fetch, metadata, c::apply);
                }, "fetch table " + tableName
            ).then(cts -> {
                JsLog.debug("innerGetTable", tableName, " succeeded ", cts);
                JsTable table = new JsTable(this, cts);
                return Promise.resolve(table);
            });
        });
    }

    public Promise<JsTable> getPandas(String name) {
        return getPandas(name, null);
    }

    public Promise<JsTable> getPandas(String name, Ticket script) {
        return whenServerReady("get a pandas table").then(serve -> {
            JsLog.debug("innerGetPandasTable", name, " started");
            return newState(info,
                    (c, cts, metadata) -> {
                        JsLog.debug("performing fetch for ", name, " / ", cts, " (" + LazyString.of(cts::getHandle), ",", script, ")");
//                        if (script != null) {
//                            getServer().fetchPandasScriptTable(cts.getHandle(), script, name, c);
//                        } else {
//                            getServer().fetchPandasTable(cts.getHandle(), name, c);
//                        }
                        throw new UnsupportedOperationException("getPandas");

                    }, "fetch pandas table " + name
            ).then(cts -> {
                JsLog.debug("innerGetPandasTable", name, " succeeded ", cts);
                JsTable table = new JsTable(this, cts);
                return Promise.resolve(table);
            });
        });
    }

    public Promise<Object> getObject(JsVariableDefinition definition) {
        switch (VariableType.valueOf(definition.getType())) {
            case Table:
                return (Promise) getTable(definition.getName());
            case TreeTable:
                return (Promise) getTreeTable(definition.getName());
            case Figure:
                return (Promise) getFigure(definition.getName());
            case TableMap:
                return (Promise) getTableMap(definition.getName());
            case Pandas:
                return (Promise) getPandas(definition.getName());
            default:
                return Promise.reject(new Error("Object " + definition.getName() + " unknown type " + definition.getType()));
        }
    }

    public Promise<Object> getObject(JsVariableDefinition definition, Ticket script) {
        switch (VariableType.valueOf(definition.getType())) {
            case Table:
                return (Promise) getTable(definition.getName(), script);
            case TreeTable:
                return (Promise) getTreeTable(definition.getName(), script);
            case Figure:
                return (Promise) getFigure(definition.getName(), script);
            case Pandas:
                return (Promise) getPandas(definition.getName(), script);
            default:
                return Promise.reject(new Error("Object " + definition.getName() + " unknown type " + definition.getType() + " for script."));
        }
    }

    public Promise<Object> whenServerReady(String operationName) {
        switch (state) {
            case Failed:
            case Disconnected:
                state = State.Reconnecting;
                newSessionReconnect.initialConnection();
                //deliberate fall-through
            case Connecting:
            case Reconnecting:
                // Create a new promise around a callback, add that to the list of callbacks to complete when
                // connection is complete
                return Callbacks.<Void, String>promise(info, c -> onOpen.add(c)).then(ignore -> Promise.resolve(this));
            case Connected:
                // Already connected, continue
                return Promise.resolve(this);
            default:
                //not possible, means null state
                //noinspection unchecked
                return (Promise) Promise.reject("Can't " + operationName + " while connection is in state " + state);
        }
    }

    public Promise<TableMap> getTableMap(String tableMapName) {
        return whenServerReady("get a tablemap")
                .then(server -> Promise.resolve(new TableMap(this, tableMapName))
                .then(TableMap::refetch));
    }

    public void registerTableMap(TableMapHandle handle, TableMap tableMap) {
        tableMaps.put(handle, tableMap);
    }

    public Promise<JsTreeTable> getTreeTable(String tableName) {
        return getTreeTable(tableName, null);
    }

    public Promise<JsTreeTable> getTreeTable(String tableName, Ticket script) {
        return getTable(tableName, script).then(t -> {
            Promise<JsTreeTable> result = Promise.resolve(new JsTreeTable(t.state(), this).finishFetch());
            t.close();
            return result;
        });
    }

    public Promise<JsFigure> getFigure(String figureName) {
        return getFigure(figureName, null);
    }

    public Promise<JsFigure> getFigure(String figureName, Ticket script) {
        return whenServerReady("get a figure")
                .then(server -> new JsFigure(this, c -> {
//                    if (script != null) {
//                        getServer().fetchScriptFigure(script, figureName, c);
//                    } else {
//                        getServer().fetchFigure(figureName, c);
//                    }
                    throw new UnsupportedOperationException("getFigure");
                }).refetch());
    }

    public void registerFigure(JsFigure figure) {
        this.figures.add(figure);
    }

    public void releaseFigure(JsFigure figure) {
        this.figures.delete(figure);
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
    public BrowserHeaders metadata() {
        return metadata;
    }

    public Promise<JsTable> newTable(String[] columnNames, String[] types, String[][] data, String userTimeZone, HasEventHandling failHandler) {
        // Store the ref to the data using an array we can clear out, so the data is garbage collected later
        // This means the table can only be created once, but that's probably what we want in this case anyway
        final String[][][] dataRef = new String[][][]{ data };
        return newState(failHandler, (c, cts, metadata) -> {
            final String[][] d = dataRef[0];
            if (d == null) {
                c.apply("Data already released, cannot re-create table", null);
                return;
            }
            dataRef[0] = null;

            final ColumnHolder[] columnHolders = new ColumnHolder[columnNames.length];
            for(int i = 0; i < columnNames.length; i++) {
                columnHolders[i] = CsvTypeParser.createColumnHolder(columnNames[i], types[i], d[i], userTimeZone);
            }
            JsLog.debug("Creating new table: ", LazyString.of(cts.getHandle()), " for ", cts.getHandle().isResolved(), cts.getResolution());
//            getServer().newTable(cts.getHandle(), columnHolders, c);
            throw new UnsupportedOperationException("newTable");
        }, "creating new table").then(cts -> Promise.resolve(new JsTable(this, cts)));
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
            JsLog.debug("Merging tables: ", LazyString.of(cts.getHandle()), " for ", cts.getHandle().isResolved(), cts.getResolution());
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

//    @Override
    public void tableMapStringKeyAdded(TableMapHandle handle, String key) {
        tableMapKeyAdded(handle, key);
    }

//    @Override
    public void tableMapStringArrayKeyAdded(TableMapHandle handle, String[] key) {
        tableMapKeyAdded(handle, key);
    }

    private void tableMapKeyAdded(TableMapHandle handle, Object key) {
        TableMap tableMap = tableMaps.get(handle);
        if (tableMap != null) {
            tableMap.notifyKeyAdded(key);
        }
    }

    public void releaseTableMap(TableMap tableMap, TableMapHandle tableMapHandle) {
//        server.releaseTableMap(tableMapHandle);
        LazyPromise.runLater(() -> {
            TableMap removed = tableMaps.remove(tableMapHandle);
            assert removed == tableMap;
        });
    }

    private TableTicket newHandle() {
        return new TableTicket(config.newTicket());
    }

    public RequestBatcher getBatcher(JsTable table) {
        // LATER: consider a global client.batch(()=>{}) method which causes all table statements to be batched together.
        // We will build this architecture to support this, without wiring it up just yet
        RequestBatcher batcher = batchers.get(table);
        if (batcher == null || batcher.isSent()) {
            final RequestBatcher myBatcher = new RequestBatcher(table, this);
            batchers.set(table, myBatcher);
            myBatcher.onSend(r->{
                // clear out our map references if we're the last batcher to finish running for the given table.
                if (batchers.get(table) == myBatcher) {
                    batchers.delete(table);
                }
            });
            return myBatcher;
        }
        return batcher;
    }

    public ClientTableState newState(JsTableFetch fetcher, String fetchSummary) {
        return cache.create(newHandle(), handle -> new ClientTableState(this, handle, fetcher, fetchSummary));
    }

    /**
     *
     * @param fetcher The lambda to perform the fetch of the table's definition.
     * @return A promise that will resolve when the ClientTableState is RUNNING (and fail if anything goes awry).
     *
     * TODO: consider a fetch timeout.
     */
    public Promise<ClientTableState> newState(HasEventHandling failHandler, JsTableFetch fetcher, String fetchSummary) {
        final TableTicket handle = newHandle();
        final ClientTableState s = cache.create(handle, h->new ClientTableState(this, h, fetcher, fetchSummary));
        return s.refetch(failHandler, metadata);
    }

    public ClientTableState newState(ClientTableState from, TableConfig to) {
        return newState(from, to, newHandle());
    }

    public ClientTableState newState(ClientTableState from, TableConfig to, TableTicket handle) {
        return cache.create(handle, h->from.newState(h, to));
    }

    public StateCache getCache() {
        return cache;
    }

    /**
     * Schedules a deferred command to check the given state for active tables and adjust viewports accordingly.
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
     * @param ticket the ticket to release
     */
    public void releaseTicket(Ticket ticket) {
        //TODO verify cleanup core#223
        sessionServiceClient.release(ticket, metadata, null);
    }


    /**
     * For those calls where we don't really care what happens
     */
    private static final Callback<Void, String> DONOTHING_CALLBACK = new Callback<Void, String>() {
        @Override
        public void onSuccess(Void value) {
            // Do nothing.
        }

        @Override
        public void onFailure(String error) {
            JsLog.error("Callback failed: " + error);
        }
    };

    private void flush() {
        // LATER: instead of running a bunch of serial operations,
        // condense these all into a single batch operation.
        // All three server calls made by this method are _only_ called by this method,
        // so we can reasonably merge all three into a single batched operation.
        ArrayList<ClientTableState> statesToFlush = new ArrayList<>(flushable);
        flushable.clear();


        for (ClientTableState state : statesToFlush) {
            if (state.hasNoSubscriptions()) {
                if (state.isEmpty()) {
                    // completely empty; perform release
                    final ClientTableState.ResolutionState previousState = state.getResolution();
                    state.setResolution(ClientTableState.ResolutionState.RELEASED);
                    state.setSubscribed(false);
                    if (previousState != ClientTableState.ResolutionState.RELEASED) {
                        cache.release(state);

                        JsLog.debug("Releasing state", state, LazyString.of(state.getHandle()));
                        // don't send a release message to the server if the table isn't really there
                        if (state.getHandle().isConnected()) {
                            releaseHandle(state.getHandle());
                        }
                    }
                } else {
                    // state is still retained as it is held by at least one paused binding;
                    // it is either an unsubscribed active table, an interim state for an
                    // active table, or a pending rollback for an operation that has not
                    // yet completed (we leave orphaned nodes paused until a request completes).
                    if (state.isSubscribed()) {
                        state.setSubscribed(false);
                        if (state.getHandle().isConnected()) {
                            ResponseStreamWrapper<BarrageData> stream = subscriptionStreams.remove(state);
                            if (stream != null) {
                                stream.cancel();
                            }
                        }
                    }
                }
            } else {
                List<TableSubscriptionRequest> vps = new ArrayList<>();
                state.forActiveSubscriptions((table, subscription)->{
                    assert table.isActive(state): "Inactive table has a viewport still attached";
                    vps.add(new TableSubscriptionRequest(table.getSubscriptionId(), subscription.getRows(), subscription.getColumns()));
                });

                boolean isViewport = vps.stream().allMatch(req -> req.getRows() != null);
                assert isViewport || vps.stream().noneMatch(req -> req.getRows() != null) : "All subscriptions to a given handle must be consistently viewport or non-viewport";


                BitSet includedColumns = vps.stream().map(TableSubscriptionRequest::getColumns).reduce((bs1, bs2) -> {
                    BitSet result = new BitSet();
                    result.or(bs1);
                    result.or(bs2);
                    return result;
                }).orElseThrow(() -> new IllegalStateException("Cannot call subscribe with zero subscriptions"));
                String[] columnTypes = Arrays.stream(state.getAllColumns())
                        .filter(c -> includedColumns.get(c.getIndex()))
                        .map(Column::getType)
                        .toArray(String[]::new);

                state.setSubscribed(true);

                SubscriptionRequest request = new SubscriptionRequest();
                request.setColumns(makeUint8ArrayFromBitset(includedColumns));
                if (isViewport) {
                    request.setViewport(serializeRanges(vps.stream().map(TableSubscriptionRequest::getRows).collect(Collectors.toSet())));
                }
//                request.setUpdateintervalms();//TODO core#188 support this, along with other subscription improvements
                request.setUseDeephavenNulls(true);

                request.setTicket(state.getHandle().makeTicket());

                final Ticket handle = new Ticket();
                handle.setTicket(config.newTicket());
                request.setExportId(handle);
                ResponseStreamWrapper<BarrageData> stream = ResponseStreamWrapper.of(barrageApiClient.doSubscribeNoClientStream(request, metadata));
                stream.onData(data -> {
                    ByteBuffer body = typedArrayToLittleEndianByteBuffer(data.getDataBody_asU8());
                    Message headerMessage = Message.getRootAsMessage(new io.deephaven.javascript.proto.dhinternal.flatbuffers.ByteBuffer(data.getDataHeader_asU8()));
                    if (body.limit() == 0 && headerMessage.headerType() != MessageHeader.BarrageRecordBatch) {
                        // a subscription stream presently ignores schemas and other message types
                        return;
                    }
                    BarrageRecordBatch header = headerMessage.header(new BarrageRecordBatch());
                    if (header.isSnapshot()) {
                        initialSnapshot(state.getHandle(), createSnapshot(header, body, isViewport, columnTypes));
                    } else {
                        incrementalUpdates(state.getHandle(), createDelta(header, body, isViewport, columnTypes));
                    }
                });
                ResponseStreamWrapper<BarrageData> oldStream = subscriptionStreams.put(state, stream);
                if (oldStream != null) {
                    // cancel any old stream, we presently expect a fresh instance
                    oldStream.cancel();
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

    public void onOpen(BiConsumer<Void, String> callback) {
        switch (state) {
            case Connected:
                LazyPromise.runLater(()->callback.accept(null, null));
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
            //TODO core#225 track latest message seen and only sub after that
            logStream = ResponseStreamWrapper.of(consoleServiceClient.subscribeToLogs(new LogSubscriptionRequest(), metadata));
            logStream.onData(data -> {
                LogItem logItem = new LogItem();
                logItem.setLogLevel(data.getLogLevel());
                logItem.setMessage(data.getMessage());
                logItem.setMicros(data.getMicros());

                notifyLog(logItem);
            });
            logStream.onEnd(status -> {
                //TODO handle reconnect
            });
        } else {
            pastLogs.forEach(callback::apply);
        }
        return ()-> {
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
                graph.append("  table").append(tableId).append(" -> handle").append(binding.getTable().getHandle().hashCode()).append("[color=blue];\n");
                if (binding.getRollback() != null) {
                    graph.append("  handle").append(binding.getState().getHandle().hashCode()).append(" -> handle").append(binding.getRollback().getState().getHandle().hashCode()).append(" [style=dotted, label=rollback];\n");
                }
            });
        });

        return graph.append("}").toString();
    }

    private void appendStatesToDump(ClientTableState parent, Map<ClientTableState, List<ClientTableState>> statesAndParents, StringBuilder graph) {
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
            graph.append(" -> handle").append(clientTableState.getHandle().hashCode()).append("[label=\"").append(clientTableState.getFetchSummary().replaceAll("\"", "")).append("\"];\n");
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
        }, "create timetable(" + periodNanos + ", " + startTime + ")")).then(cts -> Promise.resolve(new JsTable(this, cts)));
    }
}
