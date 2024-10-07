//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsIgnore;
import elemental2.core.JsArray;
import elemental2.core.JsSet;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.TerminationNotificationResponse;
import io.deephaven.web.client.ide.IdeSession;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.fu.CancellablePromise;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.List;

import static io.deephaven.web.client.ide.IdeConnection.HACK_CONNECTION_FAILURE;
import static io.deephaven.web.shared.fu.PromiseLike.CANCELLATION_MESSAGE;

/**
 * JS-exposed supertype handling details about connecting to a deephaven query worker. Wraps the WorkerConnection
 * instance, which manages the connection to the API server.
 */
@TsIgnore
public abstract class QueryConnectable<Self extends QueryConnectable<Self>> extends HasEventHandling {

    private final List<IdeSession> sessions = new ArrayList<>();
    private final JsSet<Ticket> cancelled = new JsSet<>();

    protected final JsLazy<WorkerConnection> connection;

    private boolean connected;
    private boolean closed;
    private boolean hasDisconnected;
    private boolean notifiedConnectionError = false;

    public QueryConnectable() {
        this.connection = JsLazy.of(() -> new WorkerConnection(this));
    }

    public abstract Promise<ConnectToken> getConnectToken();

    public abstract Promise<ConnectOptions> getConnectOptions();

    @Deprecated
    public void notifyConnectionError(ResponseStreamWrapper.Status status) {
        if (notifiedConnectionError || !hasListeners(HACK_CONNECTION_FAILURE)) {
            return;
        }
        notifiedConnectionError = true;

        CustomEventInit<JsPropertyMap<Object>> event = CustomEventInit.create();
        event.setDetail(JsPropertyMap.of(
                "status", status.getCode(),
                "details", status.getDetails(),
                "metadata", status.getMetadata()));
        fireEvent(HACK_CONNECTION_FAILURE, event);
        JsLog.warn(
                "The event dh.IdeConnection.HACK_CONNECTION_FAILURE is deprecated and will be removed in a later release");
    }

    protected Promise<Void> onConnected() {
        if (connected) {
            return Promise.resolve((Void) null);
        }
        if (closed) {
            return Promise.reject("Connection already closed");
        }

        return new Promise<>((resolve, reject) -> addEventListenerOneShot(
                EventPair.of(QueryInfoConstants.EVENT_CONNECT, e -> resolve.onInvoke((Void) null)),
                EventPair.of(QueryInfoConstants.EVENT_DISCONNECT, e -> reject.onInvoke("Connection disconnected"))));
    }

    @JsIgnore
    public String getServerUrl() {
        throw new UnsupportedOperationException();
    }

    /**
     * Internal method to permit delegating to some orchestration tool to see if this worker can be connected to yet.
     */
    @JsIgnore
    public Promise<Self> onReady() {
        // noinspection unchecked
        return Promise.resolve((Self) this);
    }

    /**
     * Promise that resolves when this worker instance can be connected to, or rejects if it can't be used.
     * 
     * @return A promise that resolves with this instance.
     */
    public abstract Promise<Self> running();

    /**
     * Register a callback function to handle any log messages that are emitted on the server. Returns a function ,
     * which can be invoked to remove this log handler. Any log handler registered in this way will receive as many old
     * log messages as are presently available.
     * 
     * @param callback
     * @return {@link JsRunnable}
     */
    @JsMethod
    public JsRunnable onLogMessage(JsConsumer<LogItem> callback) {
        final WorkerConnection connect = connection.get();
        // The method returns a singleton, but we'll be extra safe here anyway.
        final JsRunnable DO_NOTHING = JsRunnable.doNothing();
        // we'll use this array to handle async nature of connections.
        JsRunnable[] cancel = {DO_NOTHING};
        connect.onOpen((s, f) -> {
            // if the open did not fail, and the cancel array has not been modified...
            if (f == null && cancel[0] == DO_NOTHING) {
                // then go ahead and subscribe, plus stash the removal callback.
                cancel[0] = connect.subscribeToLogs(callback);
            }
        });
        return () -> {
            if (cancel[0] != null) {
                // if we have subscribed, this will cancel that subscription.
                cancel[0].run();
                // we'll use null here to tell the onOpen above to skip doing work
                cancel[0] = null;
            }
        };
    }

    @JsMethod
    public CancellablePromise<IdeSession> startSession(String type) {
        JsLog.debug("Starting", type, "console session");
        LazyPromise<Ticket> promise = new LazyPromise<>();
        final ClientConfiguration config = connection.get().getConfig();
        final Ticket ticket = new Ticket();
        ticket.setTicket(config.newTicketRaw());

        final JsRunnable closer = () -> {
            boolean run = !cancelled.has(ticket);
            if (run) {
                cancelled.add(ticket);
                connection.get().releaseTicket(ticket);
            }
        };

        onConnected().then(e -> Callbacks.grpcUnaryPromise(callback -> {
            StartConsoleRequest request = new StartConsoleRequest();
            request.setSessionType(type);
            request.setResultId(ticket);
            connection.get().consoleServiceClient().startConsole(request, connection.get().metadata(), callback::apply);
        })).then(result -> {
            promise.succeed(ticket);
            return null;
        }, error -> {
            promise.fail(error);
            return null;
        });

        return promise.asPromise(result -> {
            if (cancelled.has(ticket)) {
                // this is caught and turned into a promise failure.
                // a bit hacky, but it works...
                throw new RuntimeException(CANCELLATION_MESSAGE);
            }

            final IdeSession session = new IdeSession(connection.get(), result, closer);
            sessions.add(session);
            return session;
        }, closer);
    }

    @JsMethod
    public Promise<JsArray<String>> getConsoleTypes() {
        Promise<GetConsoleTypesResponse> promise = Callbacks.grpcUnaryPromise(callback -> {
            GetConsoleTypesRequest request = new GetConsoleTypesRequest();
            connection.get().consoleServiceClient().getConsoleTypes(request, connection.get().metadata(),
                    callback::apply);
        });

        return promise.then(result -> Promise.resolve(result.getConsoleTypesList()));
    }

    @JsMethod
    public Promise<JsWorkerHeapInfo> getWorkerHeapInfo() {
        Promise<GetHeapInfoResponse> promise = Callbacks.grpcUnaryPromise(callback -> {
            GetHeapInfoRequest request = new GetHeapInfoRequest();
            connection.get().consoleServiceClient().getHeapInfo(request, connection.get().metadata(),
                    callback::apply);
        });

        return promise.then(result -> Promise.resolve(new JsWorkerHeapInfo(result)));
    }

    public void connected() {
        if (closed) {
            JsLog.debug(getClass(), " closed before worker could connect");
            return;
        }

        JsLog.debug(getClass(), " connected");

        connected = true;
        notifiedConnectionError = false;

        fireEvent(QueryInfoConstants.EVENT_CONNECT);

        if (hasDisconnected) {
            fireCriticalEvent(QueryInfoConstants.EVENT_RECONNECT);
        }
    }

    protected void close() {
        JsLog.debug(getClass(), " closed");

        if (connection.isAvailable()) {
            connection.get().forceClose();
        }

        closed = true;
    }

    /**
     * Triggered when the connection has disconnected
     */
    public void disconnected() {
        JsLog.debug(getClass(), " disconnected");

        connected = false;

        hasDisconnected = true;

        fireCriticalEvent(QueryInfoConstants.EVENT_DISCONNECT);
    }

    public abstract void notifyServerShutdown(TerminationNotificationResponse success);
}
