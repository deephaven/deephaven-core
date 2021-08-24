package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.JsSet;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.ide.shared.IdeSession;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetConsoleTypesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetConsoleTypesResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.StartConsoleRequest;
import io.deephaven.web.client.fu.CancellablePromise;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.data.LogItem;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.fu.RemoverFn;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static io.deephaven.web.shared.fu.PromiseLike.CANCELLATION_MESSAGE;

/**
 * JS-exposed supertype handling details about connecting to a deephaven query worker. Wraps the
 * WorkerConnection instance, which manages the connection to the API server.
 */
public abstract class QueryConnectable<Self extends QueryConnectable<Self>>
    extends HasEventHandling {
    public interface AuthTokenPromiseSupplier extends Supplier<Promise<ConnectToken>> {
        default AuthTokenPromiseSupplier withInitialToken(ConnectToken initialToken) {
            AuthTokenPromiseSupplier original = this;
            return new AuthTokenPromiseSupplier() {
                boolean usedInitialToken = false;

                @Override
                public Promise<ConnectToken> get() {
                    if (!usedInitialToken) {
                        usedInitialToken = true;
                        return Promise.resolve(initialToken);
                    }
                    return original.get();
                }
            };
        }

        static AuthTokenPromiseSupplier oneShot(ConnectToken initialToken) {
            // noinspection unchecked
            return ((AuthTokenPromiseSupplier) () -> (Promise) Promise
                .reject("Only one token provided, cannot reconnect"))
                    .withInitialToken(initialToken);
        }
    }

    protected final JsLazy<WorkerConnection> connection;

    @JsProperty(namespace = "dh.QueryInfo") // "legacy" location
    public static final String EVENT_TABLE_OPENED = "tableopened";
    @JsProperty(namespace = "dh.QueryInfo")
    public static final String EVENT_DISCONNECT = "disconnect";
    @JsProperty(namespace = "dh.QueryInfo")
    public static final String EVENT_RECONNECT = "reconnect";
    @JsProperty(namespace = "dh.QueryInfo")
    public static final String EVENT_CONNECT = "connect";

    @JsProperty(namespace = "dh.IdeConnection")
    public static final String HACK_CONNECTION_FAILURE = "hack-connection-failure";

    private final List<IdeSession> sessions = new ArrayList<>();
    private final JsSet<Ticket> cancelled = new JsSet<>();

    private boolean connected;
    private boolean closed;
    private boolean hasDisconnected;

    public QueryConnectable(Supplier<Promise<ConnectToken>> authTokenPromiseSupplier) {
        this.connection = JsLazy.of(() -> new WorkerConnection(this, authTokenPromiseSupplier));
    }

    public void notifyConnectionError(ResponseStreamWrapper.Status status) {
        CustomEventInit event = CustomEventInit.create();
        event.setDetail(JsPropertyMap.of(
            "status", status.getCode(),
            "details", status.getDetails(),
            "metadata", status.getMetadata()));
        fireEvent(HACK_CONNECTION_FAILURE, event);
    }

    @Override
    @JsMethod
    public RemoverFn addEventListener(String name, EventFn callback) {
        return super.addEventListener(name, callback);
    }

    private Promise<Void> onConnected() {
        if (connected) {
            return Promise.resolve((Void) null);
        }
        if (closed) {
            return (Promise) Promise.reject("Connection already closed");
        }

        return new Promise<>((resolve, reject) -> addEventListenerOneShot(
            EventPair.of(EVENT_CONNECT, e -> resolve.onInvoke((Void) null)),
            EventPair.of(EVENT_DISCONNECT, e -> reject.onInvoke("Connection disconnected"))));
    }

    @JsIgnore
    public String getServerUrl() {
        throw new UnsupportedOperationException();
    }

    public abstract Promise<Self> running();

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
        ticket.setTicket(config.newTicket());

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
            connection.get().consoleServiceClient().startConsole(request,
                connection.get().metadata(), callback::apply);
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
            connection.get().consoleServiceClient().getConsoleTypes(request,
                connection.get().metadata(), callback::apply);
        });

        return promise.then(result -> Promise.resolve(result.getConsoleTypesList()));
    }


    public void connected() {
        if (closed) {
            JsLog.debug(getClass(), " closed before worker could connect");
            return;
        }

        JsLog.debug(getClass(), " connected");

        connected = true;

        fireEvent(EVENT_CONNECT);

        if (hasDisconnected) {
            if (hasListeners(EVENT_RECONNECT)) {
                fireEvent(EVENT_RECONNECT);
            } else {
                DomGlobal.console.log(logPrefix()
                    + "Query reconnected (to prevent this log message, handle the EVENT_RECONNECT event)");
            }
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

        if (hasListeners(EVENT_DISCONNECT)) {
            this.fireEvent(QueryConnectable.EVENT_DISCONNECT);
        } else {
            DomGlobal.console.log(logPrefix()
                + "Query disconnected (to prevent this log message, handle the EVENT_DISCONNECT event)");
        }
    }
}
