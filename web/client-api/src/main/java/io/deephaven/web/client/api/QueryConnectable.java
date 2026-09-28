//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.common.io.BaseEncoding;
import com.vertispan.grpc.fetch.FetchChannel;
import com.vertispan.tsdefs.annotations.TsIgnore;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.JsSet;
import elemental2.dom.URL;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.TerminationNotificationResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.script.grpc.GetConsoleTypesRequest;
import io.deephaven.proto.backplane.script.grpc.GetConsoleTypesResponse;
import io.deephaven.proto.backplane.script.grpc.GetHeapInfoRequest;
import io.deephaven.proto.backplane.script.grpc.GetHeapInfoResponse;
import io.deephaven.proto.backplane.script.grpc.StartConsoleRequest;
import io.deephaven.proto.backplane.script.grpc.StartConsoleResponse;
import io.deephaven.web.client.api.barrage.stream.AuthenticationInterceptor;
import io.deephaven.web.client.api.barrage.stream.ClientBrowserStreamInterceptor;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.grpc.CustomTransportChannel;
import io.deephaven.web.client.api.grpc.GrpcTransportFactory;
import io.deephaven.web.client.ide.IdeSession;
import io.deephaven.web.client.fu.CancellablePromise;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.InternalMetadata;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.deephaven.web.client.ide.IdeConnection.HACK_CONNECTION_FAILURE;
import static io.deephaven.web.shared.fu.PromiseLike.CANCELLATION_MESSAGE;

/**
 * JS-exposed supertype handling details about connecting to a deephaven query worker. Wraps the WorkerConnection
 * instance, which manages the connection to the API server.
 */
@TsIgnore
public abstract class QueryConnectable<Self extends QueryConnectable<Self>> extends HasEventHandling {

    public final AuthenticationInterceptor authenticationInterceptor = new AuthenticationInterceptor();
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

    public abstract ConnectToken getToken();

    public abstract ConnectOptions getOptions();

    @Deprecated
    public void notifyConnectionError(Throwable err) {
        if (err instanceof StatusRuntimeException) {
            StatusRuntimeException sre = (StatusRuntimeException) err;
            notifyConnectionError(sre);
        } else {
            notifyConnectionError(Status.fromThrowable(err).asRuntimeException());
        }
    }

    @Deprecated
    public void notifyConnectionError(StatusRuntimeException sre) {
        if (notifiedConnectionError || !hasListeners(HACK_CONNECTION_FAILURE)) {
            return;
        }
        notifiedConnectionError = true;

        fireEvent(HACK_CONNECTION_FAILURE, JsPropertyMap.of(
                "status", sre.getStatus().getCode().value(),
                "details", sre.getStatus().getDescription(),
                "metadata", makeHeaders(sre.getTrailers())));
        JsLog.warn(
                "The event dh.IdeConnection.HACK_CONNECTION_FAILURE is deprecated and will be removed in a later release");
    }

    private static JsPropertyMap<String> makeHeaders(final Metadata metadata) {
        final BaseEncoding base64 = BaseEncoding.base64().omitPadding();

        final JsPropertyMap<String> result = JsPropertyMap.of();
        final byte[][] bytes = InternalMetadata.serialize(metadata);
        for (int i = 0; i < bytes.length; i += 2) {
            final String key = new String(bytes[i], StandardCharsets.UTF_8);
            final String value;
            if (key.endsWith("-bin")) {
                value = base64.encode(bytes[i + 1]);
            } else {
                value = new String(bytes[i + 1], StandardCharsets.UTF_8);
            }
            result.set(key, value);
        }
        return result;
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
        final Tickets config = connection.get().getTickets();
        final Ticket ticket = config.newExportTicket();

        final JsRunnable closer = () -> {
            boolean run = !cancelled.has(ticket);
            if (run) {
                cancelled.add(ticket);
                connection.get().releaseTicket(ticket);
            }
        };

        onConnected().then(e -> Callbacks.<StartConsoleResponse>grpcUnaryPromise(callback -> {
            StartConsoleRequest request = StartConsoleRequest.newBuilder()
                    .setSessionType(type)
                    .setResultId(ticket)
                    .build();
            connection.get().consoleServiceClient().startConsole(request, callback);
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
            GetConsoleTypesRequest request = GetConsoleTypesRequest.getDefaultInstance();
            connection.get().consoleServiceClient().getConsoleTypes(request, callback);
        });

        return promise.then(result -> {
            JsArray<String> strings = Js.uncheckedCast(result.getConsoleTypesList().toArray());
            return Promise.resolve(strings);
        });
    }

    @JsMethod
    public Promise<JsWorkerHeapInfo> getWorkerHeapInfo() {
        Promise<GetHeapInfoResponse> promise = Callbacks.grpcUnaryPromise(callback -> {
            GetHeapInfoRequest request = GetHeapInfoRequest.getDefaultInstance();
            connection.get().consoleServiceClient().getHeapInfo(request, callback);
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

    public boolean supportsClientStreaming() {
        return getOptions().transportFactory != null && getOptions().transportFactory.getSupportsClientStreaming();
    }

    /**
     * Factory to produce grpc stubs with the configured transport, including authentication, support for emulated bidi
     * streams, and user-requested headers.
     */
    public <T> T createStub(Function<Channel, T> constructor) {
        return makeChannel(constructor, authenticationInterceptor, new ClientBrowserStreamInterceptor());
    }

    /**
     * Factory to produce grpc stubs with the configured transport and user-requested headers. No auth is provided, and
     * emulated streams cannot be available without auth.
     */
    public <T> T createStubNoAuth(Function<Channel, T> constructor) {
        return makeChannel(constructor);
    }

    private <T> T makeChannel(Function<Channel, T> constructor, ClientInterceptor... interceptors) {
        GrpcTransportFactory transportFactory = getOptions().transportFactory;

        Channel channel;
        if (transportFactory != null) {
            channel = new CustomTransportChannel(new URL(getServerUrl()), transportFactory);
        } else {
            channel = new FetchChannel(new URL(getServerUrl()));
        }
        if (getOptions().headers != null) {
            interceptors[interceptors.length] =
                    MetadataUtils.newAttachHeadersInterceptor(makeMetadata(getOptions().headers));
        }
        return constructor.apply(ClientInterceptors.intercept(
                channel,
                interceptors));
    }

    private Metadata makeMetadata(JsPropertyMap<String> headers) {
        Metadata result = new Metadata();
        JsArray<String> keys = JsObject.keys(headers);
        BaseEncoding base64 = BaseEncoding.base64().omitPadding();
        for (int i = 0; i < keys.length; ++i) {
            String key = keys.getAt(i);
            String value = headers.get(key);
            if (value != null) {
                if (key.endsWith("-bin")) {
                    result.put(Metadata.Key.of(key, Metadata.BINARY_BYTE_MARSHALLER), base64.decode(value));
                } else {
                    result.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
                }
            }
        }

        return result;
    }

    public void login(String type, String token) {
        authenticationInterceptor.login(type, token);
    }

    public void logout() {
        authenticationInterceptor.deauth();
    }
}
