//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.Grpc;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Code;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Transport;
import io.deephaven.javascript.proto.dhinternal.grpcweb.transports.transport.TransportOptions;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.TerminationNotificationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.terminationnotificationresponse.StackTrace;
import io.deephaven.web.client.api.ConnectOptions;
import io.deephaven.web.client.api.QueryConnectable;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.console.JsVariableDescriptor;
import io.deephaven.web.client.api.grpc.GrpcTransport;
import io.deephaven.web.client.api.grpc.GrpcTransportFactory;
import io.deephaven.web.client.api.grpc.GrpcTransportOptions;
import io.deephaven.web.client.api.grpc.MultiplexedWebsocketTransport;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

/**
 * Presently, this is the entrypoint into the Deephaven JS API. By creating an instance of this with the server URL and
 * some options, JS applications can run code on the server, and interact with available exportable objects.
 */
@JsType(namespace = "dh")
public class IdeConnection extends QueryConnectable<IdeConnection> {
    @Deprecated
    public static final String HACK_CONNECTION_FAILURE = "hack-connection-failure";
    public static final String EVENT_DISCONNECT = "disconnect";
    public static final String EVENT_RECONNECT = "reconnect";

    public static final String EVENT_SHUTDOWN = "shutdown";

    private final String serverUrl;

    private final ConnectToken token = new ConnectToken();
    private final ConnectOptions options;

    /**
     * Creates a new instance, from which console sessions can be made.
     * 
     * @param serverUrl The url used when connecting to the server. Read-only.
     * @param connectOptions Optional Object
     */
    @JsIgnore
    public IdeConnection(String serverUrl, Object connectOptions) {
        // Remove trailing slashes from the url
        this.serverUrl = serverUrl.replaceAll("/+$", "");

        if (connectOptions != null) {
            options = new ConnectOptions(connectOptions);
        } else {
            options = new ConnectOptions();
        }
        if (options.transportFactory == null) {
            // assign a default transport factory
            if (options.useWebsockets == Boolean.TRUE || !serverUrl.startsWith("https:")) {
                options.transportFactory = new MultiplexedWebsocketTransport.Factory();
            } else {
                options.transportFactory = new GrpcTransportFactory() {
                    @Override
                    public GrpcTransport create(GrpcTransportOptions options) {
                        return GrpcTransport
                                .from((Transport) Grpc.FetchReadableStreamTransport.onInvoke(new Object())
                                        .onInvoke((TransportOptions) options));
                    }

                    @Override
                    public boolean getSupportsClientStreaming() {
                        return false;
                    }
                };
            }
        }
    }

    @Override
    protected String logPrefix() {
        return "IdeConnection on " + getServerUrl() + ": ";
    }

    @JsIgnore
    @Override
    public ConnectToken getToken() {
        return token;
    }

    @JsIgnore
    @Override
    public ConnectOptions getOptions() {
        return options;
    }

    /**
     * Closes the current connection, releasing any resources on the server or client.
     */
    // Made public to expose to JS
    public void close() {
        super.close();
    }

    /**
     * The url used when connecting to the server.
     * 
     * @return String.
     */
    @Override
    @JsIgnore
    public String getServerUrl() {
        return serverUrl;
    }

    @Override
    public Promise<IdeConnection> running() {
        // This assumes that once the connection has been initialized and left a usable state, it cannot be used again
        if (!connection.isAvailable() || connection.get().isUsable()) {
            return Promise.resolve(this);
        } else {
            return Promise.reject("Cannot connect, session is dead.");
        }
    }

    public Promise<?> getObject(@TsTypeRef(JsVariableDescriptor.class) JsPropertyMap<Object> definitionObject) {
        WorkerConnection conn = connection.get();
        return onConnected().then(e -> conn.getJsObject(definitionObject));
    }

    public JsRunnable subscribeToFieldUpdates(JsConsumer<JsVariableChanges> callback) {
        // Need to make sure the connection is initialized and connected
        WorkerConnection conn = connection.get();
        Promise<JsRunnable> cleanupPromise =
                onConnected().then(e -> Promise.resolve(conn.subscribeToFieldUpdates(callback)));
        return () -> {
            cleanupPromise.then(c -> {
                c.run();
                return null;
            });
        };
    }

    @JsIgnore
    @Override
    public void notifyServerShutdown(TerminationNotificationResponse success) {
        final String details;
        if (!success.getAbnormalTermination()) {
            details = "Server exited normally.";
        } else {
            StringBuilder retval;
            if (!success.getReason().isEmpty()) {
                retval = new StringBuilder(success.getReason());
            } else {
                retval = new StringBuilder("Server exited abnormally.");
            }

            final JsArray<StackTrace> traces = success.getStackTracesList();
            for (int ii = 0; ii < traces.length; ++ii) {
                final StackTrace trace = traces.getAt(ii);
                retval.append("\n\n");
                if (ii != 0) {
                    retval.append("Caused By: ").append(trace.getType()).append(": ").append(trace.getMessage());
                } else {
                    retval.append(trace.getType()).append(": ").append(trace.getMessage());
                }

                final JsArray<String> elements = trace.getElementsList();
                for (int jj = 0; jj < elements.length; ++jj) {
                    retval.append("\n").append(elements.getAt(jj));
                }
            }

            details = retval.toString();
        }

        // fire shutdown advice event
        fireEvent(EVENT_SHUTDOWN, details);

        // fire deprecated event
        notifyConnectionError(new ResponseStreamWrapper.Status() {
            @Override
            public int getCode() {
                return Code.Unavailable;
            }

            @Override
            public String getDetails() {
                return details;
            }

            @Override
            public BrowserHeaders getMetadata() {
                return new BrowserHeaders(); // nothing to offer
            }
        });
    }
}
