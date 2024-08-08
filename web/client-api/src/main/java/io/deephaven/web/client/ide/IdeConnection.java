//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsArray;
import elemental2.dom.CustomEventInit;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.grpcweb.grpc.Code;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.TerminationNotificationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.terminationnotificationresponse.StackTrace;
import io.deephaven.web.client.api.ConnectOptions;
import io.deephaven.web.client.api.QueryConnectable;
import io.deephaven.web.client.api.ServerObject;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.barrage.stream.ResponseStreamWrapper;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.console.JsVariableDescriptor;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
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

    private final JsRunnable deathListenerCleanup;
    private final String serverUrl;

    private final ConnectToken token = new ConnectToken();
    private final ConnectOptions options;

    /**
     * creates a new instance, from which console sessions can be made. <b>options</b> are optional.
     * 
     * @param serverUrl The url used when connecting to the server. Read-only.
     * @param connectOptions Optional Object
     * @param fromJava Optional boolean
     */
    @Deprecated
    @JsConstructor
    public IdeConnection(String serverUrl, @TsTypeRef(ConnectOptions.class) @JsOptional Object connectOptions,
            @JsOptional Boolean fromJava) {
        this.serverUrl = serverUrl.replaceAll("/+$", "");
        deathListenerCleanup = JsRunnable.doNothing();

        if (connectOptions != null) {
            options = new ConnectOptions(connectOptions);
        } else {
            options = new ConnectOptions();
        }

        if (fromJava != Boolean.TRUE) {
            JsLog.warn(
                    "dh.IdeConnection constructor is deprecated, please create a dh.CoreClient, call login(), then call getAsIdeConnection()");
            token.setType("Anonymous");
            token.setValue("");
            connection.get().whenServerReady("login").then(ignore -> Promise.resolve((Void) null));
        }
    }

    @Override
    protected String logPrefix() {
        return "IdeConnection on " + getServerUrl() + ": ";
    }

    /**
     * Temporary method to split logic between IdeConnection and CoreClient
     */
    @JsIgnore
    public ConnectToken getToken() {
        return token;
    }

    @JsIgnore
    @Override
    public Promise<ConnectToken> getConnectToken() {
        return Promise.resolve(token);
    }

    @JsIgnore
    @Override
    public Promise<ConnectOptions> getConnectOptions() {
        return Promise.resolve(options);
    }

    /**
     * closes the current connection, releasing any resources on the server or client.
     */
    public void close() {
        super.close();

        deathListenerCleanup.run();
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

    /**
     * Makes an {@code object} available to another user or another client on this same server which knows the value of
     * the {@code sharedTicketBytes}. Use that sharedTicketBytes value like a one-time use password - any other client
     * which knows this value can read the same object.
     * <p>
     * Shared objects will remain available using the sharedTicketBytes until the client that first shared them
     * releases/closes their copy of the object. Whatever side-channel is used to share the bytes, be sure to wait until
     * the remote end has signaled that it has successfully fetched the object before releasing it from this client.
     * <p>
     * Be sure to use an unpredictable value for the shared ticket bytes, like a UUID or other large, random value to
     * prevent access by unauthorized clients.
     *
     * @param object the object to share with another client/user
     * @param sharedTicketBytes the value which another client/user must know to obtain the object. It may be a unicode
     *        string (will be encoded as utf8 bytes), or a {@link elemental2.core.Uint8Array} value.
     * @return A promise that will resolve to the value passed as sharedTicketBytes when the object is ready to be read
     *         by another client, or will reject if an error occurs.
     */
    public Promise<SharedExportBytesUnion> shareObject(ServerObject.Union object,
            SharedExportBytesUnion sharedTicketBytes) {
        return connection.get().shareObject(object.asServerObject(), sharedTicketBytes);
    }

    /**
     * Reads an object shared by another client to this server with the {@code sharedTicketBytes}. Until the other
     * client releases this object (or their session ends), the object will be available on the server.
     * <p>
     * The type of the object must be passed so that the object can be read from the server correct - the other client
     * should provide this information.
     *
     * @param sharedTicketBytes the value provided by another client/user to obtain the object. It may be a unicode
     *        string (will be encoded as utf8 bytes), or a {@link elemental2.core.Uint8Array} value.
     * @param type The type of the object, so it can be correctly read from the server
     * @return A promise that will resolve to the shared object, or will reject with an error if it cannot be read.
     */
    public Promise<?> getSharedObject(SharedExportBytesUnion sharedTicketBytes, String type) {
        return connection.get().getSharedObject(sharedTicketBytes, type);
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
        CustomEventInit<String> eventDetails = CustomEventInit.create();
        eventDetails.setDetail(details);
        fireEvent(EVENT_SHUTDOWN, eventDetails);

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
