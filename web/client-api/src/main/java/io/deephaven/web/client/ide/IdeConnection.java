/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.ide;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.QueryConnectable;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.client.api.console.JsVariableDefinition;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

/**
 */
@JsType(namespace = "dh")
public class IdeConnection extends QueryConnectable<IdeConnection> {
    public static final String HACK_CONNECTION_FAILURE = "hack-connection-failure";
    private final JsRunnable deathListenerCleanup;
    private final String serverUrl;

    private final ConnectToken token = new ConnectToken();

    @Deprecated
    @JsConstructor
    public IdeConnection(String serverUrl, @JsOptional Boolean fromJava) {
        this.serverUrl = serverUrl;
        deathListenerCleanup = JsRunnable.doNothing();

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

    public void close() {
        super.close();

        deathListenerCleanup.run();
    }

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

    public Promise<?> getObject(@TsTypeRef(JsVariableDefinition.class) JsPropertyMap<Object> definitionObject) {
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

    @Override
    public void disconnected() {
        super.disconnected();

        if (connection.isAvailable()) {
            // Currently no way for an IdeConnect to recover, so make sure it doesn't try and reconnect
            connection.get().forceClose();
        }
    }
}
