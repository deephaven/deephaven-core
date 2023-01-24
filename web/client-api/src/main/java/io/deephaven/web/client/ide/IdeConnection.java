/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.ide;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.CoreClient;
import io.deephaven.web.client.api.QueryConnectable;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

/**
 */
@JsType(namespace = "dh")
public class IdeConnection extends QueryConnectable<IdeConnection> {
    private final String serverUrl;

    private final JsRunnable deathListenerCleanup;
    private CoreClient coreClient;

    @JsIgnore
    public IdeConnection(CoreClient coreClient) {
        // Delegate to the js constructor so we can still expose it to JS
        this(coreClient.getServerUrl());

        this.coreClient = coreClient;
    }

    /**
     * Direct connection to an already-running worker instance, without first authenticating to a client.
     */
    @JsConstructor
    @Deprecated
    public IdeConnection(String serverUrl) {
        this.serverUrl = serverUrl;
        this.deathListenerCleanup = JsRunnable.doNothing();
    }

    @Override
    protected String logPrefix() {
        return "IdeConnection on " + getServerUrl() + ": ";
    }

    @Override
    public Promise<ConnectToken> getConnectToken() {
        if (coreClient == null) {
            return Promise.resolve(new ConnectToken());
        }
        return coreClient.getConnectToken();
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

    public Promise<?> getObject(JsPropertyMap<Object> definitionObject) {
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
