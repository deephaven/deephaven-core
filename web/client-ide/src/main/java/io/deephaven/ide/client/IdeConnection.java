/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ide.client;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.Callbacks;
import io.deephaven.web.client.api.QueryConnectable;
import io.deephaven.web.client.api.WorkerConnection;
import io.deephaven.web.client.api.console.JsVariableChanges;
import io.deephaven.web.shared.fu.JsConsumer;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.data.ConnectToken;
import jsinterop.annotations.*;
import jsinterop.base.JsPropertyMap;

import java.nio.charset.StandardCharsets;

/**
 */
@JsType(namespace = "dh")
public class IdeConnection extends QueryConnectable<IdeConnection> {
    @JsMethod(namespace = JsPackage.GLOBAL)
    private static native String atob(String encodedData);

    private static AuthTokenPromiseSupplier getAuthTokenPromiseSupplier(IdeConnectionOptions options) {
        ConnectToken token = null;
        if (options != null && options.authToken != null) {
            token = new ConnectToken();
            token.setBytes(atob(options.authToken).getBytes(StandardCharsets.UTF_8));
        }
        return AuthTokenPromiseSupplier.oneShot(token);
    }

    private final String serverUrl;

    private final JsRunnable deathListenerCleanup;

    @Override
    protected String logPrefix() {
        return "IdeConnection on " + getServerUrl() + ": ";
    }

    /**
     * Direct connection to an already-running worker instance, without first authenticating to a client.
     */
    @JsConstructor
    public IdeConnection(String serverUrl, @JsOptional IdeConnectionOptions options) {
        super(getAuthTokenPromiseSupplier(options));
        this.serverUrl = serverUrl;
        this.deathListenerCleanup = JsRunnable.doNothing();
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
            return (Promise) Promise.reject("Cannot connect, session is dead.");
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
