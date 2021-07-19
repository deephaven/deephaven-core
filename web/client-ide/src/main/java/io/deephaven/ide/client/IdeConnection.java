package io.deephaven.ide.client;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.QueryConnectable;
import io.deephaven.web.shared.fu.JsRunnable;
import io.deephaven.web.shared.ide.ConsoleAddress;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

/**
 */
@JsType(namespace = "dh")
public class IdeConnection extends QueryConnectable<IdeConnection> {
    private final ConsoleAddress address;
    private final JsRunnable deathListenerCleanup;

    @Override
    protected String logPrefix() {
        return "IdeConnection on " + getServerUrl() + ": ";
    }

    /**
     * Direct connection to an already-running worker instance, without first authenticating to a client.
     */
    @JsConstructor
    public IdeConnection(ConsoleAddress address) {
        super(AuthTokenPromiseSupplier.oneShot(address.getToken()));
        this.address = address;
        this.deathListenerCleanup = JsRunnable.doNothing();
    }

    public void close() {
        super.close();

        deathListenerCleanup.run();
    }

    @Override
    @JsIgnore
    public String getServerUrl() {
        return address.getWebsocketUrl();
    }

    @Override
    public Promise<IdeConnection> running() {
        // This assumes that once the connection has been initialized and left a usable state, it cannot be used again
        if (!connection.isAvailable() || connection.get().isUsable()) {
            return Promise.resolve(this);
        } else {
            return (Promise)Promise.reject("Cannot connect, session is dead.");
        }
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
