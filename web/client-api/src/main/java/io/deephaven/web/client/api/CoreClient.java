package io.deephaven.web.client.api;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.storage.JsStorageService;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
public class CoreClient extends QueryConnectable<CoreClient> {
    public static final String EVENT_CONNECT = "connect",
            EVENT_DISCONNECT = "disconnect",
            EVENT_RECONNECT = "reconnect",
            EVENT_RECONNECT_AUTH_FAILED = "reconnectauthfailed",
            EVENT_REFRESH_TOKEN_UPDATED = "refreshtokenupdated";

    public static final String LOGIN_TYPE_PASSWORD = "password",
            LOGIN_TYPE_SAML = "saml",
            LOGIN_TYPE_REFRESH = "refresh",
            LOGIN_TYPE_PSK = "psk",
            LOGIN_TYPE_OIDC = "oidc",
            LOGIN_TYPE_ANONYMOUS = "anonymous";

    private final String serverUrl;

    public CoreClient(String serverUrl) {
        super(AuthTokenPromiseSupplier.oneShot(null));
        this.serverUrl = serverUrl;
    }

    @Override
    public Promise<CoreClient> running() {
        // This assumes that once the connection has been initialized and left a usable state, it cannot be used again
        if (!connection.isAvailable() || connection.get().isUsable()) {
            return Promise.resolve(this);
        } else {
            return (Promise) Promise.reject("Cannot connect, session is dead.");
        }
    }

    @Override
    public String getServerUrl() {
        return serverUrl;
    }

    public Promise<String[][]> getAuthConfigValues() {
        return Promise.resolve(new String[0][]);
    }

    public Promise<Void> login(LoginCredentials credentials) {
        return connection.get().whenServerReady("login").then(ignore -> Promise.resolve((Void) null));
    }

    public Promise<Void> relogin(String token) {
        return login(LoginCredentials.reconnect(token));
    }

    public Promise<String[][]> getServerConfigValues() {
        return Promise.resolve(new String[0][]);
    }

    public Promise<UserInfo> getUserInfo() {
        return Promise.resolve(new UserInfo());
    }

    public JsStorageService getStorageService() {
        return new JsStorageService(connection.get());
    }
}
