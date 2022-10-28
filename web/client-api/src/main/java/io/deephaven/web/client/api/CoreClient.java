package io.deephaven.web.client.api;

import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.AuthenticationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.AuthenticationConstantsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigValue;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsResponse;
import io.deephaven.javascript.proto.dhinternal.jspb.Map;
import io.deephaven.web.client.api.storage.JsStorageService;
import io.deephaven.web.shared.fu.JsBiConsumer;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.annotations.JsType;

import java.util.function.Consumer;

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

    private <R> Promise<String[][]> getConfigs(Consumer<JsBiConsumer<Object, R>> rpcCall,
            JsFunction<R, Map<String, ConfigValue>> getConfigValues) {
        return Callbacks.grpcUnaryPromise(rpcCall).then(response -> {
            String[][] result = new String[0][];
            getConfigValues.apply(response).forEach((item, key) -> {
                result[result.length] = new String[] {key, item.getStringValue()};
            });
            return Promise.resolve(result);
        });
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
        return getConfigs(
                c -> connection.get().configServiceClient().getAuthenticationConstants(
                        new AuthenticationConstantsRequest(),
                        connection.get().metadata(),
                        c::apply),
                AuthenticationConstantsResponse::getConfigValuesMap);
    }

    public Promise<Void> login(LoginCredentials credentials) {
        return connection.get().whenServerReady("login").then(ignore -> Promise.resolve((Void) null));
    }

    public Promise<Void> relogin(String token) {
        return login(LoginCredentials.reconnect(token));
    }

    public Promise<String[][]> getServerConfigValues() {
        return getConfigs(
                c -> connection.get().configServiceClient().getConfigurationConstants(
                        new ConfigurationConstantsRequest(),
                        connection.get().metadata(),
                        c::apply),
                ConfigurationConstantsResponse::getConfigValuesMap);
    }

    public Promise<UserInfo> getUserInfo() {
        return Promise.resolve(new UserInfo());
    }

    public JsStorageService getStorageService() {
        return new JsStorageService(connection.get());
    }
}
