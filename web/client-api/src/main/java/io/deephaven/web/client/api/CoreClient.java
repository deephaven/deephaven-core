package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.AuthenticationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.AuthenticationConstantsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigValue;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb_service.ConfigServiceClient;
import io.deephaven.javascript.proto.dhinternal.jspb.Map;
import io.deephaven.web.client.api.storage.JsStorageService;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.ide.IdeConnection;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsBiConsumer;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;

import java.util.Objects;
import java.util.function.Consumer;

import static io.deephaven.web.client.api.barrage.WebGrpcUtils.CLIENT_OPTIONS;

@JsType(namespace = "dh")
public class CoreClient extends HasEventHandling {
    public static final String EVENT_CONNECT = "connect",
            EVENT_DISCONNECT = "disconnect",
            EVENT_RECONNECT = "reconnect",
            EVENT_RECONNECT_AUTH_FAILED = "reconnectauthfailed",
            EVENT_REFRESH_TOKEN_UPDATED = "refreshtokenupdated";

    public static final String LOGIN_TYPE_PASSWORD = "password",
            LOGIN_TYPE_ANONYMOUS = "anonymous";

    private final IdeConnection ideConnection;

    public CoreClient(String serverUrl) {
        ideConnection = new IdeConnection(serverUrl, true);

        // For now the only real connection is the IdeConnection, so we re-fire the auth token refresh
        // event here for the UI to listen to
        ideConnection.addEventListener(EVENT_REFRESH_TOKEN_UPDATED, event -> {
            fireEvent(EVENT_REFRESH_TOKEN_UPDATED, event);
        });
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

    public Promise<CoreClient> running() {
        // This assumes that once the connection has been initialized and left a usable state, it cannot be used again
        if (!ideConnection.connection.isAvailable() || ideConnection.connection.get().isUsable()) {
            return Promise.resolve(this);
        } else {
            return Promise.reject("Cannot connect, session is dead.");
        }
    }

    public String getServerUrl() {
        return ideConnection.getServerUrl();
    }

    public Promise<String[][]> getAuthConfigValues() {
        return getConfigs(
                // Explicitly creating a new client, and not passing auth details, so this works pre-connection
                c -> new ConfigServiceClient(getServerUrl(), CLIENT_OPTIONS).getAuthenticationConstants(
                        new AuthenticationConstantsRequest(),
                        c::apply),
                AuthenticationConstantsResponse::getConfigValuesMap);
    }

    public Promise<Void> login(LoginCredentials credentials) {
        Objects.requireNonNull(credentials.getType(), "type must be specified");
        ConnectToken token = ideConnection.getToken();
        if (LOGIN_TYPE_PASSWORD.equals(credentials.getType())) {
            Objects.requireNonNull(credentials.getUsername(), "username must be specified for password login");
            Objects.requireNonNull(credentials.getToken(), "token must be specified for password login");
            token.setType("Basic");
            token.setValue(ConnectToken.bytesToBase64(credentials.getUsername() + ":" + credentials.getToken()));
        } else if (LOGIN_TYPE_ANONYMOUS.equals(credentials.getType())) {
            token.setType("Anonymous");
            token.setValue("");
        } else {
            token.setType(credentials.getType());
            token.setValue(credentials.getToken());
            if (credentials.getUsername() != null) {
                JsLog.warn("username ignored for login type " + credentials.getType());
            }
        }
        Promise<Void> login =
                ideConnection.connection.get().whenServerReady("login").then(ignore -> Promise.resolve((Void) null));

        // fetch configs and check session timeout
        login.then(ignore -> getServerConfigValues()).then(configs -> {
            for (String[] config : configs) {
                if (config[0].equals("http.session.durationMs")) {
                    ideConnection.connection.get().setSessionTimeoutMs(Double.parseDouble(config[1]));
                }
            }
            return null;
        }).catch_(ignore -> {
            // Ignore this failure and suppress browser logging, we have a safe fallback
            return Promise.resolve((Object) null);
        });
        return login;
    }

    public Promise<Void> relogin(@TsTypeRef(JsRefreshToken.class) Object token) {
        return login(LoginCredentials.reconnect(JsRefreshToken.fromObject(token).getBytes()));
    }

    public Promise<Void> onConnected(@JsOptional Double timeoutInMillis) {
        return ideConnection.onConnected();
    }

    public Promise<String[][]> getServerConfigValues() {
        return getConfigs(
                c -> ideConnection.connection.get().configServiceClient().getConfigurationConstants(
                        new ConfigurationConstantsRequest(),
                        ideConnection.connection.get().metadata(),
                        c::apply),
                ConfigurationConstantsResponse::getConfigValuesMap);
    }

    public Promise<UserInfo> getUserInfo() {
        return Promise.resolve(new UserInfo());
    }

    public JsStorageService getStorageService() {
        return new JsStorageService(ideConnection.connection.get());
    }

    public Promise<IdeConnection> getAsIdeConnection() {
        return Promise.resolve(ideConnection);
    }

    public void disconnect() {
        ideConnection.close();
    }
}
