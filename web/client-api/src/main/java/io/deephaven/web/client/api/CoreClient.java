//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.core.JsObject;
import elemental2.promise.Promise;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.AuthenticationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.AuthenticationConstantsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigValue;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb_service.ConfigServiceClient;
import io.deephaven.javascript.proto.dhinternal.jspb.Map;
import io.deephaven.web.client.api.storage.JsStorageService;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.ide.IdeConnection;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsBiConsumer;
import io.deephaven.web.shared.fu.JsFunction;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Objects;
import java.util.function.Consumer;

import static io.deephaven.web.client.api.barrage.WebGrpcUtils.CLIENT_OPTIONS;

@JsType(namespace = "dh")
public class CoreClient extends HasEventHandling {
    public static final String EVENT_CONNECT = "connect",
            EVENT_DISCONNECT = "disconnect",
            EVENT_RECONNECT = "reconnect",
            EVENT_RECONNECT_AUTH_FAILED = "reconnectauthfailed",
            EVENT_REFRESH_TOKEN_UPDATED = "refreshtokenupdated",
            EVENT_REQUEST_FAILED = "requestfailed",
            EVENT_REQUEST_STARTED = "requeststarted",
            EVENT_REQUEST_SUCCEEDED = "requestsucceeded";

    public static final String LOGIN_TYPE_PASSWORD = "password",
            LOGIN_TYPE_ANONYMOUS = "anonymous";

    private final IdeConnection ideConnection;

    public CoreClient(String serverUrl, @TsTypeRef(ConnectOptions.class) @JsOptional Object connectOptions) {
        ideConnection = new IdeConnection(serverUrl, connectOptions, true);

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
        return ideConnection.getConnectOptions().then(options -> {
            BrowserHeaders metadata = new BrowserHeaders();
            JsObject.keys(options.headers).forEach((key, index, arr) -> {
                metadata.set(key, options.headers.get(key));
                return null;
            });
            return getConfigs(
                    // Explicitly creating a new client, and not passing auth details, so this works pre-connection
                    c -> new ConfigServiceClient(getServerUrl(), CLIENT_OPTIONS).getAuthenticationConstants(
                            new AuthenticationConstantsRequest(),
                            metadata,
                            c::apply),
                    AuthenticationConstantsResponse::getConfigValuesMap);
        });
    }

    public Promise<Void> login(@TsTypeRef(LoginCredentials.class) JsPropertyMap<Object> credentials) {
        final LoginCredentials creds;
        if (credentials instanceof LoginCredentials) {
            creds = (LoginCredentials) credentials;
        } else {
            creds = new LoginCredentials(credentials);
        }
        Objects.requireNonNull(creds.getType(), "type must be specified");
        ConnectToken token = ideConnection.getToken();
        if (LOGIN_TYPE_PASSWORD.equals(creds.getType())) {
            Objects.requireNonNull(creds.getUsername(), "username must be specified for password login");
            Objects.requireNonNull(creds.getToken(), "token must be specified for password login");
            token.setType("Basic");
            token.setValue(ConnectToken.bytesToBase64(creds.getUsername() + ":" + creds.getToken()));
        } else if (LOGIN_TYPE_ANONYMOUS.equals(creds.getType())) {
            token.setType("Anonymous");
            token.setValue("");
        } else {
            token.setType(creds.getType());
            token.setValue(creds.getToken());
            if (creds.getUsername() != null) {
                JsLog.warn("username ignored for login type " + creds.getType());
            }
        }

        boolean alreadyRunning = ideConnection.connection.isAvailable();
        WorkerConnection workerConnection = ideConnection.connection.get();
        LazyPromise<Void> loginPromise = new LazyPromise<>();
        ideConnection.addEventListenerOneShot(
                EventPair.of(QueryInfoConstants.EVENT_CONNECT, ignore -> loginPromise.succeed(null)),
                EventPair.of(CoreClient.EVENT_RECONNECT_AUTH_FAILED, loginPromise::fail));
        Promise<Void> login = loginPromise.asPromise();

        // fetch configs and check session timeout
        login.then(ignore -> getServerConfigValues()).then(configs -> {
            for (String[] config : configs) {
                if (config[0].equals("http.session.durationMs")) {
                    workerConnection.setSessionTimeoutMs(Double.parseDouble(config[1]));
                }
            }
            return null;
        }).catch_(ignore -> {
            // Ignore this failure and suppress browser logging, we have a safe fallback
            return Promise.resolve((Object) null);
        });

        if (alreadyRunning) {
            ideConnection.connection.get().forceReconnect();
        }
        return login;
    }

    public Promise<Void> relogin(@TsTypeRef(JsRefreshToken.class) Object token) {
        return login(Js.cast(LoginCredentials.reconnect(JsRefreshToken.fromObject(token).getBytes())));
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
