//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import elemental2.promise.Promise;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc;
import io.deephaven.proto.backplane.grpc.ConfigValue;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse;
import io.deephaven.web.client.api.event.HasEventHandling;
import io.deephaven.web.client.api.storage.JsStorageService;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.client.ide.IdeConnection;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.fu.JsFunction;
import io.grpc.stub.StreamObserver;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

@JsType(namespace = "dh")
public class CoreClient extends HasEventHandling {
    public static final String EVENT_CONNECT = "connect",
            EVENT_DISCONNECT = "disconnect",
            EVENT_RECONNECT = "reconnect",
            EVENT_RECONNECT_AUTH_FAILED = "reconnectauthfailed",
            EVENT_REQUEST_FAILED = "requestfailed",
            EVENT_REQUEST_STARTED = "requeststarted",
            EVENT_REQUEST_SUCCEEDED = "requestsucceeded";

    @Deprecated
    public static final String EVENT_REFRESH_TOKEN_UPDATED = "refreshtokenupdated";
    public static final String LOGIN_TYPE_PASSWORD = "password",
            LOGIN_TYPE_ANONYMOUS = "anonymous";

    private final IdeConnection ideConnection;

    public CoreClient(String serverUrl,
            @TsTypeRef(ConnectOptions.class) @JsOptional @JsNullable Object connectOptions) {
        ideConnection = new IdeConnection(serverUrl, connectOptions);
    }

    private <R> Promise<String[][]> getConfigs(Consumer<StreamObserver<R>> rpcCall,
            JsFunction<R, Map<String, ConfigValue>> getConfigValues) {
        return Callbacks.grpcUnaryPromise(rpcCall).then(response -> {
            String[][] result = new String[0][];
            getConfigValues.apply(response).forEach((key, value) -> {
                result[result.length] = new String[] {key, value.getStringValue()};
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
        ConfigServiceGrpc.ConfigServiceStub configService = ideConnection.createStubNoAuth(ConfigServiceGrpc::newStub);
        return this.getConfigs(
                // Explicitly creating a new client, and not passing auth details, so this works pre-connection
                c -> configService.getAuthenticationConstants(
                        AuthenticationConstantsRequest.getDefaultInstance(), c),
                AuthenticationConstantsResponse::getConfigValuesMap);
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
        ideConnection.login(token.getType(), token.getValue());

        boolean alreadyRunning = ideConnection.connection.isAvailable();
        WorkerConnection workerConnection = ideConnection.connection.get();
        LazyPromise<Void> loginPromise = new LazyPromise<>();
        ideConnection.addEventListenerOneShot(
                EventPair.of(QueryInfoConstants.EVENT_CONNECT, ignore -> loginPromise.succeed(null)),
                EventPair.of(CoreClient.EVENT_DISCONNECT, loginPromise::fail),
                EventPair.of(CoreClient.EVENT_RECONNECT_AUTH_FAILED, loginPromise::fail),
                EventPair.of(CoreClient.EVENT_REQUEST_FAILED, loginPromise::fail));
        Promise<Void> login = loginPromise.asPromise();

        if (alreadyRunning) {
            ideConnection.connection.get().forceReconnect();
        }
        return login;
    }

    public Promise<Void> relogin(@TsTypeRef(JsRefreshToken.class) Object token) {
        return login(Js.cast(LoginCredentials.reconnect(JsRefreshToken.fromObject(token).getBytes())));
    }

    /**
     * Resolves when the client is connected.
     *
     * @param timeoutInMillis ignored, only exists for legacy callers
     * @return a promise that resolves when connection is established, or rejects if connection fails
     */
    public Promise<Void> onConnected(@JsOptional @JsNullable Double timeoutInMillis) {
        return ideConnection.onConnected();
    }

    public Promise<String[][]> getServerConfigValues() {
        return getConfigs(
                c -> ideConnection.connection.get().configServiceClient().getConfigurationConstants(
                        ConfigurationConstantsRequest.getDefaultInstance(),
                        c),
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
