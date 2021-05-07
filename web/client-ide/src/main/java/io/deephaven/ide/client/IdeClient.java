package io.deephaven.ide.client;

import elemental2.promise.Promise;
import io.deephaven.ide.shared.IdeSession;
import io.deephaven.web.client.fu.CancellablePromise;
import io.deephaven.web.shared.data.ConnectToken;
import io.deephaven.web.shared.ide.ConsoleAddress;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

import java.nio.charset.StandardCharsets;

/**
 */
@JsType(name = "Ide", namespace = "dh")
public class IdeClient {
    @JsMethod(namespace = JsPackage.GLOBAL)
    private static native String atob(String encodedData);

    @Deprecated
    @JsMethod(name = "getExistingSession")
    public Promise<IdeSession> getExistingSession_old(String websocketUrl, String authToken, String serviceId, String language) {
        return IdeClient.getExistingSession(websocketUrl, authToken, serviceId, language);
    }

    public static CancellablePromise<IdeSession> getExistingSession(String websocketUrl, String authToken, String serviceId, String language) {
        ConnectToken token = null;
        if (authToken != null) {
            token = new ConnectToken();
            token.setBytes(atob(authToken).getBytes(StandardCharsets.ISO_8859_1));
        }

        ConsoleAddress address = new ConsoleAddress();
        address.setWebsocketUrl(websocketUrl);
        address.setServiceId(serviceId);
        address.setToken(token);

        IdeConnection ideConnection = new IdeConnection(address);
        return ideConnection.startSession(language);
    }
}
