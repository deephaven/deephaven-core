package io.deephaven.ide.client;

import elemental2.promise.Promise;
import io.deephaven.ide.shared.IdeSession;
import io.deephaven.web.client.fu.CancellablePromise;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;

/**
 */
@JsType(name = "Ide", namespace = "dh")
public class IdeClient {
    @Deprecated
    @JsMethod(name = "getExistingSession")
    public Promise<IdeSession> getExistingSession_old(String websocketUrl, String authToken, String serviceId, String language) {
        return IdeClient.getExistingSession(websocketUrl, authToken, serviceId, language);
    }

    public static CancellablePromise<IdeSession> getExistingSession(String websocketUrl, String authToken, String serviceId, String language) {
        IdeConnectionOptions options = new IdeConnectionOptions();
        options.authToken = authToken;
        options.serviceId = serviceId;

        IdeConnection ideConnection = new IdeConnection(websocketUrl, options);
        return ideConnection.startSession(language);
    }
}
