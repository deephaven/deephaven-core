//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import elemental2.promise.Promise;
import io.deephaven.web.client.fu.CancellablePromise;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;

/**
 */
@JsType(name = "Ide", namespace = "dh")
public class IdeClient {
    @Deprecated
    @JsMethod(name = "getExistingSession")
    public Promise<IdeSession> getExistingSession_old(String websocketUrl, String authToken, String serviceId,
            String language) {
        return IdeClient.getExistingSession(websocketUrl, authToken, serviceId, language);
    }

    @Deprecated
    public static CancellablePromise<IdeSession> getExistingSession(String websocketUrl, String authToken,
            String serviceId, String language) {
        IdeConnection ideConnection = new IdeConnection(websocketUrl, null, true);
        return ideConnection.startSession(language);
    }
}
