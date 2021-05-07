package io.deephaven.web.shared.ide;

import io.deephaven.web.shared.data.ConnectToken;

import java.io.Serializable;

/**
 * Sent from the server to the client so the client can figure out where to connect to the worker.
 */

public class ConsoleAddress implements Serializable {
    private ConnectToken token;
    private String websocketUrl;
    private String serviceId;

    public void setToken(ConnectToken token) {
        this.token = token;
    }

    public ConnectToken getToken() {
        return token;
    }

    public void setWebsocketUrl(String websocketUrl) {
        this.websocketUrl = websocketUrl;
    }

    public String getWebsocketUrl() {
        return websocketUrl;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getServiceId() {
        return serviceId;
    }
}
