package io.deephaven.ide.client;

import jsinterop.annotations.JsConstructor;

public class IdeConnectionOptions {
    private String authToken;
    private String serviceId;

    @JsConstructor
    public IdeConnectionOptions() {
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }
}
