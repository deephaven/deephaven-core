package io.deephaven.ide.client;

import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
public class IdeConnectionOptions {
    private String authToken;
    private String serviceId;

    @JsConstructor
    public IdeConnectionOptions() {
    }

    @JsProperty
    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    @JsProperty
    public String getServiceId() {
        return serviceId;
    }

    @JsProperty
    public String getAuthToken() {
        return authToken;
    }

    @JsProperty
    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }
}
