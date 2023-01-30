package io.deephaven.web.client.api;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(namespace = JsPackage.GLOBAL, name = "Object", isNative = true)
public class LoginCredentials {

    @JsOverlay
    public static LoginCredentials reconnect(String token) {
        LoginCredentials loginCredentials = new LoginCredentials();
        loginCredentials.setType("Bearer");
        loginCredentials.setToken(token);
        return loginCredentials;
    }

    public String username, token, type;

    @JsOverlay
    public final String getUsername() {
        return username;
    }

    @JsOverlay
    public final void setUsername(String username) {
        this.username = username;
    }

    @JsOverlay
    public final String getToken() {
        return token;
    }

    @JsOverlay
    public final void setToken(String token) {
        this.token = token;
    }

    @JsOverlay
    public final String getType() {
        return type;
    }

    @JsOverlay
    public final void setType(String type) {
        this.type = type;
    }

}
