//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

@JsType(namespace = "dh")
public class LoginCredentials {

    @JsIgnore
    public static LoginCredentials reconnect(String token) {
        LoginCredentials loginCredentials = new LoginCredentials();
        loginCredentials.setType("Bearer");
        loginCredentials.setToken(token);
        return loginCredentials;
    }

    private String username;
    private String token;
    private String type;

    @JsConstructor
    public LoginCredentials() {}

    @JsIgnore
    public LoginCredentials(JsPropertyMap<Object> source) {
        this();
        if (source.has("username")) {
            username = source.getAsAny("username").asString();
        }
        if (source.has("token")) {
            token = source.getAsAny("token").asString();
        }
        if (source.has("type")) {
            type = source.getAsAny("type").asString();
        }
    }

    @JsProperty
    @JsNullable
    public final String getUsername() {
        return username;
    }

    @JsProperty
    public final void setUsername(String username) {
        this.username = username;
    }

    @JsProperty
    @JsNullable
    public final String getToken() {
        return token;
    }

    @JsProperty
    public final void setToken(String token) {
        this.token = token;
    }

    @JsProperty
    @JsNullable
    public final String getType() {
        return type;
    }

    @JsProperty
    public final void setType(String type) {
        this.type = type;
    }

}
