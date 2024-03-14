//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsDate;
import jsinterop.annotations.JsProperty;
import jsinterop.base.JsPropertyMap;

@TsInterface
@TsName(name = "RefreshToken", namespace = "dh")
public class JsRefreshToken {

    public static JsRefreshToken fromObject(Object token) {
        if (token instanceof JsRefreshToken) {
            return (JsRefreshToken) token;
        }
        return new JsRefreshToken((JsPropertyMap<Object>) token);
    }

    private String bytes;
    private double expiry;

    public JsRefreshToken(String token, double tokenDuration) {
        this.bytes = token;
        this.expiry = JsDate.now() + tokenDuration;
    }

    private JsRefreshToken(JsPropertyMap<Object> source) {
        if (source.has("bytes")) {
            bytes = source.getAsAny("bytes").asString();
        }
        if (source.has("expiry")) {
            expiry = source.getAsAny("expiry").asDouble();
        }
    }

    @JsProperty
    public String getBytes() {
        return bytes;
    }

    @JsProperty
    public double getExpiry() {
        return expiry;
    }
}
