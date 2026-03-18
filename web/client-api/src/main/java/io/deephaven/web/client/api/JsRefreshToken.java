//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsDate;
import jsinterop.annotations.JsProperty;
import jsinterop.base.JsPropertyMap;

@TsInterface
@TsName(name = "RefreshToken", namespace = "dh")
/**
 * A refresh token and its expiration time.
 */
public class JsRefreshToken {

    public static JsRefreshToken fromObject(Object token) {
        if (token instanceof JsRefreshToken) {
            return (JsRefreshToken) token;
        }
        return new JsRefreshToken((JsPropertyMap<Object>) token);
    }

    /**
     * The refresh token bytes.
     */
    private String bytes;
    /**
     * The token expiry time.
     */
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

    /**
     * The refresh token bytes.
     */
    @JsProperty
    public String getBytes() {
        return bytes;
    }

    /**
     * The token expiry time, as milliseconds since the epoch.
     */
    @JsProperty
    public double getExpiry() {
        return expiry;
    }
}
