//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.data;

import elemental2.core.JsString;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class ConnectToken implements Serializable {
    private String type;
    private String value;

    @JsMethod(namespace = JsPackage.GLOBAL)
    private static native String atob(String encodedData);

    @JsMethod(namespace = JsPackage.GLOBAL)
    private static native String btoa(String stringToEncode);

    public static String bytesToBase64(String str) {
        return bytesToBase64(str.getBytes(StandardCharsets.UTF_8));
    }

    public static String bytesToBase64(byte[] bytes) {
        String stringOfBytes = "";
        for (int i = 0; i < bytes.length; i++) {
            stringOfBytes += JsString.fromCharCode(bytes[i] & 0xff);
        }
        return btoa(stringOfBytes);
    }

    public static byte[] base64ToBytes(String base64) {
        return atob(base64).getBytes(StandardCharsets.ISO_8859_1);
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
