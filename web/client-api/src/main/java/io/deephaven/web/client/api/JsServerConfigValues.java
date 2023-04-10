/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import io.deephaven.web.client.api.ServerConfigValues;
import elemental2.core.JsObject;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class JsServerConfigValues {
    private final Map<String, String> valueMap;

    private static final String[] EMPTY_STRING_ARRAY = JsObject.freeze(new String[] {});

    public JsServerConfigValues(String[][] serverConfigValues) {
        valueMap = Arrays.stream(serverConfigValues).collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));
    }

    @JsMethod
    public String getValue(String key) {
        return valueMap.get(key);
    }

    private String[] getStringArray(String key) {
        String str = getValue(key);
        if (str != null) {
            return str.split("[, ]");
        }
        return EMPTY_STRING_ARRAY;
    }

    @JsMethod
    boolean hasValue(String key) {
        return valueMap.containsKey(key);
    }

    @JsMethod
    private boolean getBooleanValue(String key) {
        return Boolean.parseBoolean(getValue(key));
    }

    @JsProperty
    public String getGradleVersion() {
        return getValue(ServerConfigValues.GRADLE_VERSION);
    }

    @JsProperty
    public String getVcsVersion() {
        return getValue(ServerConfigValues.VCS_VERSION);
    }

    @JsProperty
    public String getJavaVersion() {
        return getValue(ServerConfigValues.JAVA_VERSION);
    }
}
