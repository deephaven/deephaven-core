/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.ide.client;

import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
public class IdeConnectionOptions {
    public String authToken;
    public String serviceId;

    @JsConstructor
    public IdeConnectionOptions() {}
}
