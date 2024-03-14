//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import jsinterop.annotations.JsType;

@JsType(name = "QueryInfo", namespace = "dh")
public class QueryInfoConstants {
    public static final String EVENT_TABLE_OPENED = "tableopened";
    public static final String EVENT_DISCONNECT = "disconnect";
    public static final String EVENT_RECONNECT = "reconnect";
    public static final String EVENT_CONNECT = "connect";

    private QueryInfoConstants() {

    }
}
