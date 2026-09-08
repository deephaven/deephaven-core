//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import jsinterop.annotations.JsType;

/**
 * Event name constants used by {@code dh.QueryInfo}.
 */
@JsType(name = "QueryInfo", namespace = "dh")
public class QueryInfoConstants {
    /**
     * Fired when a table is opened.
     */
    public static final String EVENT_TABLE_OPENED = "tableopened";

    /**
     * Fired when the client disconnects.
     */
    public static final String EVENT_DISCONNECT = "disconnect";

    /**
     * Fired when the client reconnects.
     */
    public static final String EVENT_RECONNECT = "reconnect";

    /**
     * Fired when the client connects.
     */
    public static final String EVENT_CONNECT = "connect";

    private QueryInfoConstants() {

    }
}
