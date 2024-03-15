//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import jsinterop.annotations.JsType;

/**
 * Exists to keep the dh.TableMap namespace so that the web UI can remain compatible with the DHE API, which still calls
 * this type TableMap.
 */
@Deprecated
@JsType(namespace = "dh", name = "TableMap")
public class TableMapEventLegacyNamespace {
    public static final String EVENT_KEYADDED = "keyadded",
            EVENT_DISCONNECT = JsTable.EVENT_DISCONNECT,
            EVENT_RECONNECT = JsTable.EVENT_RECONNECT,
            EVENT_RECONNECTFAILED = JsTable.EVENT_RECONNECTFAILED;

    private TableMapEventLegacyNamespace() {

    }
}
