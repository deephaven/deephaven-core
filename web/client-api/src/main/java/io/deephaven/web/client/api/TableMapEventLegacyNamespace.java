/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import jsinterop.annotations.JsProperty;

/**
 * Exists to keep the dh.TableMap namespace so that the web UI can remain compatible with the DHE API, which still calls
 * this type TableMap.
 */
public class TableMapEventLegacyNamespace {
    @JsProperty(namespace = "dh.TableMap")

    public static final String EVENT_KEYADDED = "keyadded",
            EVENT_DISCONNECT = JsTable.EVENT_DISCONNECT,
            EVENT_RECONNECT = JsTable.EVENT_RECONNECT,
            EVENT_RECONNECTFAILED = JsTable.EVENT_RECONNECTFAILED;

}
