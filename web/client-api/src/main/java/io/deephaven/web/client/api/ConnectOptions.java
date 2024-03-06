//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

/**
 * Presently optional and not used by the server, this allows the client to specify some authentication details. String
 * authToken <i>- base 64 encoded auth token. String serviceId -</i> The service ID to use for the connection.
 */
@JsType(namespace = "dh")
public class ConnectOptions {
    public JsPropertyMap<String> headers = Js.uncheckedCast(JsPropertyMap.of());

    public ConnectOptions() {

    }

    @JsIgnore
    public ConnectOptions(Object connectOptions) {
        this();
        JsPropertyMap<Object> map = Js.asPropertyMap(connectOptions);
        headers = Js.uncheckedCast(map.getAsAny("headers").asPropertyMap());
    }
}
