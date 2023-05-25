package io.deephaven.web.client.api;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

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
