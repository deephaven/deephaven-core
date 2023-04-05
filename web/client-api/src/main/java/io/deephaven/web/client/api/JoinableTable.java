package io.deephaven.web.client.api;

import io.deephaven.web.client.state.ClientTableState;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
public interface JoinableTable {
    @JsIgnore
    ClientTableState state();
}
