package io.deephaven.javascript.proto.dhinternal.jspb.map;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(isNative = true, name = "dhinternal.jspb.Map.IteratorResult", namespace = JsPackage.GLOBAL)
public interface IteratorResult<T> {
    @JsOverlay
    static IteratorResult create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    T getValue();

    @JsProperty
    boolean isDone();

    @JsProperty
    void setDone(boolean done);

    @JsProperty
    void setValue(T value);
}
