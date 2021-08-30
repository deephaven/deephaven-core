package io.deephaven.javascript.proto.dhinternal.flatbuffers;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(isNative = true, name = "dhinternal.flatbuffers.Table", namespace = JsPackage.GLOBAL)
public interface Table {
    @JsOverlay
    static Table create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    ByteBuffer getBb();

    @JsProperty
    double getBb_pos();

    @JsProperty
    void setBb(ByteBuffer bb);

    @JsProperty
    void setBb_pos(double bb_pos);
}
