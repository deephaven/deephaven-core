package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.NullValueMap",
        namespace = JsPackage.GLOBAL)
public interface NullValueMap {
    @JsOverlay
    static NullValueMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "NULL_VALUE")
    double getNULL_VALUE();

    @JsProperty(name = "NULL_VALUE")
    void setNULL_VALUE(double NULL_VALUE);
}
