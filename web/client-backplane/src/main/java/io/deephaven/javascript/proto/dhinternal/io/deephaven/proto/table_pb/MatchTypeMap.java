package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.MatchTypeMap",
    namespace = JsPackage.GLOBAL)
public interface MatchTypeMap {
    @JsOverlay
    static MatchTypeMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "INVERTED")
    double getINVERTED();

    @JsProperty(name = "REGULAR")
    double getREGULAR();

    @JsProperty(name = "INVERTED")
    void setINVERTED(double INVERTED);

    @JsProperty(name = "REGULAR")
    void setREGULAR(double REGULAR);
}
