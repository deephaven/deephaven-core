package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.CaseSensitivityMap",
    namespace = JsPackage.GLOBAL)
public interface CaseSensitivityMap {
    @JsOverlay
    static CaseSensitivityMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "IGNORE_CASE")
    double getIGNORE_CASE();

    @JsProperty(name = "MATCH_CASE")
    double getMATCH_CASE();

    @JsProperty(name = "IGNORE_CASE")
    void setIGNORE_CASE(double IGNORE_CASE);

    @JsProperty(name = "MATCH_CASE")
    void setMATCH_CASE(double MATCH_CASE);
}
