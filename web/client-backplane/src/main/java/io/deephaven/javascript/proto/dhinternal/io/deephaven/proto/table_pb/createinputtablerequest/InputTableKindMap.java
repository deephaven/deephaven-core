package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.createinputtablerequest;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.CreateInputTableRequest.InputTableKindMap",
        namespace = JsPackage.GLOBAL)
public interface InputTableKindMap {
    @JsOverlay
    static InputTableKindMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "APPEND_ONLY")
    double getAPPEND_ONLY();

    @JsProperty(name = "KEYED_ARRAY_BACKED")
    double getKEYED_ARRAY_BACKED();

    @JsProperty(name = "APPEND_ONLY")
    void setAPPEND_ONLY(double APPEND_ONLY);

    @JsProperty(name = "KEYED_ARRAY_BACKED")
    void setKEYED_ARRAY_BACKED(double KEYED_ARRAY_BACKED);
}
