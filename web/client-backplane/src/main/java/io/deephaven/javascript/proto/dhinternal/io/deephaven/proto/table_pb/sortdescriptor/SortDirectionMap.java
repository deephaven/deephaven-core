package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.sortdescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.SortDescriptor.SortDirectionMap",
    namespace = JsPackage.GLOBAL)
public interface SortDirectionMap {
    @JsOverlay
    static SortDirectionMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "ASCENDING")
    double getASCENDING();

    @JsProperty(name = "DESCENDING")
    double getDESCENDING();

    @JsProperty(name = "REVERSE")
    double getREVERSE();

    @JsProperty(name = "UNKNOWN")
    double getUNKNOWN();

    @JsProperty(name = "ASCENDING")
    void setASCENDING(double ASCENDING);

    @JsProperty(name = "DESCENDING")
    void setDESCENDING(double DESCENDING);

    @JsProperty(name = "REVERSE")
    void setREVERSE(double REVERSE);

    @JsProperty(name = "UNKNOWN")
    void setUNKNOWN(double UNKNOWN);
}
