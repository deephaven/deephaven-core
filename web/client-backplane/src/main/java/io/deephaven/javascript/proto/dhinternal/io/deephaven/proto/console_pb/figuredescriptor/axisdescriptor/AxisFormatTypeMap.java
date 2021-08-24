package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.AxisDescriptor.AxisFormatTypeMap",
    namespace = JsPackage.GLOBAL)
public interface AxisFormatTypeMap {
    @JsOverlay
    static AxisFormatTypeMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "CATEGORY")
    int getCATEGORY();

    @JsProperty(name = "NUMBER")
    int getNUMBER();

    @JsProperty(name = "CATEGORY")
    void setCATEGORY(int CATEGORY);

    @JsProperty(name = "NUMBER")
    void setNUMBER(int NUMBER);
}
