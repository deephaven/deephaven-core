package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.figuredescriptor.axisdescriptor;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.FigureDescriptor.AxisDescriptor.AxisPositionMap",
    namespace = JsPackage.GLOBAL)
public interface AxisPositionMap {
    @JsOverlay
    static AxisPositionMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "BOTTOM")
    int getBOTTOM();

    @JsProperty(name = "LEFT")
    int getLEFT();

    @JsProperty(name = "NONE")
    int getNONE();

    @JsProperty(name = "RIGHT")
    int getRIGHT();

    @JsProperty(name = "TOP")
    int getTOP();

    @JsProperty(name = "BOTTOM")
    void setBOTTOM(int BOTTOM);

    @JsProperty(name = "LEFT")
    void setLEFT(int LEFT);

    @JsProperty(name = "NONE")
    void setNONE(int NONE);

    @JsProperty(name = "RIGHT")
    void setRIGHT(int RIGHT);

    @JsProperty(name = "TOP")
    void setTOP(int TOP);
}
