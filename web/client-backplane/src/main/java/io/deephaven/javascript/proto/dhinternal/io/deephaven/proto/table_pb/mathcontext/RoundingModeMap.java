/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.mathcontext;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.MathContext.RoundingModeMap",
        namespace = JsPackage.GLOBAL)
public interface RoundingModeMap {
    @JsOverlay
    static RoundingModeMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "CEILING")
    double getCEILING();

    @JsProperty(name = "DOWN")
    double getDOWN();

    @JsProperty(name = "FLOOR")
    double getFLOOR();

    @JsProperty(name = "HALF_DOWN")
    double getHALF_DOWN();

    @JsProperty(name = "HALF_EVEN")
    double getHALF_EVEN();

    @JsProperty(name = "HALF_UP")
    double getHALF_UP();

    @JsProperty(name = "UNNECESSARY")
    double getUNNECESSARY();

    @JsProperty(name = "UP")
    double getUP();

    @JsProperty(name = "CEILING")
    void setCEILING(double CEILING);

    @JsProperty(name = "DOWN")
    void setDOWN(double DOWN);

    @JsProperty(name = "FLOOR")
    void setFLOOR(double FLOOR);

    @JsProperty(name = "HALF_DOWN")
    void setHALF_DOWN(double HALF_DOWN);

    @JsProperty(name = "HALF_EVEN")
    void setHALF_EVEN(double HALF_EVEN);

    @JsProperty(name = "HALF_UP")
    void setHALF_UP(double HALF_UP);

    @JsProperty(name = "UNNECESSARY")
    void setUNNECESSARY(double UNNECESSARY);

    @JsProperty(name = "UP")
    void setUP(double UP);
}
