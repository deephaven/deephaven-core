//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.mathcontext;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.MathContext.RoundingModeMap",
        namespace = JsPackage.GLOBAL)
public interface RoundingModeMap {
    @JsOverlay
    static RoundingModeMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "CEILING")
    int getCEILING();

    @JsProperty(name = "DOWN")
    int getDOWN();

    @JsProperty(name = "FLOOR")
    int getFLOOR();

    @JsProperty(name = "HALF_DOWN")
    int getHALF_DOWN();

    @JsProperty(name = "HALF_EVEN")
    int getHALF_EVEN();

    @JsProperty(name = "HALF_UP")
    int getHALF_UP();

    @JsProperty(name = "UNNECESSARY")
    int getUNNECESSARY();

    @JsProperty(name = "UP")
    int getUP();

    @JsProperty(name = "CEILING")
    void setCEILING(int CEILING);

    @JsProperty(name = "DOWN")
    void setDOWN(int DOWN);

    @JsProperty(name = "FLOOR")
    void setFLOOR(int FLOOR);

    @JsProperty(name = "HALF_DOWN")
    void setHALF_DOWN(int HALF_DOWN);

    @JsProperty(name = "HALF_EVEN")
    void setHALF_EVEN(int HALF_EVEN);

    @JsProperty(name = "HALF_UP")
    void setHALF_UP(int HALF_UP);

    @JsProperty(name = "UNNECESSARY")
    void setUNNECESSARY(int UNNECESSARY);

    @JsProperty(name = "UP")
    void setUP(int UP);
}
