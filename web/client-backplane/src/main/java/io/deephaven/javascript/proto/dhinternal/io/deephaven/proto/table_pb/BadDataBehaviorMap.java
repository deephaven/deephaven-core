/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.BadDataBehaviorMap",
        namespace = JsPackage.GLOBAL)
public interface BadDataBehaviorMap {
    @JsOverlay
    static BadDataBehaviorMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "BAD_DATA_BEHAVIOR_NOT_SPECIFIED")
    double getBAD_DATA_BEHAVIOR_NOT_SPECIFIED();

    @JsProperty(name = "POISON")
    int getPOISON();

    @JsProperty(name = "RESET")
    int getRESET();

    @JsProperty(name = "SKIP")
    int getSKIP();

    @JsProperty(name = "THROW")
    int getTHROW();

    @JsProperty(name = "BAD_DATA_BEHAVIOR_NOT_SPECIFIED")
    void setBAD_DATA_BEHAVIOR_NOT_SPECIFIED(double BAD_DATA_BEHAVIOR_NOT_SPECIFIED);

    @JsProperty(name = "POISON")
    void setPOISON(int POISON);

    @JsProperty(name = "RESET")
    void setRESET(int RESET);

    @JsProperty(name = "SKIP")
    void setSKIP(int SKIP);

    @JsProperty(name = "THROW")
    void setTHROW(int THROW);
}
