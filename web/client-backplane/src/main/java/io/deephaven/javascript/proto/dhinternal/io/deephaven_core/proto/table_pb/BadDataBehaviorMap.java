//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.BadDataBehaviorMap",
        namespace = JsPackage.GLOBAL)
public interface BadDataBehaviorMap {
    @JsOverlay
    static BadDataBehaviorMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "POISON")
    int getPOISON();

    @JsProperty(name = "RESET")
    int getRESET();

    @JsProperty(name = "SKIP")
    int getSKIP();

    @JsProperty(name = "THROW")
    int getTHROW();

    @JsProperty(name = "POISON")
    void setPOISON(int POISON);

    @JsProperty(name = "RESET")
    void setRESET(int RESET);

    @JsProperty(name = "SKIP")
    void setSKIP(int SKIP);

    @JsProperty(name = "THROW")
    void setTHROW(int THROW);
}
