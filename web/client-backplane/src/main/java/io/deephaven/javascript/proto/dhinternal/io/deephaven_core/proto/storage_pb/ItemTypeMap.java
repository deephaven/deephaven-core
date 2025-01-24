//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.storage_pb.ItemTypeMap",
        namespace = JsPackage.GLOBAL)
public interface ItemTypeMap {
    @JsOverlay
    static ItemTypeMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "DIRECTORY")
    int getDIRECTORY();

    @JsProperty(name = "FILE")
    int getFILE();

    @JsProperty(name = "UNKNOWN")
    int getUNKNOWN();

    @JsProperty(name = "DIRECTORY")
    void setDIRECTORY(int DIRECTORY);

    @JsProperty(name = "FILE")
    void setFILE(int FILE);

    @JsProperty(name = "UNKNOWN")
    void setUNKNOWN(int UNKNOWN);
}
