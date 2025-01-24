//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.diagnostic;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.Diagnostic.DiagnosticTagMap",
        namespace = JsPackage.GLOBAL)
public interface DiagnosticTagMap {
    @JsOverlay
    static DiagnosticTagMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "DEPRECATED")
    int getDEPRECATED();

    @JsProperty(name = "NOT_SET_TAG")
    int getNOT_SET_TAG();

    @JsProperty(name = "UNNECESSARY")
    int getUNNECESSARY();

    @JsProperty(name = "DEPRECATED")
    void setDEPRECATED(int DEPRECATED);

    @JsProperty(name = "NOT_SET_TAG")
    void setNOT_SET_TAG(int NOT_SET_TAG);

    @JsProperty(name = "UNNECESSARY")
    void setUNNECESSARY(int UNNECESSARY);
}
