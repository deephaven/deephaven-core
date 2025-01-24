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
        name = "dhinternal.io.deephaven_core.proto.console_pb.Diagnostic.DiagnosticSeverityMap",
        namespace = JsPackage.GLOBAL)
public interface DiagnosticSeverityMap {
    @JsOverlay
    static DiagnosticSeverityMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "ERROR")
    int getERROR();

    @JsProperty(name = "HINT")
    int getHINT();

    @JsProperty(name = "INFORMATION")
    int getINFORMATION();

    @JsProperty(name = "NOT_SET_SEVERITY")
    int getNOT_SET_SEVERITY();

    @JsProperty(name = "WARNING")
    int getWARNING();

    @JsProperty(name = "ERROR")
    void setERROR(int ERROR);

    @JsProperty(name = "HINT")
    void setHINT(int HINT);

    @JsProperty(name = "INFORMATION")
    void setINFORMATION(int INFORMATION);

    @JsProperty(name = "NOT_SET_SEVERITY")
    void setNOT_SET_SEVERITY(int NOT_SET_SEVERITY);

    @JsProperty(name = "WARNING")
    void setWARNING(int WARNING);
}
