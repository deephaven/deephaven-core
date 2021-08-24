package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.exportnotification;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.session_pb.ExportNotification.StateMap",
    namespace = JsPackage.GLOBAL)
public interface StateMap {
    @JsOverlay
    static StateMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "CANCELLED")
    double getCANCELLED();

    @JsProperty(name = "DEPENDENCY_CANCELLED")
    double getDEPENDENCY_CANCELLED();

    @JsProperty(name = "DEPENDENCY_FAILED")
    double getDEPENDENCY_FAILED();

    @JsProperty(name = "DEPENDENCY_NEVER_FOUND")
    double getDEPENDENCY_NEVER_FOUND();

    @JsProperty(name = "DEPENDENCY_RELEASED")
    double getDEPENDENCY_RELEASED();

    @JsProperty(name = "EXPORTED")
    double getEXPORTED();

    @JsProperty(name = "FAILED")
    double getFAILED();

    @JsProperty(name = "PENDING")
    double getPENDING();

    @JsProperty(name = "PUBLISHING")
    double getPUBLISHING();

    @JsProperty(name = "QUEUED")
    double getQUEUED();

    @JsProperty(name = "RELEASED")
    double getRELEASED();

    @JsProperty(name = "UNKNOWN")
    double getUNKNOWN();

    @JsProperty(name = "CANCELLED")
    void setCANCELLED(double CANCELLED);

    @JsProperty(name = "DEPENDENCY_CANCELLED")
    void setDEPENDENCY_CANCELLED(double DEPENDENCY_CANCELLED);

    @JsProperty(name = "DEPENDENCY_FAILED")
    void setDEPENDENCY_FAILED(double DEPENDENCY_FAILED);

    @JsProperty(name = "DEPENDENCY_NEVER_FOUND")
    void setDEPENDENCY_NEVER_FOUND(double DEPENDENCY_NEVER_FOUND);

    @JsProperty(name = "DEPENDENCY_RELEASED")
    void setDEPENDENCY_RELEASED(double DEPENDENCY_RELEASED);

    @JsProperty(name = "EXPORTED")
    void setEXPORTED(double EXPORTED);

    @JsProperty(name = "FAILED")
    void setFAILED(double FAILED);

    @JsProperty(name = "PENDING")
    void setPENDING(double PENDING);

    @JsProperty(name = "PUBLISHING")
    void setPUBLISHING(double PUBLISHING);

    @JsProperty(name = "QUEUED")
    void setQUEUED(double QUEUED);

    @JsProperty(name = "RELEASED")
    void setRELEASED(double RELEASED);

    @JsProperty(name = "UNKNOWN")
    void setUNKNOWN(double UNKNOWN);
}
