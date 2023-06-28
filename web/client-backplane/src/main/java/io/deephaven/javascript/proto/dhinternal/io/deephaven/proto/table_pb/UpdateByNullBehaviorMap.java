package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByNullBehaviorMap",
        namespace = JsPackage.GLOBAL)
public interface UpdateByNullBehaviorMap {
    @JsOverlay
    static UpdateByNullBehaviorMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "NULL_BEHAVIOR_NOT_SPECIFIED")
    double getNULL_BEHAVIOR_NOT_SPECIFIED();

    @JsProperty(name = "NULL_DOMINATES")
    double getNULL_DOMINATES();

    @JsProperty(name = "VALUE_DOMINATES")
    double getVALUE_DOMINATES();

    @JsProperty(name = "ZERO_DOMINATES")
    double getZERO_DOMINATES();

    @JsProperty(name = "NULL_BEHAVIOR_NOT_SPECIFIED")
    void setNULL_BEHAVIOR_NOT_SPECIFIED(double NULL_BEHAVIOR_NOT_SPECIFIED);

    @JsProperty(name = "NULL_DOMINATES")
    void setNULL_DOMINATES(double NULL_DOMINATES);

    @JsProperty(name = "VALUE_DOMINATES")
    void setVALUE_DOMINATES(double VALUE_DOMINATES);

    @JsProperty(name = "ZERO_DOMINATES")
    void setZERO_DOMINATES(double ZERO_DOMINATES);
}
