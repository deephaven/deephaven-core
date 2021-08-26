package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.asofjointablesrequest;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.AsOfJoinTablesRequest.MatchRuleMap",
        namespace = JsPackage.GLOBAL)
public interface MatchRuleMap {
    @JsOverlay
    static MatchRuleMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "GREATER_THAN")
    double getGREATER_THAN();

    @JsProperty(name = "GREATER_THAN_EQUAL")
    double getGREATER_THAN_EQUAL();

    @JsProperty(name = "LESS_THAN")
    double getLESS_THAN();

    @JsProperty(name = "LESS_THAN_EQUAL")
    double getLESS_THAN_EQUAL();

    @JsProperty(name = "GREATER_THAN")
    void setGREATER_THAN(double GREATER_THAN);

    @JsProperty(name = "GREATER_THAN_EQUAL")
    void setGREATER_THAN_EQUAL(double GREATER_THAN_EQUAL);

    @JsProperty(name = "LESS_THAN")
    void setLESS_THAN(double LESS_THAN);

    @JsProperty(name = "LESS_THAN_EQUAL")
    void setLESS_THAN_EQUAL(double LESS_THAN_EQUAL);
}
