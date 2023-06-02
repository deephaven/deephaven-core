package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.rangejointablesrequest;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.RangeJoinTablesRequest.RangeStartRuleMap",
        namespace = JsPackage.GLOBAL)
public interface RangeStartRuleMap {
    @JsOverlay
    static RangeStartRuleMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "LESS_THAN")
    double getLESS_THAN();

    @JsProperty(name = "LESS_THAN_OR_EQUAL")
    double getLESS_THAN_OR_EQUAL();

    @JsProperty(name = "LESS_THAN_OR_EQUAL_ALLOW_PRECEDING")
    double getLESS_THAN_OR_EQUAL_ALLOW_PRECEDING();

    @JsProperty(name = "START_UNSPECIFIED")
    double getSTART_UNSPECIFIED();

    @JsProperty(name = "LESS_THAN")
    void setLESS_THAN(double LESS_THAN);

    @JsProperty(name = "LESS_THAN_OR_EQUAL")
    void setLESS_THAN_OR_EQUAL(double LESS_THAN_OR_EQUAL);

    @JsProperty(name = "LESS_THAN_OR_EQUAL_ALLOW_PRECEDING")
    void setLESS_THAN_OR_EQUAL_ALLOW_PRECEDING(double LESS_THAN_OR_EQUAL_ALLOW_PRECEDING);

    @JsProperty(name = "START_UNSPECIFIED")
    void setSTART_UNSPECIFIED(double START_UNSPECIFIED);
}
