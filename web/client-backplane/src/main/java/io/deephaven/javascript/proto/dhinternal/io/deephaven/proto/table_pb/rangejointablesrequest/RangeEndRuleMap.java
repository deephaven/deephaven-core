package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.rangejointablesrequest;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.RangeJoinTablesRequest.RangeEndRuleMap",
        namespace = JsPackage.GLOBAL)
public interface RangeEndRuleMap {
    @JsOverlay
    static RangeEndRuleMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "END_UNSPECIFIED")
    double getEND_UNSPECIFIED();

    @JsProperty(name = "GREATER_THAN")
    double getGREATER_THAN();

    @JsProperty(name = "GREATER_THAN_OR_EQUAL")
    double getGREATER_THAN_OR_EQUAL();

    @JsProperty(name = "GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING")
    double getGREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING();

    @JsProperty(name = "END_UNSPECIFIED")
    void setEND_UNSPECIFIED(double END_UNSPECIFIED);

    @JsProperty(name = "GREATER_THAN")
    void setGREATER_THAN(double GREATER_THAN);

    @JsProperty(name = "GREATER_THAN_OR_EQUAL")
    void setGREATER_THAN_OR_EQUAL(double GREATER_THAN_OR_EQUAL);

    @JsProperty(name = "GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING")
    void setGREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING(double GREATER_THAN_OR_EQUAL_ALLOW_FOLLOWING);
}
