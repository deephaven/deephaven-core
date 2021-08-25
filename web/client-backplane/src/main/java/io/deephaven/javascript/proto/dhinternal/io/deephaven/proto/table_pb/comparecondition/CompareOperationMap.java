package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comparecondition;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.CompareCondition.CompareOperationMap",
    namespace = JsPackage.GLOBAL)
public interface CompareOperationMap {
    @JsOverlay
    static CompareOperationMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "EQUALS")
    double getEQUALS();

    @JsProperty(name = "GREATER_THAN")
    double getGREATER_THAN();

    @JsProperty(name = "GREATER_THAN_OR_EQUAL")
    double getGREATER_THAN_OR_EQUAL();

    @JsProperty(name = "LESS_THAN")
    double getLESS_THAN();

    @JsProperty(name = "LESS_THAN_OR_EQUAL")
    double getLESS_THAN_OR_EQUAL();

    @JsProperty(name = "NOT_EQUALS")
    double getNOT_EQUALS();

    @JsProperty(name = "EQUALS")
    void setEQUALS(double EQUALS);

    @JsProperty(name = "GREATER_THAN")
    void setGREATER_THAN(double GREATER_THAN);

    @JsProperty(name = "GREATER_THAN_OR_EQUAL")
    void setGREATER_THAN_OR_EQUAL(double GREATER_THAN_OR_EQUAL);

    @JsProperty(name = "LESS_THAN")
    void setLESS_THAN(double LESS_THAN);

    @JsProperty(name = "LESS_THAN_OR_EQUAL")
    void setLESS_THAN_OR_EQUAL(double LESS_THAN_OR_EQUAL);

    @JsProperty(name = "NOT_EQUALS")
    void setNOT_EQUALS(double NOT_EQUALS);
}
