//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.aggspec;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecFormula",
        namespace = JsPackage.GLOBAL)
public class AggSpecFormula {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecFormula.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getFormula();

        @JsProperty
        String getParamToken();

        @JsProperty
        void setFormula(String formula);

        @JsProperty
        void setParamToken(String paramToken);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecFormula.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getFormula();

        @JsProperty
        String getParamToken();

        @JsProperty
        void setFormula(String formula);

        @JsProperty
        void setParamToken(String paramToken);
    }

    public static native AggSpecFormula deserializeBinary(Uint8Array bytes);

    public static native AggSpecFormula deserializeBinaryFromReader(
            AggSpecFormula message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecFormula message, Object writer);

    public static native AggSpecFormula.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecFormula msg);

    public native String getFormula();

    public native String getParamToken();

    public native Uint8Array serializeBinary();

    public native void setFormula(String value);

    public native void setParamToken(String value);

    public native AggSpecFormula.ToObjectReturnType0 toObject();

    public native AggSpecFormula.ToObjectReturnType0 toObject(boolean includeInstance);
}
