//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.mathcontext.RoundingModeMap;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.MathContext",
        namespace = JsPackage.GLOBAL)
public class MathContext {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static MathContext.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getPrecision();

        @JsProperty
        double getRoundingMode();

        @JsProperty
        void setPrecision(double precision);

        @JsProperty
        void setRoundingMode(double roundingMode);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static MathContext.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getPrecision();

        @JsProperty
        double getRoundingMode();

        @JsProperty
        void setPrecision(double precision);

        @JsProperty
        void setRoundingMode(double roundingMode);
    }

    public static RoundingModeMap RoundingMode;

    public static native MathContext deserializeBinary(Uint8Array bytes);

    public static native MathContext deserializeBinaryFromReader(MathContext message, Object reader);

    public static native void serializeBinaryToWriter(MathContext message, Object writer);

    public static native MathContext.ToObjectReturnType toObject(
            boolean includeInstance, MathContext msg);

    public native int getPrecision();

    public native int getRoundingMode();

    public native Uint8Array serializeBinary();

    public native void setPrecision(int value);

    public native void setRoundingMode(int value);

    public native MathContext.ToObjectReturnType0 toObject();

    public native MathContext.ToObjectReturnType0 toObject(boolean includeInstance);
}
