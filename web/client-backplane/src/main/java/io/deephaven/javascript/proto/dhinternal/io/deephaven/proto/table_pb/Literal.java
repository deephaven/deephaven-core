package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.literal.ValueCase;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.Literal",
        namespace = JsPackage.GLOBAL)
public class Literal {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static Literal.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getDoubleValue();

        @JsProperty
        String getLongValue();

        @JsProperty
        String getNanoTimeValue();

        @JsProperty
        String getStringValue();

        @JsProperty
        boolean isBoolValue();

        @JsProperty
        void setBoolValue(boolean boolValue);

        @JsProperty
        void setDoubleValue(double doubleValue);

        @JsProperty
        void setLongValue(String longValue);

        @JsProperty
        void setNanoTimeValue(String nanoTimeValue);

        @JsProperty
        void setStringValue(String stringValue);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static Literal.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getDoubleValue();

        @JsProperty
        String getLongValue();

        @JsProperty
        String getNanoTimeValue();

        @JsProperty
        String getStringValue();

        @JsProperty
        boolean isBoolValue();

        @JsProperty
        void setBoolValue(boolean boolValue);

        @JsProperty
        void setDoubleValue(double doubleValue);

        @JsProperty
        void setLongValue(String longValue);

        @JsProperty
        void setNanoTimeValue(String nanoTimeValue);

        @JsProperty
        void setStringValue(String stringValue);
    }

    public static native Literal deserializeBinary(Uint8Array bytes);

    public static native Literal deserializeBinaryFromReader(Literal message, Object reader);

    public static native void serializeBinaryToWriter(Literal message, Object writer);

    public static native Literal.ToObjectReturnType toObject(boolean includeInstance, Literal msg);

    public native void clearBoolValue();

    public native void clearDoubleValue();

    public native void clearLongValue();

    public native void clearNanoTimeValue();

    public native void clearStringValue();

    public native boolean getBoolValue();

    public native double getDoubleValue();

    public native String getLongValue();

    public native String getNanoTimeValue();

    public native String getStringValue();

    public native ValueCase getValueCase();

    public native boolean hasBoolValue();

    public native boolean hasDoubleValue();

    public native boolean hasLongValue();

    public native boolean hasNanoTimeValue();

    public native boolean hasStringValue();

    public native Uint8Array serializeBinary();

    public native void setBoolValue(boolean value);

    public native void setDoubleValue(double value);

    public native void setLongValue(String value);

    public native void setNanoTimeValue(String value);

    public native void setStringValue(String value);

    public native Literal.ToObjectReturnType0 toObject();

    public native Literal.ToObjectReturnType0 toObject(boolean includeInstance);
}
