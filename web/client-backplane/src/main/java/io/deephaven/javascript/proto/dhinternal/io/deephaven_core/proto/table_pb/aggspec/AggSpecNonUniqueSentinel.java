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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecNonUniqueSentinel",
        namespace = JsPackage.GLOBAL)
public class AggSpecNonUniqueSentinel {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static AggSpecNonUniqueSentinel.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getByteValue();

        @JsProperty
        double getCharValue();

        @JsProperty
        double getDoubleValue();

        @JsProperty
        double getFloatValue();

        @JsProperty
        double getIntValue();

        @JsProperty
        String getLongValue();

        @JsProperty
        double getNullValue();

        @JsProperty
        double getShortValue();

        @JsProperty
        String getStringValue();

        @JsProperty
        boolean isBoolValue();

        @JsProperty
        void setBoolValue(boolean boolValue);

        @JsProperty
        void setByteValue(double byteValue);

        @JsProperty
        void setCharValue(double charValue);

        @JsProperty
        void setDoubleValue(double doubleValue);

        @JsProperty
        void setFloatValue(double floatValue);

        @JsProperty
        void setIntValue(double intValue);

        @JsProperty
        void setLongValue(String longValue);

        @JsProperty
        void setNullValue(double nullValue);

        @JsProperty
        void setShortValue(double shortValue);

        @JsProperty
        void setStringValue(String stringValue);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static AggSpecNonUniqueSentinel.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getByteValue();

        @JsProperty
        double getCharValue();

        @JsProperty
        double getDoubleValue();

        @JsProperty
        double getFloatValue();

        @JsProperty
        double getIntValue();

        @JsProperty
        String getLongValue();

        @JsProperty
        double getNullValue();

        @JsProperty
        double getShortValue();

        @JsProperty
        String getStringValue();

        @JsProperty
        boolean isBoolValue();

        @JsProperty
        void setBoolValue(boolean boolValue);

        @JsProperty
        void setByteValue(double byteValue);

        @JsProperty
        void setCharValue(double charValue);

        @JsProperty
        void setDoubleValue(double doubleValue);

        @JsProperty
        void setFloatValue(double floatValue);

        @JsProperty
        void setIntValue(double intValue);

        @JsProperty
        void setLongValue(String longValue);

        @JsProperty
        void setNullValue(double nullValue);

        @JsProperty
        void setShortValue(double shortValue);

        @JsProperty
        void setStringValue(String stringValue);
    }

    public static native AggSpecNonUniqueSentinel deserializeBinary(Uint8Array bytes);

    public static native AggSpecNonUniqueSentinel deserializeBinaryFromReader(
            AggSpecNonUniqueSentinel message, Object reader);

    public static native void serializeBinaryToWriter(
            AggSpecNonUniqueSentinel message, Object writer);

    public static native AggSpecNonUniqueSentinel.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecNonUniqueSentinel msg);

    public native void clearBoolValue();

    public native void clearByteValue();

    public native void clearCharValue();

    public native void clearDoubleValue();

    public native void clearFloatValue();

    public native void clearIntValue();

    public native void clearLongValue();

    public native void clearNullValue();

    public native void clearShortValue();

    public native void clearStringValue();

    public native boolean getBoolValue();

    public native double getByteValue();

    public native double getCharValue();

    public native double getDoubleValue();

    public native double getFloatValue();

    public native double getIntValue();

    public native String getLongValue();

    public native int getNullValue();

    public native double getShortValue();

    public native String getStringValue();

    public native int getTypeCase();

    public native boolean hasBoolValue();

    public native boolean hasByteValue();

    public native boolean hasCharValue();

    public native boolean hasDoubleValue();

    public native boolean hasFloatValue();

    public native boolean hasIntValue();

    public native boolean hasLongValue();

    public native boolean hasNullValue();

    public native boolean hasShortValue();

    public native boolean hasStringValue();

    public native Uint8Array serializeBinary();

    public native void setBoolValue(boolean value);

    public native void setByteValue(double value);

    public native void setCharValue(double value);

    public native void setDoubleValue(double value);

    public native void setFloatValue(double value);

    public native void setIntValue(double value);

    public native void setLongValue(String value);

    public native void setNullValue(int value);

    public native void setShortValue(double value);

    public native void setStringValue(String value);

    public native AggSpecNonUniqueSentinel.ToObjectReturnType0 toObject();

    public native AggSpecNonUniqueSentinel.ToObjectReturnType0 toObject(boolean includeInstance);
}
