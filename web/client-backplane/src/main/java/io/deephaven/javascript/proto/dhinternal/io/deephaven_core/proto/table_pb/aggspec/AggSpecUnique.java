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
        name = "dhinternal.io.deephaven_core.proto.table_pb.AggSpec.AggSpecUnique",
        namespace = JsPackage.GLOBAL)
public class AggSpecUnique {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface NonUniqueSentinelFieldType {
            @JsOverlay
            static AggSpecUnique.ToObjectReturnType.NonUniqueSentinelFieldType create() {
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

        @JsOverlay
        static AggSpecUnique.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AggSpecUnique.ToObjectReturnType.NonUniqueSentinelFieldType getNonUniqueSentinel();

        @JsProperty
        boolean isIncludeNulls();

        @JsProperty
        void setIncludeNulls(boolean includeNulls);

        @JsProperty
        void setNonUniqueSentinel(
                AggSpecUnique.ToObjectReturnType.NonUniqueSentinelFieldType nonUniqueSentinel);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface NonUniqueSentinelFieldType {
            @JsOverlay
            static AggSpecUnique.ToObjectReturnType0.NonUniqueSentinelFieldType create() {
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

        @JsOverlay
        static AggSpecUnique.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        AggSpecUnique.ToObjectReturnType0.NonUniqueSentinelFieldType getNonUniqueSentinel();

        @JsProperty
        boolean isIncludeNulls();

        @JsProperty
        void setIncludeNulls(boolean includeNulls);

        @JsProperty
        void setNonUniqueSentinel(
                AggSpecUnique.ToObjectReturnType0.NonUniqueSentinelFieldType nonUniqueSentinel);
    }

    public static native AggSpecUnique deserializeBinary(Uint8Array bytes);

    public static native AggSpecUnique deserializeBinaryFromReader(
            AggSpecUnique message, Object reader);

    public static native void serializeBinaryToWriter(AggSpecUnique message, Object writer);

    public static native AggSpecUnique.ToObjectReturnType toObject(
            boolean includeInstance, AggSpecUnique msg);

    public native void clearNonUniqueSentinel();

    public native boolean getIncludeNulls();

    public native AggSpecNonUniqueSentinel getNonUniqueSentinel();

    public native boolean hasNonUniqueSentinel();

    public native Uint8Array serializeBinary();

    public native void setIncludeNulls(boolean value);

    public native void setNonUniqueSentinel();

    public native void setNonUniqueSentinel(AggSpecNonUniqueSentinel value);

    public native AggSpecUnique.ToObjectReturnType0 toObject();

    public native AggSpecUnique.ToObjectReturnType0 toObject(boolean includeInstance);
}
