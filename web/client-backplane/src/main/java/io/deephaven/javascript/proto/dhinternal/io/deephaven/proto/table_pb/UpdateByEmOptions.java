package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByEmOptions",
        namespace = JsPackage.GLOBAL)
public class UpdateByEmOptions {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BigValueContextFieldType {
            @JsOverlay
            static UpdateByEmOptions.ToObjectReturnType.BigValueContextFieldType create() {
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

        @JsOverlay
        static UpdateByEmOptions.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEmOptions.ToObjectReturnType.BigValueContextFieldType getBigValueContext();

        @JsProperty
        double getOnNanValue();

        @JsProperty
        double getOnNegativeDeltaTime();

        @JsProperty
        double getOnNullTime();

        @JsProperty
        double getOnNullValue();

        @JsProperty
        double getOnZeroDeltaTime();

        @JsProperty
        void setBigValueContext(
                UpdateByEmOptions.ToObjectReturnType.BigValueContextFieldType bigValueContext);

        @JsProperty
        void setOnNanValue(double onNanValue);

        @JsProperty
        void setOnNegativeDeltaTime(double onNegativeDeltaTime);

        @JsProperty
        void setOnNullTime(double onNullTime);

        @JsProperty
        void setOnNullValue(double onNullValue);

        @JsProperty
        void setOnZeroDeltaTime(double onZeroDeltaTime);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BigValueContextFieldType {
            @JsOverlay
            static UpdateByEmOptions.ToObjectReturnType0.BigValueContextFieldType create() {
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

        @JsOverlay
        static UpdateByEmOptions.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEmOptions.ToObjectReturnType0.BigValueContextFieldType getBigValueContext();

        @JsProperty
        double getOnNanValue();

        @JsProperty
        double getOnNegativeDeltaTime();

        @JsProperty
        double getOnNullTime();

        @JsProperty
        double getOnNullValue();

        @JsProperty
        double getOnZeroDeltaTime();

        @JsProperty
        void setBigValueContext(
                UpdateByEmOptions.ToObjectReturnType0.BigValueContextFieldType bigValueContext);

        @JsProperty
        void setOnNanValue(double onNanValue);

        @JsProperty
        void setOnNegativeDeltaTime(double onNegativeDeltaTime);

        @JsProperty
        void setOnNullTime(double onNullTime);

        @JsProperty
        void setOnNullValue(double onNullValue);

        @JsProperty
        void setOnZeroDeltaTime(double onZeroDeltaTime);
    }

    public static native UpdateByEmOptions deserializeBinary(Uint8Array bytes);

    public static native UpdateByEmOptions deserializeBinaryFromReader(
            UpdateByEmOptions message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByEmOptions message, Object writer);

    public static native UpdateByEmOptions.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByEmOptions msg);

    public native void clearBigValueContext();

    public native MathContext getBigValueContext();

    public native double getOnNanValue();

    public native double getOnNegativeDeltaTime();

    public native double getOnNullTime();

    public native double getOnNullValue();

    public native double getOnZeroDeltaTime();

    public native boolean hasBigValueContext();

    public native Uint8Array serializeBinary();

    public native void setBigValueContext();

    public native void setBigValueContext(MathContext value);

    public native void setOnNanValue(double value);

    public native void setOnNegativeDeltaTime(double value);

    public native void setOnNullTime(double value);

    public native void setOnNullValue(double value);

    public native void setOnZeroDeltaTime(double value);

    public native UpdateByEmOptions.ToObjectReturnType0 toObject();

    public native UpdateByEmOptions.ToObjectReturnType0 toObject(boolean includeInstance);
}
