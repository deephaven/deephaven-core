//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.updatebyema;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.MathContext;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaOptions",
        namespace = JsPackage.GLOBAL)
public class UpdateByEmaOptions {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface BigValueContextFieldType {
            @JsOverlay
            static UpdateByEmaOptions.ToObjectReturnType.BigValueContextFieldType create() {
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
        static UpdateByEmaOptions.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEmaOptions.ToObjectReturnType.BigValueContextFieldType getBigValueContext();

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
                UpdateByEmaOptions.ToObjectReturnType.BigValueContextFieldType bigValueContext);

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
            static UpdateByEmaOptions.ToObjectReturnType0.BigValueContextFieldType create() {
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
        static UpdateByEmaOptions.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEmaOptions.ToObjectReturnType0.BigValueContextFieldType getBigValueContext();

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
                UpdateByEmaOptions.ToObjectReturnType0.BigValueContextFieldType bigValueContext);

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

    public static native UpdateByEmaOptions deserializeBinary(Uint8Array bytes);

    public static native UpdateByEmaOptions deserializeBinaryFromReader(
            UpdateByEmaOptions message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByEmaOptions message, Object writer);

    public static native UpdateByEmaOptions.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByEmaOptions msg);

    public native void clearBigValueContext();

    public native void clearOnNanValue();

    public native void clearOnNegativeDeltaTime();

    public native void clearOnNullTime();

    public native void clearOnNullValue();

    public native void clearOnZeroDeltaTime();

    public native MathContext getBigValueContext();

    public native int getOnNanValue();

    public native int getOnNegativeDeltaTime();

    public native int getOnNullTime();

    public native int getOnNullValue();

    public native int getOnZeroDeltaTime();

    public native boolean hasBigValueContext();

    public native boolean hasOnNanValue();

    public native boolean hasOnNegativeDeltaTime();

    public native boolean hasOnNullTime();

    public native boolean hasOnNullValue();

    public native boolean hasOnZeroDeltaTime();

    public native Uint8Array serializeBinary();

    public native void setBigValueContext();

    public native void setBigValueContext(MathContext value);

    public native void setOnNanValue(int value);

    public native void setOnNegativeDeltaTime(int value);

    public native void setOnNullTime(int value);

    public native void setOnNullValue(int value);

    public native void setOnZeroDeltaTime(int value);

    public native UpdateByEmaOptions.ToObjectReturnType0 toObject();

    public native UpdateByEmaOptions.ToObjectReturnType0 toObject(boolean includeInstance);
}
