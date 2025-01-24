//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest;

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
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOptions",
        namespace = JsPackage.GLOBAL)
public class UpdateByOptions {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MathContextFieldType {
            @JsOverlay
            static UpdateByOptions.ToObjectReturnType.MathContextFieldType create() {
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
        static UpdateByOptions.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getChunkCapacity();

        @JsProperty
        double getInitialHashTableSize();

        @JsProperty
        UpdateByOptions.ToObjectReturnType.MathContextFieldType getMathContext();

        @JsProperty
        double getMaxStaticSparseMemoryOverhead();

        @JsProperty
        double getMaximumLoadFactor();

        @JsProperty
        double getTargetLoadFactor();

        @JsProperty
        boolean isUseRedirection();

        @JsProperty
        void setChunkCapacity(double chunkCapacity);

        @JsProperty
        void setInitialHashTableSize(double initialHashTableSize);

        @JsProperty
        void setMathContext(UpdateByOptions.ToObjectReturnType.MathContextFieldType mathContext);

        @JsProperty
        void setMaxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

        @JsProperty
        void setMaximumLoadFactor(double maximumLoadFactor);

        @JsProperty
        void setTargetLoadFactor(double targetLoadFactor);

        @JsProperty
        void setUseRedirection(boolean useRedirection);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface MathContextFieldType {
            @JsOverlay
            static UpdateByOptions.ToObjectReturnType0.MathContextFieldType create() {
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
        static UpdateByOptions.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getChunkCapacity();

        @JsProperty
        double getInitialHashTableSize();

        @JsProperty
        UpdateByOptions.ToObjectReturnType0.MathContextFieldType getMathContext();

        @JsProperty
        double getMaxStaticSparseMemoryOverhead();

        @JsProperty
        double getMaximumLoadFactor();

        @JsProperty
        double getTargetLoadFactor();

        @JsProperty
        boolean isUseRedirection();

        @JsProperty
        void setChunkCapacity(double chunkCapacity);

        @JsProperty
        void setInitialHashTableSize(double initialHashTableSize);

        @JsProperty
        void setMathContext(UpdateByOptions.ToObjectReturnType0.MathContextFieldType mathContext);

        @JsProperty
        void setMaxStaticSparseMemoryOverhead(double maxStaticSparseMemoryOverhead);

        @JsProperty
        void setMaximumLoadFactor(double maximumLoadFactor);

        @JsProperty
        void setTargetLoadFactor(double targetLoadFactor);

        @JsProperty
        void setUseRedirection(boolean useRedirection);
    }

    public static native UpdateByOptions deserializeBinary(Uint8Array bytes);

    public static native UpdateByOptions deserializeBinaryFromReader(
            UpdateByOptions message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByOptions message, Object writer);

    public static native UpdateByOptions.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByOptions msg);

    public native void clearChunkCapacity();

    public native void clearInitialHashTableSize();

    public native void clearMathContext();

    public native void clearMaxStaticSparseMemoryOverhead();

    public native void clearMaximumLoadFactor();

    public native void clearTargetLoadFactor();

    public native void clearUseRedirection();

    public native int getChunkCapacity();

    public native int getInitialHashTableSize();

    public native MathContext getMathContext();

    public native double getMaxStaticSparseMemoryOverhead();

    public native double getMaximumLoadFactor();

    public native double getTargetLoadFactor();

    public native boolean getUseRedirection();

    public native boolean hasChunkCapacity();

    public native boolean hasInitialHashTableSize();

    public native boolean hasMathContext();

    public native boolean hasMaxStaticSparseMemoryOverhead();

    public native boolean hasMaximumLoadFactor();

    public native boolean hasTargetLoadFactor();

    public native boolean hasUseRedirection();

    public native Uint8Array serializeBinary();

    public native void setChunkCapacity(int value);

    public native void setInitialHashTableSize(int value);

    public native void setMathContext();

    public native void setMathContext(MathContext value);

    public native void setMaxStaticSparseMemoryOverhead(double value);

    public native void setMaximumLoadFactor(double value);

    public native void setTargetLoadFactor(double value);

    public native void setUseRedirection(boolean value);

    public native UpdateByOptions.ToObjectReturnType0 toObject();

    public native UpdateByOptions.ToObjectReturnType0 toObject(boolean includeInstance);
}
