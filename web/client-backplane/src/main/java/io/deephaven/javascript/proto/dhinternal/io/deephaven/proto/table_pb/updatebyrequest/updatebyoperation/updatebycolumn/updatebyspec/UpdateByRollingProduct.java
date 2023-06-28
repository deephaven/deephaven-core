package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.UpdateByWindowScale;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingProduct",
        namespace = JsPackage.GLOBAL)
public class UpdateByRollingProduct {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReverseWindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getTicks();

                @JsProperty
                void setTicks(double ticks);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TimeFieldType {
                @JsOverlay
                static UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumn();

                @JsProperty
                String getDurationString();

                @JsProperty
                String getNanos();

                @JsProperty
                void setColumn(String column);

                @JsProperty
                void setDurationString(String durationString);

                @JsProperty
                void setNanos(String nanos);
            }

            @JsOverlay
            static UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(
                    UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(
                    UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByRollingProduct.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getForwardWindowScale();

        @JsProperty
        UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType getReverseWindowScale();

        @JsProperty
        void setForwardWindowScale(Object forwardWindowScale);

        @JsProperty
        void setReverseWindowScale(
                UpdateByRollingProduct.ToObjectReturnType.ReverseWindowScaleFieldType reverseWindowScale);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReverseWindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                double getTicks();

                @JsProperty
                void setTicks(double ticks);
            }

            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TimeFieldType {
                @JsOverlay
                static UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumn();

                @JsProperty
                String getDurationString();

                @JsProperty
                String getNanos();

                @JsProperty
                void setColumn(String column);

                @JsProperty
                void setDurationString(String durationString);

                @JsProperty
                void setNanos(String nanos);
            }

            @JsOverlay
            static UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(
                    UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(
                    UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByRollingProduct.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getForwardWindowScale();

        @JsProperty
        UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType getReverseWindowScale();

        @JsProperty
        void setForwardWindowScale(Object forwardWindowScale);

        @JsProperty
        void setReverseWindowScale(
                UpdateByRollingProduct.ToObjectReturnType0.ReverseWindowScaleFieldType reverseWindowScale);
    }

    public static native UpdateByRollingProduct deserializeBinary(Uint8Array bytes);

    public static native UpdateByRollingProduct deserializeBinaryFromReader(
            UpdateByRollingProduct message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByRollingProduct message, Object writer);

    public static native UpdateByRollingProduct.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByRollingProduct msg);

    public native void clearForwardWindowScale();

    public native void clearReverseWindowScale();

    public native UpdateByWindowScale getForwardWindowScale();

    public native UpdateByWindowScale getReverseWindowScale();

    public native boolean hasForwardWindowScale();

    public native boolean hasReverseWindowScale();

    public native Uint8Array serializeBinary();

    public native void setForwardWindowScale();

    public native void setForwardWindowScale(UpdateByWindowScale value);

    public native void setReverseWindowScale();

    public native void setReverseWindowScale(UpdateByWindowScale value);

    public native UpdateByRollingProduct.ToObjectReturnType0 toObject();

    public native UpdateByRollingProduct.ToObjectReturnType0 toObject(boolean includeInstance);
}
