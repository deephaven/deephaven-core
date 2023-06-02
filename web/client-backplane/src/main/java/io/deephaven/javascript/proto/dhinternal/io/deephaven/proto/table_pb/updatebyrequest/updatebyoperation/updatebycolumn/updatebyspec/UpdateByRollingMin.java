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
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingMin",
        namespace = JsPackage.GLOBAL)
public class UpdateByRollingMin {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReverseWindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType create() {
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
                static UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumn();

                @JsProperty
                String getPeriodNanos();

                @JsProperty
                void setColumn(String column);

                @JsProperty
                void setPeriodNanos(String periodNanos);
            }

            @JsOverlay
            static UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(
                    UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(
                    UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByRollingMin.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getForwardWindowScale();

        @JsProperty
        UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType getReverseWindowScale();

        @JsProperty
        void setForwardWindowScale(Object forwardWindowScale);

        @JsProperty
        void setReverseWindowScale(
                UpdateByRollingMin.ToObjectReturnType.ReverseWindowScaleFieldType reverseWindowScale);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReverseWindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType create() {
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
                static UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType create() {
                    return Js.uncheckedCast(JsPropertyMap.of());
                }

                @JsProperty
                String getColumn();

                @JsProperty
                String getPeriodNanos();

                @JsProperty
                void setColumn(String column);

                @JsProperty
                void setPeriodNanos(String periodNanos);
            }

            @JsOverlay
            static UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(
                    UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(
                    UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByRollingMin.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getForwardWindowScale();

        @JsProperty
        UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType getReverseWindowScale();

        @JsProperty
        void setForwardWindowScale(Object forwardWindowScale);

        @JsProperty
        void setReverseWindowScale(
                UpdateByRollingMin.ToObjectReturnType0.ReverseWindowScaleFieldType reverseWindowScale);
    }

    public static native UpdateByRollingMin deserializeBinary(Uint8Array bytes);

    public static native UpdateByRollingMin deserializeBinaryFromReader(
            UpdateByRollingMin message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByRollingMin message, Object writer);

    public static native UpdateByRollingMin.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByRollingMin msg);

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

    public native UpdateByRollingMin.ToObjectReturnType0 toObject();

    public native UpdateByRollingMin.ToObjectReturnType0 toObject(boolean includeInstance);
}
