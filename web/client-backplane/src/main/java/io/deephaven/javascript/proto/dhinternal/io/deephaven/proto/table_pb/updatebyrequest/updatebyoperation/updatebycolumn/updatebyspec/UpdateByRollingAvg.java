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
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingAvg",
        namespace = JsPackage.GLOBAL)
public class UpdateByRollingAvg {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReverseWindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType create() {
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
                static UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType create() {
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
            static UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(
                    UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(
                    UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByRollingAvg.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getForwardWindowScale();

        @JsProperty
        UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType getReverseWindowScale();

        @JsProperty
        void setForwardWindowScale(Object forwardWindowScale);

        @JsProperty
        void setReverseWindowScale(
                UpdateByRollingAvg.ToObjectReturnType.ReverseWindowScaleFieldType reverseWindowScale);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReverseWindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType create() {
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
                static UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType create() {
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
            static UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(
                    UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(
                    UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByRollingAvg.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getForwardWindowScale();

        @JsProperty
        UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType getReverseWindowScale();

        @JsProperty
        void setForwardWindowScale(Object forwardWindowScale);

        @JsProperty
        void setReverseWindowScale(
                UpdateByRollingAvg.ToObjectReturnType0.ReverseWindowScaleFieldType reverseWindowScale);
    }

    public static native UpdateByRollingAvg deserializeBinary(Uint8Array bytes);

    public static native UpdateByRollingAvg deserializeBinaryFromReader(
            UpdateByRollingAvg message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByRollingAvg message, Object writer);

    public static native UpdateByRollingAvg.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByRollingAvg msg);

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

    public native UpdateByRollingAvg.ToObjectReturnType0 toObject();

    public native UpdateByRollingAvg.ToObjectReturnType0 toObject(boolean includeInstance);
}
