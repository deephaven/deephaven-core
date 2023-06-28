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
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByRollingWAvg",
        namespace = JsPackage.GLOBAL)
public class UpdateByRollingWAvg {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReverseWindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType create() {
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
                static UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType create() {
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
            static UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(
                    UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(
                    UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByRollingWAvg.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getForwardWindowScale();

        @JsProperty
        UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType getReverseWindowScale();

        @JsProperty
        String getWeightColumn();

        @JsProperty
        void setForwardWindowScale(Object forwardWindowScale);

        @JsProperty
        void setReverseWindowScale(
                UpdateByRollingWAvg.ToObjectReturnType.ReverseWindowScaleFieldType reverseWindowScale);

        @JsProperty
        void setWeightColumn(String weightColumn);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ReverseWindowScaleFieldType {
            @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
            public interface TicksFieldType {
                @JsOverlay
                static UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType create() {
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
                static UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType create() {
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
            static UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType getTicks();

            @JsProperty
            UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType getTime();

            @JsProperty
            void setTicks(
                    UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TicksFieldType ticks);

            @JsProperty
            void setTime(
                    UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType.TimeFieldType time);
        }

        @JsOverlay
        static UpdateByRollingWAvg.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getForwardWindowScale();

        @JsProperty
        UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType getReverseWindowScale();

        @JsProperty
        String getWeightColumn();

        @JsProperty
        void setForwardWindowScale(Object forwardWindowScale);

        @JsProperty
        void setReverseWindowScale(
                UpdateByRollingWAvg.ToObjectReturnType0.ReverseWindowScaleFieldType reverseWindowScale);

        @JsProperty
        void setWeightColumn(String weightColumn);
    }

    public static native UpdateByRollingWAvg deserializeBinary(Uint8Array bytes);

    public static native UpdateByRollingWAvg deserializeBinaryFromReader(
            UpdateByRollingWAvg message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByRollingWAvg message, Object writer);

    public static native UpdateByRollingWAvg.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByRollingWAvg msg);

    public native void clearForwardWindowScale();

    public native void clearReverseWindowScale();

    public native UpdateByWindowScale getForwardWindowScale();

    public native UpdateByWindowScale getReverseWindowScale();

    public native String getWeightColumn();

    public native boolean hasForwardWindowScale();

    public native boolean hasReverseWindowScale();

    public native Uint8Array serializeBinary();

    public native void setForwardWindowScale();

    public native void setForwardWindowScale(UpdateByWindowScale value);

    public native void setReverseWindowScale();

    public native void setReverseWindowScale(UpdateByWindowScale value);

    public native void setWeightColumn(String value);

    public native UpdateByRollingWAvg.ToObjectReturnType0 toObject();

    public native UpdateByRollingWAvg.ToObjectReturnType0 toObject(boolean includeInstance);
}
