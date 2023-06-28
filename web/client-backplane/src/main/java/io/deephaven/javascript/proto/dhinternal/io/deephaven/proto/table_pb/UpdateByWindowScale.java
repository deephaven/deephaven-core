package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebywindowscale.UpdateByWindowTicks;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.updatebywindowscale.UpdateByWindowTime;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.UpdateByWindowScale",
        namespace = JsPackage.GLOBAL)
public class UpdateByWindowScale {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicksFieldType {
            @JsOverlay
            static UpdateByWindowScale.ToObjectReturnType.TicksFieldType create() {
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
            static UpdateByWindowScale.ToObjectReturnType.TimeFieldType create() {
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
        static UpdateByWindowScale.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByWindowScale.ToObjectReturnType.TicksFieldType getTicks();

        @JsProperty
        UpdateByWindowScale.ToObjectReturnType.TimeFieldType getTime();

        @JsProperty
        void setTicks(UpdateByWindowScale.ToObjectReturnType.TicksFieldType ticks);

        @JsProperty
        void setTime(UpdateByWindowScale.ToObjectReturnType.TimeFieldType time);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicksFieldType {
            @JsOverlay
            static UpdateByWindowScale.ToObjectReturnType0.TicksFieldType create() {
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
            static UpdateByWindowScale.ToObjectReturnType0.TimeFieldType create() {
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
        static UpdateByWindowScale.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByWindowScale.ToObjectReturnType0.TicksFieldType getTicks();

        @JsProperty
        UpdateByWindowScale.ToObjectReturnType0.TimeFieldType getTime();

        @JsProperty
        void setTicks(UpdateByWindowScale.ToObjectReturnType0.TicksFieldType ticks);

        @JsProperty
        void setTime(UpdateByWindowScale.ToObjectReturnType0.TimeFieldType time);
    }

    public static native UpdateByWindowScale deserializeBinary(Uint8Array bytes);

    public static native UpdateByWindowScale deserializeBinaryFromReader(
            UpdateByWindowScale message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByWindowScale message, Object writer);

    public static native UpdateByWindowScale.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByWindowScale msg);

    public native void clearTicks();

    public native void clearTime();

    public native UpdateByWindowTicks getTicks();

    public native UpdateByWindowTime getTime();

    public native int getTypeCase();

    public native boolean hasTicks();

    public native boolean hasTime();

    public native Uint8Array serializeBinary();

    public native void setTicks();

    public native void setTicks(UpdateByWindowTicks value);

    public native void setTime();

    public native void setTime(UpdateByWindowTime value);

    public native UpdateByWindowScale.ToObjectReturnType0 toObject();

    public native UpdateByWindowScale.ToObjectReturnType0 toObject(boolean includeInstance);
}
