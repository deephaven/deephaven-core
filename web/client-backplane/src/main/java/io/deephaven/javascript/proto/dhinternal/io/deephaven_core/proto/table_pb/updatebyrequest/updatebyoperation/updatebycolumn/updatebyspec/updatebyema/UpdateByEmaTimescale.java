//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.updatebyema;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.updatebyema.updatebyematimescale.UpdateByEmaTicks;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.updatebyema.updatebyematimescale.UpdateByEmaTime;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaTimescale",
        namespace = JsPackage.GLOBAL)
public class UpdateByEmaTimescale {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicksFieldType {
            @JsOverlay
            static UpdateByEmaTimescale.ToObjectReturnType.TicksFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getTicks();

            @JsProperty
            void setTicks(String ticks);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TimeFieldType {
            @JsOverlay
            static UpdateByEmaTimescale.ToObjectReturnType.TimeFieldType create() {
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
        static UpdateByEmaTimescale.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEmaTimescale.ToObjectReturnType.TicksFieldType getTicks();

        @JsProperty
        UpdateByEmaTimescale.ToObjectReturnType.TimeFieldType getTime();

        @JsProperty
        void setTicks(UpdateByEmaTimescale.ToObjectReturnType.TicksFieldType ticks);

        @JsProperty
        void setTime(UpdateByEmaTimescale.ToObjectReturnType.TimeFieldType time);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TicksFieldType {
            @JsOverlay
            static UpdateByEmaTimescale.ToObjectReturnType0.TicksFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getTicks();

            @JsProperty
            void setTicks(String ticks);
        }

        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface TimeFieldType {
            @JsOverlay
            static UpdateByEmaTimescale.ToObjectReturnType0.TimeFieldType create() {
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
        static UpdateByEmaTimescale.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        UpdateByEmaTimescale.ToObjectReturnType0.TicksFieldType getTicks();

        @JsProperty
        UpdateByEmaTimescale.ToObjectReturnType0.TimeFieldType getTime();

        @JsProperty
        void setTicks(UpdateByEmaTimescale.ToObjectReturnType0.TicksFieldType ticks);

        @JsProperty
        void setTime(UpdateByEmaTimescale.ToObjectReturnType0.TimeFieldType time);
    }

    public static native UpdateByEmaTimescale deserializeBinary(Uint8Array bytes);

    public static native UpdateByEmaTimescale deserializeBinaryFromReader(
            UpdateByEmaTimescale message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByEmaTimescale message, Object writer);

    public static native UpdateByEmaTimescale.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByEmaTimescale msg);

    public native void clearTicks();

    public native void clearTime();

    public native UpdateByEmaTicks getTicks();

    public native UpdateByEmaTime getTime();

    public native int getTypeCase();

    public native boolean hasTicks();

    public native boolean hasTime();

    public native Uint8Array serializeBinary();

    public native void setTicks();

    public native void setTicks(UpdateByEmaTicks value);

    public native void setTime();

    public native void setTime(UpdateByEmaTime value);

    public native UpdateByEmaTimescale.ToObjectReturnType0 toObject();

    public native UpdateByEmaTimescale.ToObjectReturnType0 toObject(boolean includeInstance);
}
