//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.updatebyrequest.updatebyoperation.updatebycolumn.updatebyspec.updatebyema.updatebyematimescale;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaTimescale.UpdateByEmaTime",
        namespace = JsPackage.GLOBAL)
public class UpdateByEmaTime {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static UpdateByEmaTime.ToObjectReturnType create() {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static UpdateByEmaTime.ToObjectReturnType0 create() {
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

    public static native UpdateByEmaTime deserializeBinary(Uint8Array bytes);

    public static native UpdateByEmaTime deserializeBinaryFromReader(
            UpdateByEmaTime message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByEmaTime message, Object writer);

    public static native UpdateByEmaTime.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByEmaTime msg);

    public native String getColumn();

    public native String getPeriodNanos();

    public native Uint8Array serializeBinary();

    public native void setColumn(String value);

    public native void setPeriodNanos(String value);

    public native UpdateByEmaTime.ToObjectReturnType0 toObject();

    public native UpdateByEmaTime.ToObjectReturnType0 toObject(boolean includeInstance);
}
