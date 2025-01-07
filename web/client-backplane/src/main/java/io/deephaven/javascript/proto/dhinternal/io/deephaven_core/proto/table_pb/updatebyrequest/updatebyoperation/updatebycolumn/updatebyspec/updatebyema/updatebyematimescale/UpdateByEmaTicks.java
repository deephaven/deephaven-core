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
        name = "dhinternal.io.deephaven_core.proto.table_pb.UpdateByRequest.UpdateByOperation.UpdateByColumn.UpdateBySpec.UpdateByEma.UpdateByEmaTimescale.UpdateByEmaTicks",
        namespace = JsPackage.GLOBAL)
public class UpdateByEmaTicks {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static UpdateByEmaTicks.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getTicks();

        @JsProperty
        void setTicks(String ticks);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static UpdateByEmaTicks.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getTicks();

        @JsProperty
        void setTicks(String ticks);
    }

    public static native UpdateByEmaTicks deserializeBinary(Uint8Array bytes);

    public static native UpdateByEmaTicks deserializeBinaryFromReader(
            UpdateByEmaTicks message, Object reader);

    public static native void serializeBinaryToWriter(UpdateByEmaTicks message, Object writer);

    public static native UpdateByEmaTicks.ToObjectReturnType toObject(
            boolean includeInstance, UpdateByEmaTicks msg);

    public native String getTicks();

    public native Uint8Array serializeBinary();

    public native void setTicks(String value);

    public native UpdateByEmaTicks.ToObjectReturnType0 toObject();

    public native UpdateByEmaTicks.ToObjectReturnType0 toObject(boolean includeInstance);
}
