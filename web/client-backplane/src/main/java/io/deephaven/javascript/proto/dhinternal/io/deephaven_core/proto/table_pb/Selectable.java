//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.selectable.TypeCase;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.Selectable",
        namespace = JsPackage.GLOBAL)
public class Selectable {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static Selectable.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getRaw();

        @JsProperty
        void setRaw(String raw);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static Selectable.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getRaw();

        @JsProperty
        void setRaw(String raw);
    }

    public static native Selectable deserializeBinary(Uint8Array bytes);

    public static native Selectable deserializeBinaryFromReader(Selectable message, Object reader);

    public static native void serializeBinaryToWriter(Selectable message, Object writer);

    public static native Selectable.ToObjectReturnType toObject(
            boolean includeInstance, Selectable msg);

    public native void clearRaw();

    public native String getRaw();

    public native int getTypeCase();

    public native boolean hasRaw();

    public native Uint8Array serializeBinary();

    public native void setRaw(String value);

    public native Selectable.ToObjectReturnType0 toObject();

    public native Selectable.ToObjectReturnType0 toObject(boolean includeInstance);
}
