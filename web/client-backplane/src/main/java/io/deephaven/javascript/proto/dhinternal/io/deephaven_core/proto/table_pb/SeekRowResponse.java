//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.table_pb.SeekRowResponse",
        namespace = JsPackage.GLOBAL)
public class SeekRowResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static SeekRowResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getResultRow();

        @JsProperty
        void setResultRow(String resultRow);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static SeekRowResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getResultRow();

        @JsProperty
        void setResultRow(String resultRow);
    }

    public static native SeekRowResponse deserializeBinary(Uint8Array bytes);

    public static native SeekRowResponse deserializeBinaryFromReader(
            SeekRowResponse message, Object reader);

    public static native void serializeBinaryToWriter(SeekRowResponse message, Object writer);

    public static native SeekRowResponse.ToObjectReturnType toObject(
            boolean includeInstance, SeekRowResponse msg);

    public native String getResultRow();

    public native Uint8Array serializeBinary();

    public native void setResultRow(String value);

    public native SeekRowResponse.ToObjectReturnType0 toObject();

    public native SeekRowResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
