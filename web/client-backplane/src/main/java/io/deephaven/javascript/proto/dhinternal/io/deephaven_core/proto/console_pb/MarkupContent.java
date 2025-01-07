//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.MarkupContent",
        namespace = JsPackage.GLOBAL)
public class MarkupContent {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static MarkupContent.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getKind();

        @JsProperty
        String getValue();

        @JsProperty
        void setKind(String kind);

        @JsProperty
        void setValue(String value);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static MarkupContent.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getKind();

        @JsProperty
        String getValue();

        @JsProperty
        void setKind(String kind);

        @JsProperty
        void setValue(String value);
    }

    public static native MarkupContent deserializeBinary(Uint8Array bytes);

    public static native MarkupContent deserializeBinaryFromReader(
            MarkupContent message, Object reader);

    public static native void serializeBinaryToWriter(MarkupContent message, Object writer);

    public static native MarkupContent.ToObjectReturnType toObject(
            boolean includeInstance, MarkupContent msg);

    public native String getKind();

    public native String getValue();

    public native Uint8Array serializeBinary();

    public native void setKind(String value);

    public native void setValue(String value);

    public native MarkupContent.ToObjectReturnType0 toObject();

    public native MarkupContent.ToObjectReturnType0 toObject(boolean includeInstance);
}
