//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.diagnostic;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.Diagnostic.CodeDescription",
        namespace = JsPackage.GLOBAL)
public class CodeDescription {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static CodeDescription.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getHref();

        @JsProperty
        void setHref(String href);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static CodeDescription.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getHref();

        @JsProperty
        void setHref(String href);
    }

    public static native CodeDescription deserializeBinary(Uint8Array bytes);

    public static native CodeDescription deserializeBinaryFromReader(
            CodeDescription message, Object reader);

    public static native void serializeBinaryToWriter(CodeDescription message, Object writer);

    public static native CodeDescription.ToObjectReturnType toObject(
            boolean includeInstance, CodeDescription msg);

    public native String getHref();

    public native Uint8Array serializeBinary();

    public native void setHref(String value);

    public native CodeDescription.ToObjectReturnType0 toObject();

    public native CodeDescription.ToObjectReturnType0 toObject(boolean includeInstance);
}
