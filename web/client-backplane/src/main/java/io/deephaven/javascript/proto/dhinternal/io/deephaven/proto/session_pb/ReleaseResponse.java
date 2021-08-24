package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.session_pb.ReleaseResponse",
    namespace = JsPackage.GLOBAL)
public class ReleaseResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static ReleaseResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isSuccess();

        @JsProperty
        void setSuccess(boolean success);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static ReleaseResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isSuccess();

        @JsProperty
        void setSuccess(boolean success);
    }

    public static native ReleaseResponse deserializeBinary(Uint8Array bytes);

    public static native ReleaseResponse deserializeBinaryFromReader(
        ReleaseResponse message, Object reader);

    public static native void serializeBinaryToWriter(ReleaseResponse message, Object writer);

    public static native ReleaseResponse.ToObjectReturnType toObject(
        boolean includeInstance, ReleaseResponse msg);

    public native boolean getSuccess();

    public native Uint8Array serializeBinary();

    public native void setSuccess(boolean value);

    public native ReleaseResponse.ToObjectReturnType0 toObject();

    public native ReleaseResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
