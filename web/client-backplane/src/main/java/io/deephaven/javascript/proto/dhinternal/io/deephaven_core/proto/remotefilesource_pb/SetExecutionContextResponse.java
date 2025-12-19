//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.SetExecutionContextResponse",
        namespace = JsPackage.GLOBAL)
public class SetExecutionContextResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static SetExecutionContextResponse.ToObjectReturnType create() {
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
        static SetExecutionContextResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        boolean isSuccess();

        @JsProperty
        void setSuccess(boolean success);
    }

    public static native SetExecutionContextResponse deserializeBinary(Uint8Array bytes);

    public static native SetExecutionContextResponse deserializeBinaryFromReader(
            SetExecutionContextResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            SetExecutionContextResponse message, Object writer);

    public static native SetExecutionContextResponse.ToObjectReturnType toObject(
            boolean includeInstance, SetExecutionContextResponse msg);

    public native boolean getSuccess();

    public native Uint8Array serializeBinary();

    public native void setSuccess(boolean value);

    public native SetExecutionContextResponse.ToObjectReturnType0 toObject();

    public native SetExecutionContextResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
