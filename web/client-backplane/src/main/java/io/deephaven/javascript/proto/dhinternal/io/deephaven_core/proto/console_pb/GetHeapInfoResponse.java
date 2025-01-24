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
        name = "dhinternal.io.deephaven_core.proto.console_pb.GetHeapInfoResponse",
        namespace = JsPackage.GLOBAL)
public class GetHeapInfoResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static GetHeapInfoResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getFreeMemory();

        @JsProperty
        String getMaxMemory();

        @JsProperty
        String getTotalMemory();

        @JsProperty
        void setFreeMemory(String freeMemory);

        @JsProperty
        void setMaxMemory(String maxMemory);

        @JsProperty
        void setTotalMemory(String totalMemory);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static GetHeapInfoResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getFreeMemory();

        @JsProperty
        String getMaxMemory();

        @JsProperty
        String getTotalMemory();

        @JsProperty
        void setFreeMemory(String freeMemory);

        @JsProperty
        void setMaxMemory(String maxMemory);

        @JsProperty
        void setTotalMemory(String totalMemory);
    }

    public static native GetHeapInfoResponse deserializeBinary(Uint8Array bytes);

    public static native GetHeapInfoResponse deserializeBinaryFromReader(
            GetHeapInfoResponse message, Object reader);

    public static native void serializeBinaryToWriter(GetHeapInfoResponse message, Object writer);

    public static native GetHeapInfoResponse.ToObjectReturnType toObject(
            boolean includeInstance, GetHeapInfoResponse msg);

    public native String getFreeMemory();

    public native String getMaxMemory();

    public native String getTotalMemory();

    public native Uint8Array serializeBinary();

    public native void setFreeMemory(String value);

    public native void setMaxMemory(String value);

    public native void setTotalMemory(String value);

    public native GetHeapInfoResponse.ToObjectReturnType0 toObject();

    public native GetHeapInfoResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
