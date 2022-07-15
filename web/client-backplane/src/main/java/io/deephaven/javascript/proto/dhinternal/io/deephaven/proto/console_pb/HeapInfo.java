package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.HeapInfo",
        namespace = JsPackage.GLOBAL)
public class HeapInfo {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static HeapInfo.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getFreeMemory();

        @JsProperty
        double getMaximumHeapSize();

        @JsProperty
        double getTotalHeapSize();

        @JsProperty
        void setFreeMemory(double freeMemory);

        @JsProperty
        void setMaximumHeapSize(double maximumHeapSize);

        @JsProperty
        void setTotalHeapSize(double totalHeapSize);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static HeapInfo.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getFreeMemory();

        @JsProperty
        double getMaximumHeapSize();

        @JsProperty
        double getTotalHeapSize();

        @JsProperty
        void setFreeMemory(double freeMemory);

        @JsProperty
        void setMaximumHeapSize(double maximumHeapSize);

        @JsProperty
        void setTotalHeapSize(double totalHeapSize);
    }

    public static native HeapInfo deserializeBinary(Uint8Array bytes);

    public static native HeapInfo deserializeBinaryFromReader(HeapInfo message, Object reader);

    public static native void serializeBinaryToWriter(HeapInfo message, Object writer);

    public static native HeapInfo.ToObjectReturnType toObject(boolean includeInstance, HeapInfo msg);

    public native double getFreeMemory();

    public native double getMaximumHeapSize();

    public native double getTotalHeapSize();

    public native Uint8Array serializeBinary();

    public native void setFreeMemory(double value);

    public native void setMaximumHeapSize(double value);

    public native void setTotalHeapSize(double value);

    public native HeapInfo.ToObjectReturnType0 toObject();

    public native HeapInfo.ToObjectReturnType0 toObject(boolean includeInstance);
}
