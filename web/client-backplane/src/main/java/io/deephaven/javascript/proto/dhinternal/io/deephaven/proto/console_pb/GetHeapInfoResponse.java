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
        name = "dhinternal.io.deephaven.proto.console_pb.GetHeapInfoResponse",
        namespace = JsPackage.GLOBAL)
public class GetHeapInfoResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InfoFieldType {
            @JsOverlay
            static GetHeapInfoResponse.ToObjectReturnType.InfoFieldType create() {
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

        @JsOverlay
        static GetHeapInfoResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getErrorMessage();

        @JsProperty
        GetHeapInfoResponse.ToObjectReturnType.InfoFieldType getInfo();

        @JsProperty
        void setErrorMessage(String errorMessage);

        @JsProperty
        void setInfo(GetHeapInfoResponse.ToObjectReturnType.InfoFieldType info);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface InfoFieldType {
            @JsOverlay
            static GetHeapInfoResponse.ToObjectReturnType0.InfoFieldType create() {
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

        @JsOverlay
        static GetHeapInfoResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getErrorMessage();

        @JsProperty
        GetHeapInfoResponse.ToObjectReturnType0.InfoFieldType getInfo();

        @JsProperty
        void setErrorMessage(String errorMessage);

        @JsProperty
        void setInfo(GetHeapInfoResponse.ToObjectReturnType0.InfoFieldType info);
    }

    public static native GetHeapInfoResponse deserializeBinary(Uint8Array bytes);

    public static native GetHeapInfoResponse deserializeBinaryFromReader(
            GetHeapInfoResponse message, Object reader);

    public static native void serializeBinaryToWriter(GetHeapInfoResponse message, Object writer);

    public static native GetHeapInfoResponse.ToObjectReturnType toObject(
            boolean includeInstance, GetHeapInfoResponse msg);

    public native void clearInfo();

    public native String getErrorMessage();

    public native HeapInfo getInfo();

    public native boolean hasInfo();

    public native Uint8Array serializeBinary();

    public native void setErrorMessage(String value);

    public native void setInfo();

    public native void setInfo(HeapInfo value);

    public native GetHeapInfoResponse.ToObjectReturnType0 toObject();

    public native GetHeapInfoResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
