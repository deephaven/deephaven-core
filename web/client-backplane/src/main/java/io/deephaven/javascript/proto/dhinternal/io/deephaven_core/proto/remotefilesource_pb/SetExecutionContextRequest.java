//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.remotefilesource_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.remotefilesource_pb.SetExecutionContextRequest",
        namespace = JsPackage.GLOBAL)
public class SetExecutionContextRequest {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static SetExecutionContextRequest.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getResourcePathsList();

        @JsProperty
        void setResourcePathsList(JsArray<String> resourcePathsList);

        @JsOverlay
        default void setResourcePathsList(String[] resourcePathsList) {
            setResourcePathsList(Js.<JsArray<String>>uncheckedCast(resourcePathsList));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static SetExecutionContextRequest.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<String> getResourcePathsList();

        @JsProperty
        void setResourcePathsList(JsArray<String> resourcePathsList);

        @JsOverlay
        default void setResourcePathsList(String[] resourcePathsList) {
            setResourcePathsList(Js.<JsArray<String>>uncheckedCast(resourcePathsList));
        }
    }

    public static native SetExecutionContextRequest deserializeBinary(Uint8Array bytes);

    public static native SetExecutionContextRequest deserializeBinaryFromReader(
            SetExecutionContextRequest message, Object reader);

    public static native void serializeBinaryToWriter(
            SetExecutionContextRequest message, Object writer);

    public static native SetExecutionContextRequest.ToObjectReturnType toObject(
            boolean includeInstance, SetExecutionContextRequest msg);

    public native String addResourcePaths(String value, double index);

    public native String addResourcePaths(String value);

    public native void clearResourcePathsList();

    public native JsArray<String> getResourcePathsList();

    public native Uint8Array serializeBinary();

    public native void setResourcePathsList(JsArray<String> value);

    @JsOverlay
    public final void setResourcePathsList(String[] value) {
        setResourcePathsList(Js.<JsArray<String>>uncheckedCast(value));
    }

    public native SetExecutionContextRequest.ToObjectReturnType0 toObject();

    public native SetExecutionContextRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
