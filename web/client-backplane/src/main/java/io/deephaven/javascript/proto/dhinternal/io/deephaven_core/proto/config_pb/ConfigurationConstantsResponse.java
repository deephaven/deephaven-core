//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.jspb.Map;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.config_pb.ConfigurationConstantsResponse",
        namespace = JsPackage.GLOBAL)
public class ConfigurationConstantsResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsOverlay
        static ConfigurationConstantsResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<JsArray<Object>> getConfigValuesMap();

        @JsProperty
        void setConfigValuesMap(JsArray<JsArray<Object>> configValuesMap);

        @JsOverlay
        default void setConfigValuesMap(Object[][] configValuesMap) {
            setConfigValuesMap(Js.<JsArray<JsArray<Object>>>uncheckedCast(configValuesMap));
        }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsOverlay
        static ConfigurationConstantsResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<JsArray<Object>> getConfigValuesMap();

        @JsProperty
        void setConfigValuesMap(JsArray<JsArray<Object>> configValuesMap);

        @JsOverlay
        default void setConfigValuesMap(Object[][] configValuesMap) {
            setConfigValuesMap(Js.<JsArray<JsArray<Object>>>uncheckedCast(configValuesMap));
        }
    }

    public static native ConfigurationConstantsResponse deserializeBinary(Uint8Array bytes);

    public static native ConfigurationConstantsResponse deserializeBinaryFromReader(
            ConfigurationConstantsResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            ConfigurationConstantsResponse message, Object writer);

    public static native ConfigurationConstantsResponse.ToObjectReturnType toObject(
            boolean includeInstance, ConfigurationConstantsResponse msg);

    public native void clearConfigValuesMap();

    public native Map<String, ConfigValue> getConfigValuesMap();

    public native Uint8Array serializeBinary();

    public native ConfigurationConstantsResponse.ToObjectReturnType0 toObject();

    public native ConfigurationConstantsResponse.ToObjectReturnType0 toObject(
            boolean includeInstance);
}
