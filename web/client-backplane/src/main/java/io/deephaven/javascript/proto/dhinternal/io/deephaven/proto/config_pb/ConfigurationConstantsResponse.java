package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.config_pb;

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
        name = "dhinternal.io.deephaven.proto.config_pb.ConfigurationConstantsResponse",
        namespace = JsPackage.GLOBAL)
public class ConfigurationConstantsResponse {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConfigValuesListFieldType {
            @JsOverlay
            static ConfigurationConstantsResponse.ToObjectReturnType.ConfigValuesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getKey();

            @JsProperty
            String getStringValue();

            @JsProperty
            void setKey(String key);

            @JsProperty
            void setStringValue(String stringValue);
        }

        @JsOverlay
        static ConfigurationConstantsResponse.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ConfigurationConstantsResponse.ToObjectReturnType.ConfigValuesListFieldType> getConfigValuesList();

        @JsOverlay
        default void setConfigValuesList(
                ConfigurationConstantsResponse.ToObjectReturnType.ConfigValuesListFieldType[] configValuesList) {
            setConfigValuesList(
                    Js.<JsArray<ConfigurationConstantsResponse.ToObjectReturnType.ConfigValuesListFieldType>>uncheckedCast(
                            configValuesList));
        }

        @JsProperty
        void setConfigValuesList(
                JsArray<ConfigurationConstantsResponse.ToObjectReturnType.ConfigValuesListFieldType> configValuesList);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface ConfigValuesListFieldType {
            @JsOverlay
            static ConfigurationConstantsResponse.ToObjectReturnType0.ConfigValuesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getKey();

            @JsProperty
            String getStringValue();

            @JsProperty
            void setKey(String key);

            @JsProperty
            void setStringValue(String stringValue);
        }

        @JsOverlay
        static ConfigurationConstantsResponse.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<ConfigurationConstantsResponse.ToObjectReturnType0.ConfigValuesListFieldType> getConfigValuesList();

        @JsOverlay
        default void setConfigValuesList(
                ConfigurationConstantsResponse.ToObjectReturnType0.ConfigValuesListFieldType[] configValuesList) {
            setConfigValuesList(
                    Js.<JsArray<ConfigurationConstantsResponse.ToObjectReturnType0.ConfigValuesListFieldType>>uncheckedCast(
                            configValuesList));
        }

        @JsProperty
        void setConfigValuesList(
                JsArray<ConfigurationConstantsResponse.ToObjectReturnType0.ConfigValuesListFieldType> configValuesList);
    }

    public static native ConfigurationConstantsResponse deserializeBinary(Uint8Array bytes);

    public static native ConfigurationConstantsResponse deserializeBinaryFromReader(
            ConfigurationConstantsResponse message, Object reader);

    public static native void serializeBinaryToWriter(
            ConfigurationConstantsResponse message, Object writer);

    public static native ConfigurationConstantsResponse.ToObjectReturnType toObject(
            boolean includeInstance, ConfigurationConstantsResponse msg);

    public native ConfigPair addConfigValues();

    public native ConfigPair addConfigValues(ConfigPair value, double index);

    public native ConfigPair addConfigValues(ConfigPair value);

    public native void clearConfigValuesList();

    public native JsArray<ConfigPair> getConfigValuesList();

    public native Uint8Array serializeBinary();

    @JsOverlay
    public final void setConfigValuesList(ConfigPair[] value) {
        setConfigValuesList(Js.<JsArray<ConfigPair>>uncheckedCast(value));
    }

    public native void setConfigValuesList(JsArray<ConfigPair> value);

    public native ConfigurationConstantsResponse.ToObjectReturnType0 toObject();

    public native ConfigurationConstantsResponse.ToObjectReturnType0 toObject(
            boolean includeInstance);
}
