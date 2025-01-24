//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb.AuthenticationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb.AuthenticationConstantsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb.ConfigurationConstantsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb.ConfigurationConstantsResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.config_pb_service.ConfigServiceClient",
        namespace = JsPackage.GLOBAL)
public class ConfigServiceClient {
    @JsFunction
    public interface GetAuthenticationConstantsCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConfigServiceClient.GetAuthenticationConstantsCallbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                ConfigServiceClient.GetAuthenticationConstantsCallbackFn.P0Type p0,
                AuthenticationConstantsResponse p1);
    }

    @JsFunction
    public interface GetAuthenticationConstantsMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackFn.P0Type p0,
                AuthenticationConstantsResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetAuthenticationConstantsMetadata_or_callbackUnionType {
        @JsOverlay
        static ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackUnionType of(
                Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackFn asGetAuthenticationConstantsMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isGetAuthenticationConstantsMetadata_or_callbackFn() {
            return (Object) this instanceof ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface GetConfigurationConstantsCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConfigServiceClient.GetConfigurationConstantsCallbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                ConfigServiceClient.GetConfigurationConstantsCallbackFn.P0Type p0,
                ConfigurationConstantsResponse p1);
    }

    @JsFunction
    public interface GetConfigurationConstantsMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackFn.P0Type p0,
                ConfigurationConstantsResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetConfigurationConstantsMetadata_or_callbackUnionType {
        @JsOverlay
        static ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackFn asGetConfigurationConstantsMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isGetConfigurationConstantsMetadata_or_callbackFn() {
            return (Object) this instanceof ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public ConfigServiceClient(String serviceHost, Object options) {}

    public ConfigServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse getAuthenticationConstants(
            AuthenticationConstantsRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            ConfigServiceClient.GetAuthenticationConstantsCallbackFn callback) {
        return getAuthenticationConstants(
                requestMessage,
                Js.<ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse getAuthenticationConstants(
            AuthenticationConstantsRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return getAuthenticationConstants(
                requestMessage,
                Js.<ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse getAuthenticationConstants(
            AuthenticationConstantsRequest requestMessage,
            ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackFn metadata_or_callback,
            ConfigServiceClient.GetAuthenticationConstantsCallbackFn callback) {
        return getAuthenticationConstants(
                requestMessage,
                Js.<ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse getAuthenticationConstants(
            AuthenticationConstantsRequest requestMessage,
            ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackFn metadata_or_callback) {
        return getAuthenticationConstants(
                requestMessage,
                Js.<ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse getAuthenticationConstants(
            AuthenticationConstantsRequest requestMessage,
            ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackUnionType metadata_or_callback,
            ConfigServiceClient.GetAuthenticationConstantsCallbackFn callback);

    public native UnaryResponse getAuthenticationConstants(
            AuthenticationConstantsRequest requestMessage,
            ConfigServiceClient.GetAuthenticationConstantsMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse getConfigurationConstants(
            ConfigurationConstantsRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            ConfigServiceClient.GetConfigurationConstantsCallbackFn callback) {
        return getConfigurationConstants(
                requestMessage,
                Js.<ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse getConfigurationConstants(
            ConfigurationConstantsRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return getConfigurationConstants(
                requestMessage,
                Js.<ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse getConfigurationConstants(
            ConfigurationConstantsRequest requestMessage,
            ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackFn metadata_or_callback,
            ConfigServiceClient.GetConfigurationConstantsCallbackFn callback) {
        return getConfigurationConstants(
                requestMessage,
                Js.<ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse getConfigurationConstants(
            ConfigurationConstantsRequest requestMessage,
            ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackFn metadata_or_callback) {
        return getConfigurationConstants(
                requestMessage,
                Js.<ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse getConfigurationConstants(
            ConfigurationConstantsRequest requestMessage,
            ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackUnionType metadata_or_callback,
            ConfigServiceClient.GetConfigurationConstantsCallbackFn callback);

    public native UnaryResponse getConfigurationConstants(
            ConfigurationConstantsRequest requestMessage,
            ConfigServiceClient.GetConfigurationConstantsMetadata_or_callbackUnionType metadata_or_callback);
}
