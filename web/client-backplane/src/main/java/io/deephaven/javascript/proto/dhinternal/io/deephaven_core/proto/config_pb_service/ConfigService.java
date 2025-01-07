//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.config_pb_service;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.config_pb_service.ConfigService",
        namespace = JsPackage.GLOBAL)
public class ConfigService {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetAuthenticationConstantsType {
        @JsOverlay
        static ConfigService.GetAuthenticationConstantsType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getMethodName();

        @JsProperty
        Object getRequestType();

        @JsProperty
        Object getResponseType();

        @JsProperty
        Object getService();

        @JsProperty
        boolean isRequestStream();

        @JsProperty
        boolean isResponseStream();

        @JsProperty
        void setMethodName(String methodName);

        @JsProperty
        void setRequestStream(boolean requestStream);

        @JsProperty
        void setRequestType(Object requestType);

        @JsProperty
        void setResponseStream(boolean responseStream);

        @JsProperty
        void setResponseType(Object responseType);

        @JsProperty
        void setService(Object service);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetConfigurationConstantsType {
        @JsOverlay
        static ConfigService.GetConfigurationConstantsType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getMethodName();

        @JsProperty
        Object getRequestType();

        @JsProperty
        Object getResponseType();

        @JsProperty
        Object getService();

        @JsProperty
        boolean isRequestStream();

        @JsProperty
        boolean isResponseStream();

        @JsProperty
        void setMethodName(String methodName);

        @JsProperty
        void setRequestStream(boolean requestStream);

        @JsProperty
        void setRequestType(Object requestType);

        @JsProperty
        void setResponseStream(boolean responseStream);

        @JsProperty
        void setResponseType(Object responseType);

        @JsProperty
        void setService(Object service);
    }

    public static ConfigService.GetAuthenticationConstantsType GetAuthenticationConstants;
    public static ConfigService.GetConfigurationConstantsType GetConfigurationConstants;
    public static String serviceName;
}
