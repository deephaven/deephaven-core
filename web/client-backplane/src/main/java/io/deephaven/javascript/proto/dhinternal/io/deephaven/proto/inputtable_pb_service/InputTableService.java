package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb_service;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.inputtable_pb_service.InputTableService",
        namespace = JsPackage.GLOBAL)
public class InputTableService {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AddTablesToInputTableType {
        @JsOverlay
        static InputTableService.AddTablesToInputTableType create() {
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
    public interface DeleteTablesFromInputTableType {
        @JsOverlay
        static InputTableService.DeleteTablesFromInputTableType create() {
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

    public static InputTableService.AddTablesToInputTableType AddTablesToInputTable;
    public static InputTableService.DeleteTablesFromInputTableType DeleteTablesFromInputTable;
    public static String serviceName;
}
