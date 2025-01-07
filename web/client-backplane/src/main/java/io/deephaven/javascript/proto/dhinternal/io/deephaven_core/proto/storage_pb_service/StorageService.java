//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb_service;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.storage_pb_service.StorageService",
        namespace = JsPackage.GLOBAL)
public class StorageService {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateDirectoryType {
        @JsOverlay
        static StorageService.CreateDirectoryType create() {
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
    public interface DeleteItemType {
        @JsOverlay
        static StorageService.DeleteItemType create() {
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
    public interface FetchFileType {
        @JsOverlay
        static StorageService.FetchFileType create() {
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
    public interface ListItemsType {
        @JsOverlay
        static StorageService.ListItemsType create() {
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
    public interface MoveItemType {
        @JsOverlay
        static StorageService.MoveItemType create() {
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
    public interface SaveFileType {
        @JsOverlay
        static StorageService.SaveFileType create() {
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

    public static StorageService.CreateDirectoryType CreateDirectory;
    public static StorageService.DeleteItemType DeleteItem;
    public static StorageService.FetchFileType FetchFile;
    public static StorageService.ListItemsType ListItems;
    public static StorageService.MoveItemType MoveItem;
    public static StorageService.SaveFileType SaveFile;
    public static String serviceName;
}
