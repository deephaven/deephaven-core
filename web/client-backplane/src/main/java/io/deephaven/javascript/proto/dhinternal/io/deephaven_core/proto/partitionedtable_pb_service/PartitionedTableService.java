//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb_service;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.partitionedtable_pb_service.PartitionedTableService",
        namespace = JsPackage.GLOBAL)
public class PartitionedTableService {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetTableType {
        @JsOverlay
        static PartitionedTableService.GetTableType create() {
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
    public interface MergeType {
        @JsOverlay
        static PartitionedTableService.MergeType create() {
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
    public interface PartitionByType {
        @JsOverlay
        static PartitionedTableService.PartitionByType create() {
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

    public static PartitionedTableService.GetTableType GetTable;
    public static PartitionedTableService.MergeType Merge;
    public static PartitionedTableService.PartitionByType PartitionBy;
    public static String serviceName;
}
