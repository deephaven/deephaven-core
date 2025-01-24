//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.GetTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.MergeRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.PartitionByRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.PartitionByResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExportedTableCreationResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.partitionedtable_pb_service.PartitionedTableServiceClient",
        namespace = JsPackage.GLOBAL)
public class PartitionedTableServiceClient {
    @JsFunction
    public interface GetTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static PartitionedTableServiceClient.GetTableCallbackFn.P0Type create() {
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
                PartitionedTableServiceClient.GetTableCallbackFn.P0Type p0,
                ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface GetTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static PartitionedTableServiceClient.GetTableMetadata_or_callbackFn.P0Type create() {
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
                PartitionedTableServiceClient.GetTableMetadata_or_callbackFn.P0Type p0,
                ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetTableMetadata_or_callbackUnionType {
        @JsOverlay
        static PartitionedTableServiceClient.GetTableMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default PartitionedTableServiceClient.GetTableMetadata_or_callbackFn asGetTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isGetTableMetadata_or_callbackFn() {
            return (Object) this instanceof PartitionedTableServiceClient.GetTableMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface MergeCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static PartitionedTableServiceClient.MergeCallbackFn.P0Type create() {
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
                PartitionedTableServiceClient.MergeCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface MergeMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static PartitionedTableServiceClient.MergeMetadata_or_callbackFn.P0Type create() {
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
                PartitionedTableServiceClient.MergeMetadata_or_callbackFn.P0Type p0,
                ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface MergeMetadata_or_callbackUnionType {
        @JsOverlay
        static PartitionedTableServiceClient.MergeMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default PartitionedTableServiceClient.MergeMetadata_or_callbackFn asMergeMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isMergeMetadata_or_callbackFn() {
            return (Object) this instanceof PartitionedTableServiceClient.MergeMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface PartitionByCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static PartitionedTableServiceClient.PartitionByCallbackFn.P0Type create() {
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
                PartitionedTableServiceClient.PartitionByCallbackFn.P0Type p0, PartitionByResponse p1);
    }

    @JsFunction
    public interface PartitionByMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static PartitionedTableServiceClient.PartitionByMetadata_or_callbackFn.P0Type create() {
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
                PartitionedTableServiceClient.PartitionByMetadata_or_callbackFn.P0Type p0,
                PartitionByResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface PartitionByMetadata_or_callbackUnionType {
        @JsOverlay
        static PartitionedTableServiceClient.PartitionByMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default PartitionedTableServiceClient.PartitionByMetadata_or_callbackFn asPartitionByMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isPartitionByMetadata_or_callbackFn() {
            return (Object) this instanceof PartitionedTableServiceClient.PartitionByMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public PartitionedTableServiceClient(String serviceHost, Object options) {}

    public PartitionedTableServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse getTable(
            GetTableRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            PartitionedTableServiceClient.GetTableCallbackFn callback) {
        return getTable(
                requestMessage,
                Js.<PartitionedTableServiceClient.GetTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse getTable(
            GetTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return getTable(
                requestMessage,
                Js.<PartitionedTableServiceClient.GetTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse getTable(
            GetTableRequest requestMessage,
            PartitionedTableServiceClient.GetTableMetadata_or_callbackFn metadata_or_callback,
            PartitionedTableServiceClient.GetTableCallbackFn callback) {
        return getTable(
                requestMessage,
                Js.<PartitionedTableServiceClient.GetTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse getTable(
            GetTableRequest requestMessage,
            PartitionedTableServiceClient.GetTableMetadata_or_callbackFn metadata_or_callback) {
        return getTable(
                requestMessage,
                Js.<PartitionedTableServiceClient.GetTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse getTable(
            GetTableRequest requestMessage,
            PartitionedTableServiceClient.GetTableMetadata_or_callbackUnionType metadata_or_callback,
            PartitionedTableServiceClient.GetTableCallbackFn callback);

    public native UnaryResponse getTable(
            GetTableRequest requestMessage,
            PartitionedTableServiceClient.GetTableMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse merge(
            MergeRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            PartitionedTableServiceClient.MergeCallbackFn callback) {
        return merge(
                requestMessage,
                Js.<PartitionedTableServiceClient.MergeMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse merge(
            MergeRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return merge(
                requestMessage,
                Js.<PartitionedTableServiceClient.MergeMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse merge(
            MergeRequest requestMessage,
            PartitionedTableServiceClient.MergeMetadata_or_callbackFn metadata_or_callback,
            PartitionedTableServiceClient.MergeCallbackFn callback) {
        return merge(
                requestMessage,
                Js.<PartitionedTableServiceClient.MergeMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse merge(
            MergeRequest requestMessage,
            PartitionedTableServiceClient.MergeMetadata_or_callbackFn metadata_or_callback) {
        return merge(
                requestMessage,
                Js.<PartitionedTableServiceClient.MergeMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse merge(
            MergeRequest requestMessage,
            PartitionedTableServiceClient.MergeMetadata_or_callbackUnionType metadata_or_callback,
            PartitionedTableServiceClient.MergeCallbackFn callback);

    public native UnaryResponse merge(
            MergeRequest requestMessage,
            PartitionedTableServiceClient.MergeMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse partitionBy(
            PartitionByRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            PartitionedTableServiceClient.PartitionByCallbackFn callback) {
        return partitionBy(
                requestMessage,
                Js.<PartitionedTableServiceClient.PartitionByMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse partitionBy(
            PartitionByRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return partitionBy(
                requestMessage,
                Js.<PartitionedTableServiceClient.PartitionByMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse partitionBy(
            PartitionByRequest requestMessage,
            PartitionedTableServiceClient.PartitionByMetadata_or_callbackFn metadata_or_callback,
            PartitionedTableServiceClient.PartitionByCallbackFn callback) {
        return partitionBy(
                requestMessage,
                Js.<PartitionedTableServiceClient.PartitionByMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse partitionBy(
            PartitionByRequest requestMessage,
            PartitionedTableServiceClient.PartitionByMetadata_or_callbackFn metadata_or_callback) {
        return partitionBy(
                requestMessage,
                Js.<PartitionedTableServiceClient.PartitionByMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse partitionBy(
            PartitionByRequest requestMessage,
            PartitionedTableServiceClient.PartitionByMetadata_or_callbackUnionType metadata_or_callback,
            PartitionedTableServiceClient.PartitionByCallbackFn callback);

    public native UnaryResponse partitionBy(
            PartitionByRequest requestMessage,
            PartitionedTableServiceClient.PartitionByMetadata_or_callbackUnionType metadata_or_callback);
}
