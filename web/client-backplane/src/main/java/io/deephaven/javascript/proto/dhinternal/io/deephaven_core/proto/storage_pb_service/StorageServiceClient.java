//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.CreateDirectoryRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.CreateDirectoryResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.DeleteItemRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.DeleteItemResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.FetchFileRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.FetchFileResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.ListItemsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.ListItemsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.MoveItemRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.MoveItemResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.SaveFileRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.storage_pb.SaveFileResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.storage_pb_service.StorageServiceClient",
        namespace = JsPackage.GLOBAL)
public class StorageServiceClient {
    @JsFunction
    public interface CreateDirectoryCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.CreateDirectoryCallbackFn.P0Type create() {
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
                StorageServiceClient.CreateDirectoryCallbackFn.P0Type p0, CreateDirectoryResponse p1);
    }

    @JsFunction
    public interface CreateDirectoryMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.CreateDirectoryMetadata_or_callbackFn.P0Type create() {
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
                StorageServiceClient.CreateDirectoryMetadata_or_callbackFn.P0Type p0,
                CreateDirectoryResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateDirectoryMetadata_or_callbackUnionType {
        @JsOverlay
        static StorageServiceClient.CreateDirectoryMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default StorageServiceClient.CreateDirectoryMetadata_or_callbackFn asCreateDirectoryMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isCreateDirectoryMetadata_or_callbackFn() {
            return (Object) this instanceof StorageServiceClient.CreateDirectoryMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface DeleteItemCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.DeleteItemCallbackFn.P0Type create() {
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

        void onInvoke(StorageServiceClient.DeleteItemCallbackFn.P0Type p0, DeleteItemResponse p1);
    }

    @JsFunction
    public interface DeleteItemMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.DeleteItemMetadata_or_callbackFn.P0Type create() {
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
                StorageServiceClient.DeleteItemMetadata_or_callbackFn.P0Type p0, DeleteItemResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DeleteItemMetadata_or_callbackUnionType {
        @JsOverlay
        static StorageServiceClient.DeleteItemMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default StorageServiceClient.DeleteItemMetadata_or_callbackFn asDeleteItemMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isDeleteItemMetadata_or_callbackFn() {
            return (Object) this instanceof StorageServiceClient.DeleteItemMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface FetchFileCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.FetchFileCallbackFn.P0Type create() {
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

        void onInvoke(StorageServiceClient.FetchFileCallbackFn.P0Type p0, FetchFileResponse p1);
    }

    @JsFunction
    public interface FetchFileMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.FetchFileMetadata_or_callbackFn.P0Type create() {
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
                StorageServiceClient.FetchFileMetadata_or_callbackFn.P0Type p0, FetchFileResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FetchFileMetadata_or_callbackUnionType {
        @JsOverlay
        static StorageServiceClient.FetchFileMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default StorageServiceClient.FetchFileMetadata_or_callbackFn asFetchFileMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFetchFileMetadata_or_callbackFn() {
            return (Object) this instanceof StorageServiceClient.FetchFileMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface ListItemsCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.ListItemsCallbackFn.P0Type create() {
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

        void onInvoke(StorageServiceClient.ListItemsCallbackFn.P0Type p0, ListItemsResponse p1);
    }

    @JsFunction
    public interface ListItemsMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.ListItemsMetadata_or_callbackFn.P0Type create() {
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
                StorageServiceClient.ListItemsMetadata_or_callbackFn.P0Type p0, ListItemsResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ListItemsMetadata_or_callbackUnionType {
        @JsOverlay
        static StorageServiceClient.ListItemsMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default StorageServiceClient.ListItemsMetadata_or_callbackFn asListItemsMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isListItemsMetadata_or_callbackFn() {
            return (Object) this instanceof StorageServiceClient.ListItemsMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface MoveItemCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.MoveItemCallbackFn.P0Type create() {
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

        void onInvoke(StorageServiceClient.MoveItemCallbackFn.P0Type p0, MoveItemResponse p1);
    }

    @JsFunction
    public interface MoveItemMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.MoveItemMetadata_or_callbackFn.P0Type create() {
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
                StorageServiceClient.MoveItemMetadata_or_callbackFn.P0Type p0, MoveItemResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface MoveItemMetadata_or_callbackUnionType {
        @JsOverlay
        static StorageServiceClient.MoveItemMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default StorageServiceClient.MoveItemMetadata_or_callbackFn asMoveItemMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isMoveItemMetadata_or_callbackFn() {
            return (Object) this instanceof StorageServiceClient.MoveItemMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface SaveFileCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.SaveFileCallbackFn.P0Type create() {
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

        void onInvoke(StorageServiceClient.SaveFileCallbackFn.P0Type p0, SaveFileResponse p1);
    }

    @JsFunction
    public interface SaveFileMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static StorageServiceClient.SaveFileMetadata_or_callbackFn.P0Type create() {
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
                StorageServiceClient.SaveFileMetadata_or_callbackFn.P0Type p0, SaveFileResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SaveFileMetadata_or_callbackUnionType {
        @JsOverlay
        static StorageServiceClient.SaveFileMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default StorageServiceClient.SaveFileMetadata_or_callbackFn asSaveFileMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isSaveFileMetadata_or_callbackFn() {
            return (Object) this instanceof StorageServiceClient.SaveFileMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public StorageServiceClient(String serviceHost, Object options) {}

    public StorageServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            StorageServiceClient.CreateDirectoryCallbackFn callback) {
        return createDirectory(
                requestMessage,
                Js.<StorageServiceClient.CreateDirectoryMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return createDirectory(
                requestMessage,
                Js.<StorageServiceClient.CreateDirectoryMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            StorageServiceClient.CreateDirectoryMetadata_or_callbackFn metadata_or_callback,
            StorageServiceClient.CreateDirectoryCallbackFn callback) {
        return createDirectory(
                requestMessage,
                Js.<StorageServiceClient.CreateDirectoryMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            StorageServiceClient.CreateDirectoryMetadata_or_callbackFn metadata_or_callback) {
        return createDirectory(
                requestMessage,
                Js.<StorageServiceClient.CreateDirectoryMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            StorageServiceClient.CreateDirectoryMetadata_or_callbackUnionType metadata_or_callback,
            StorageServiceClient.CreateDirectoryCallbackFn callback);

    public native UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            StorageServiceClient.CreateDirectoryMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            StorageServiceClient.DeleteItemCallbackFn callback) {
        return deleteItem(
                requestMessage,
                Js.<StorageServiceClient.DeleteItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse deleteItem(
            DeleteItemRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return deleteItem(
                requestMessage,
                Js.<StorageServiceClient.DeleteItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            StorageServiceClient.DeleteItemMetadata_or_callbackFn metadata_or_callback,
            StorageServiceClient.DeleteItemCallbackFn callback) {
        return deleteItem(
                requestMessage,
                Js.<StorageServiceClient.DeleteItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            StorageServiceClient.DeleteItemMetadata_or_callbackFn metadata_or_callback) {
        return deleteItem(
                requestMessage,
                Js.<StorageServiceClient.DeleteItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            StorageServiceClient.DeleteItemMetadata_or_callbackUnionType metadata_or_callback,
            StorageServiceClient.DeleteItemCallbackFn callback);

    public native UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            StorageServiceClient.DeleteItemMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            StorageServiceClient.FetchFileCallbackFn callback) {
        return fetchFile(
                requestMessage,
                Js.<StorageServiceClient.FetchFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse fetchFile(
            FetchFileRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return fetchFile(
                requestMessage,
                Js.<StorageServiceClient.FetchFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            StorageServiceClient.FetchFileMetadata_or_callbackFn metadata_or_callback,
            StorageServiceClient.FetchFileCallbackFn callback) {
        return fetchFile(
                requestMessage,
                Js.<StorageServiceClient.FetchFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            StorageServiceClient.FetchFileMetadata_or_callbackFn metadata_or_callback) {
        return fetchFile(
                requestMessage,
                Js.<StorageServiceClient.FetchFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            StorageServiceClient.FetchFileMetadata_or_callbackUnionType metadata_or_callback,
            StorageServiceClient.FetchFileCallbackFn callback);

    public native UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            StorageServiceClient.FetchFileMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse listItems(
            ListItemsRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            StorageServiceClient.ListItemsCallbackFn callback) {
        return listItems(
                requestMessage,
                Js.<StorageServiceClient.ListItemsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse listItems(
            ListItemsRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return listItems(
                requestMessage,
                Js.<StorageServiceClient.ListItemsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse listItems(
            ListItemsRequest requestMessage,
            StorageServiceClient.ListItemsMetadata_or_callbackFn metadata_or_callback,
            StorageServiceClient.ListItemsCallbackFn callback) {
        return listItems(
                requestMessage,
                Js.<StorageServiceClient.ListItemsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse listItems(
            ListItemsRequest requestMessage,
            StorageServiceClient.ListItemsMetadata_or_callbackFn metadata_or_callback) {
        return listItems(
                requestMessage,
                Js.<StorageServiceClient.ListItemsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse listItems(
            ListItemsRequest requestMessage,
            StorageServiceClient.ListItemsMetadata_or_callbackUnionType metadata_or_callback,
            StorageServiceClient.ListItemsCallbackFn callback);

    public native UnaryResponse listItems(
            ListItemsRequest requestMessage,
            StorageServiceClient.ListItemsMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            StorageServiceClient.MoveItemCallbackFn callback) {
        return moveItem(
                requestMessage,
                Js.<StorageServiceClient.MoveItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse moveItem(
            MoveItemRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return moveItem(
                requestMessage,
                Js.<StorageServiceClient.MoveItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            StorageServiceClient.MoveItemMetadata_or_callbackFn metadata_or_callback,
            StorageServiceClient.MoveItemCallbackFn callback) {
        return moveItem(
                requestMessage,
                Js.<StorageServiceClient.MoveItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            StorageServiceClient.MoveItemMetadata_or_callbackFn metadata_or_callback) {
        return moveItem(
                requestMessage,
                Js.<StorageServiceClient.MoveItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            StorageServiceClient.MoveItemMetadata_or_callbackUnionType metadata_or_callback,
            StorageServiceClient.MoveItemCallbackFn callback);

    public native UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            StorageServiceClient.MoveItemMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            StorageServiceClient.SaveFileCallbackFn callback) {
        return saveFile(
                requestMessage,
                Js.<StorageServiceClient.SaveFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse saveFile(
            SaveFileRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return saveFile(
                requestMessage,
                Js.<StorageServiceClient.SaveFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            StorageServiceClient.SaveFileMetadata_or_callbackFn metadata_or_callback,
            StorageServiceClient.SaveFileCallbackFn callback) {
        return saveFile(
                requestMessage,
                Js.<StorageServiceClient.SaveFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            StorageServiceClient.SaveFileMetadata_or_callbackFn metadata_or_callback) {
        return saveFile(
                requestMessage,
                Js.<StorageServiceClient.SaveFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            StorageServiceClient.SaveFileMetadata_or_callbackUnionType metadata_or_callback,
            StorageServiceClient.SaveFileCallbackFn callback);

    public native UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            StorageServiceClient.SaveFileMetadata_or_callbackUnionType metadata_or_callback);
}
