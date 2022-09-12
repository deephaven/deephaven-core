package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.CreateDirectoryRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.CreateDirectoryResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.DeleteItemRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.DeleteItemResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.FetchFileRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.FetchFileResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.ListItemsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.ListItemsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.MoveItemRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.MoveItemResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.SaveFileRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.notebook_pb.SaveFileResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.notebook_pb_service.NotebookServiceClient",
        namespace = JsPackage.GLOBAL)
public class NotebookServiceClient {
    @JsFunction
    public interface CreateDirectoryCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.CreateDirectoryCallbackFn.P0Type create() {
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
                NotebookServiceClient.CreateDirectoryCallbackFn.P0Type p0, CreateDirectoryResponse p1);
    }

    @JsFunction
    public interface CreateDirectoryMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.CreateDirectoryMetadata_or_callbackFn.P0Type create() {
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
                NotebookServiceClient.CreateDirectoryMetadata_or_callbackFn.P0Type p0,
                CreateDirectoryResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CreateDirectoryMetadata_or_callbackUnionType {
        @JsOverlay
        static NotebookServiceClient.CreateDirectoryMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default NotebookServiceClient.CreateDirectoryMetadata_or_callbackFn asCreateDirectoryMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isCreateDirectoryMetadata_or_callbackFn() {
            return (Object) this instanceof NotebookServiceClient.CreateDirectoryMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface DeleteItemCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.DeleteItemCallbackFn.P0Type create() {
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

        void onInvoke(NotebookServiceClient.DeleteItemCallbackFn.P0Type p0, DeleteItemResponse p1);
    }

    @JsFunction
    public interface DeleteItemMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.DeleteItemMetadata_or_callbackFn.P0Type create() {
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
                NotebookServiceClient.DeleteItemMetadata_or_callbackFn.P0Type p0, DeleteItemResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DeleteItemMetadata_or_callbackUnionType {
        @JsOverlay
        static NotebookServiceClient.DeleteItemMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default NotebookServiceClient.DeleteItemMetadata_or_callbackFn asDeleteItemMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isDeleteItemMetadata_or_callbackFn() {
            return (Object) this instanceof NotebookServiceClient.DeleteItemMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface FetchFileCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.FetchFileCallbackFn.P0Type create() {
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

        void onInvoke(NotebookServiceClient.FetchFileCallbackFn.P0Type p0, FetchFileResponse p1);
    }

    @JsFunction
    public interface FetchFileMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.FetchFileMetadata_or_callbackFn.P0Type create() {
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
                NotebookServiceClient.FetchFileMetadata_or_callbackFn.P0Type p0, FetchFileResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FetchFileMetadata_or_callbackUnionType {
        @JsOverlay
        static NotebookServiceClient.FetchFileMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default NotebookServiceClient.FetchFileMetadata_or_callbackFn asFetchFileMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFetchFileMetadata_or_callbackFn() {
            return (Object) this instanceof NotebookServiceClient.FetchFileMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface ListItemsCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.ListItemsCallbackFn.P0Type create() {
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

        void onInvoke(NotebookServiceClient.ListItemsCallbackFn.P0Type p0, ListItemsResponse p1);
    }

    @JsFunction
    public interface ListItemsMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.ListItemsMetadata_or_callbackFn.P0Type create() {
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
                NotebookServiceClient.ListItemsMetadata_or_callbackFn.P0Type p0, ListItemsResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ListItemsMetadata_or_callbackUnionType {
        @JsOverlay
        static NotebookServiceClient.ListItemsMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default NotebookServiceClient.ListItemsMetadata_or_callbackFn asListItemsMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isListItemsMetadata_or_callbackFn() {
            return (Object) this instanceof NotebookServiceClient.ListItemsMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface MoveItemCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.MoveItemCallbackFn.P0Type create() {
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

        void onInvoke(NotebookServiceClient.MoveItemCallbackFn.P0Type p0, MoveItemResponse p1);
    }

    @JsFunction
    public interface MoveItemMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.MoveItemMetadata_or_callbackFn.P0Type create() {
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
                NotebookServiceClient.MoveItemMetadata_or_callbackFn.P0Type p0, MoveItemResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface MoveItemMetadata_or_callbackUnionType {
        @JsOverlay
        static NotebookServiceClient.MoveItemMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default NotebookServiceClient.MoveItemMetadata_or_callbackFn asMoveItemMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isMoveItemMetadata_or_callbackFn() {
            return (Object) this instanceof NotebookServiceClient.MoveItemMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface SaveFileCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.SaveFileCallbackFn.P0Type create() {
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

        void onInvoke(NotebookServiceClient.SaveFileCallbackFn.P0Type p0, SaveFileResponse p1);
    }

    @JsFunction
    public interface SaveFileMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static NotebookServiceClient.SaveFileMetadata_or_callbackFn.P0Type create() {
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
                NotebookServiceClient.SaveFileMetadata_or_callbackFn.P0Type p0, SaveFileResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SaveFileMetadata_or_callbackUnionType {
        @JsOverlay
        static NotebookServiceClient.SaveFileMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default NotebookServiceClient.SaveFileMetadata_or_callbackFn asSaveFileMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isSaveFileMetadata_or_callbackFn() {
            return (Object) this instanceof NotebookServiceClient.SaveFileMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public NotebookServiceClient(String serviceHost, Object options) {}

    public NotebookServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            NotebookServiceClient.CreateDirectoryCallbackFn callback) {
        return createDirectory(
                requestMessage,
                Js.<NotebookServiceClient.CreateDirectoryMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return createDirectory(
                requestMessage,
                Js.<NotebookServiceClient.CreateDirectoryMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            NotebookServiceClient.CreateDirectoryMetadata_or_callbackFn metadata_or_callback,
            NotebookServiceClient.CreateDirectoryCallbackFn callback) {
        return createDirectory(
                requestMessage,
                Js.<NotebookServiceClient.CreateDirectoryMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            NotebookServiceClient.CreateDirectoryMetadata_or_callbackFn metadata_or_callback) {
        return createDirectory(
                requestMessage,
                Js.<NotebookServiceClient.CreateDirectoryMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            NotebookServiceClient.CreateDirectoryMetadata_or_callbackUnionType metadata_or_callback,
            NotebookServiceClient.CreateDirectoryCallbackFn callback);

    public native UnaryResponse createDirectory(
            CreateDirectoryRequest requestMessage,
            NotebookServiceClient.CreateDirectoryMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            NotebookServiceClient.DeleteItemCallbackFn callback) {
        return deleteItem(
                requestMessage,
                Js.<NotebookServiceClient.DeleteItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse deleteItem(
            DeleteItemRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return deleteItem(
                requestMessage,
                Js.<NotebookServiceClient.DeleteItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            NotebookServiceClient.DeleteItemMetadata_or_callbackFn metadata_or_callback,
            NotebookServiceClient.DeleteItemCallbackFn callback) {
        return deleteItem(
                requestMessage,
                Js.<NotebookServiceClient.DeleteItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            NotebookServiceClient.DeleteItemMetadata_or_callbackFn metadata_or_callback) {
        return deleteItem(
                requestMessage,
                Js.<NotebookServiceClient.DeleteItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            NotebookServiceClient.DeleteItemMetadata_or_callbackUnionType metadata_or_callback,
            NotebookServiceClient.DeleteItemCallbackFn callback);

    public native UnaryResponse deleteItem(
            DeleteItemRequest requestMessage,
            NotebookServiceClient.DeleteItemMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            NotebookServiceClient.FetchFileCallbackFn callback) {
        return fetchFile(
                requestMessage,
                Js.<NotebookServiceClient.FetchFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse fetchFile(
            FetchFileRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return fetchFile(
                requestMessage,
                Js.<NotebookServiceClient.FetchFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            NotebookServiceClient.FetchFileMetadata_or_callbackFn metadata_or_callback,
            NotebookServiceClient.FetchFileCallbackFn callback) {
        return fetchFile(
                requestMessage,
                Js.<NotebookServiceClient.FetchFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            NotebookServiceClient.FetchFileMetadata_or_callbackFn metadata_or_callback) {
        return fetchFile(
                requestMessage,
                Js.<NotebookServiceClient.FetchFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            NotebookServiceClient.FetchFileMetadata_or_callbackUnionType metadata_or_callback,
            NotebookServiceClient.FetchFileCallbackFn callback);

    public native UnaryResponse fetchFile(
            FetchFileRequest requestMessage,
            NotebookServiceClient.FetchFileMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse listItems(
            ListItemsRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            NotebookServiceClient.ListItemsCallbackFn callback) {
        return listItems(
                requestMessage,
                Js.<NotebookServiceClient.ListItemsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse listItems(
            ListItemsRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return listItems(
                requestMessage,
                Js.<NotebookServiceClient.ListItemsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse listItems(
            ListItemsRequest requestMessage,
            NotebookServiceClient.ListItemsMetadata_or_callbackFn metadata_or_callback,
            NotebookServiceClient.ListItemsCallbackFn callback) {
        return listItems(
                requestMessage,
                Js.<NotebookServiceClient.ListItemsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse listItems(
            ListItemsRequest requestMessage,
            NotebookServiceClient.ListItemsMetadata_or_callbackFn metadata_or_callback) {
        return listItems(
                requestMessage,
                Js.<NotebookServiceClient.ListItemsMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse listItems(
            ListItemsRequest requestMessage,
            NotebookServiceClient.ListItemsMetadata_or_callbackUnionType metadata_or_callback,
            NotebookServiceClient.ListItemsCallbackFn callback);

    public native UnaryResponse listItems(
            ListItemsRequest requestMessage,
            NotebookServiceClient.ListItemsMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            NotebookServiceClient.MoveItemCallbackFn callback) {
        return moveItem(
                requestMessage,
                Js.<NotebookServiceClient.MoveItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse moveItem(
            MoveItemRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return moveItem(
                requestMessage,
                Js.<NotebookServiceClient.MoveItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            NotebookServiceClient.MoveItemMetadata_or_callbackFn metadata_or_callback,
            NotebookServiceClient.MoveItemCallbackFn callback) {
        return moveItem(
                requestMessage,
                Js.<NotebookServiceClient.MoveItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            NotebookServiceClient.MoveItemMetadata_or_callbackFn metadata_or_callback) {
        return moveItem(
                requestMessage,
                Js.<NotebookServiceClient.MoveItemMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            NotebookServiceClient.MoveItemMetadata_or_callbackUnionType metadata_or_callback,
            NotebookServiceClient.MoveItemCallbackFn callback);

    public native UnaryResponse moveItem(
            MoveItemRequest requestMessage,
            NotebookServiceClient.MoveItemMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            NotebookServiceClient.SaveFileCallbackFn callback) {
        return saveFile(
                requestMessage,
                Js.<NotebookServiceClient.SaveFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse saveFile(
            SaveFileRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return saveFile(
                requestMessage,
                Js.<NotebookServiceClient.SaveFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            NotebookServiceClient.SaveFileMetadata_or_callbackFn metadata_or_callback,
            NotebookServiceClient.SaveFileCallbackFn callback) {
        return saveFile(
                requestMessage,
                Js.<NotebookServiceClient.SaveFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            NotebookServiceClient.SaveFileMetadata_or_callbackFn metadata_or_callback) {
        return saveFile(
                requestMessage,
                Js.<NotebookServiceClient.SaveFileMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            NotebookServiceClient.SaveFileMetadata_or_callbackUnionType metadata_or_callback,
            NotebookServiceClient.SaveFileCallbackFn callback);

    public native UnaryResponse saveFile(
            SaveFileRequest requestMessage,
            NotebookServiceClient.SaveFileMetadata_or_callbackUnionType metadata_or_callback);
}
