package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb.AddTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb.AddTableResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb.DeleteTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.inputtable_pb.DeleteTableResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.inputtable_pb_service.InputTableServiceClient",
        namespace = JsPackage.GLOBAL)
public class InputTableServiceClient {
    @JsFunction
    public interface AddTableToInputTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static InputTableServiceClient.AddTableToInputTableCallbackFn.P0Type create() {
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
                InputTableServiceClient.AddTableToInputTableCallbackFn.P0Type p0, AddTableResponse p1);
    }

    @JsFunction
    public interface AddTableToInputTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static InputTableServiceClient.AddTableToInputTableMetadata_or_callbackFn.P0Type create() {
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
                InputTableServiceClient.AddTableToInputTableMetadata_or_callbackFn.P0Type p0,
                AddTableResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AddTableToInputTableMetadata_or_callbackUnionType {
        @JsOverlay
        static InputTableServiceClient.AddTableToInputTableMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default InputTableServiceClient.AddTableToInputTableMetadata_or_callbackFn asAddTableToInputTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isAddTableToInputTableMetadata_or_callbackFn() {
            return (Object) this instanceof InputTableServiceClient.AddTableToInputTableMetadata_or_callbackFn;
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }
    }

    @JsFunction
    public interface DeleteTableFromInputTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static InputTableServiceClient.DeleteTableFromInputTableCallbackFn.P0Type create() {
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
                InputTableServiceClient.DeleteTableFromInputTableCallbackFn.P0Type p0,
                DeleteTableResponse p1);
    }

    @JsFunction
    public interface DeleteTableFromInputTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackFn.P0Type create() {
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
                InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackFn.P0Type p0,
                DeleteTableResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DeleteTableFromInputTableMetadata_or_callbackUnionType {
        @JsOverlay
        static InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackUnionType of(
                Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackFn asDeleteTableFromInputTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isDeleteTableFromInputTableMetadata_or_callbackFn() {
            return (Object) this instanceof InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public InputTableServiceClient(String serviceHost, Object options) {}

    public InputTableServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse addTableToInputTable(
            AddTableRequest requestMessage,
            InputTableServiceClient.AddTableToInputTableMetadata_or_callbackFn metadata_or_callback,
            InputTableServiceClient.AddTableToInputTableCallbackFn callback) {
        return addTableToInputTable(
                requestMessage,
                Js.<InputTableServiceClient.AddTableToInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse addTableToInputTable(
            AddTableRequest requestMessage,
            InputTableServiceClient.AddTableToInputTableMetadata_or_callbackFn metadata_or_callback) {
        return addTableToInputTable(
                requestMessage,
                Js.<InputTableServiceClient.AddTableToInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse addTableToInputTable(
            AddTableRequest requestMessage,
            InputTableServiceClient.AddTableToInputTableMetadata_or_callbackUnionType metadata_or_callback,
            InputTableServiceClient.AddTableToInputTableCallbackFn callback);

    public native UnaryResponse addTableToInputTable(
            AddTableRequest requestMessage,
            InputTableServiceClient.AddTableToInputTableMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse addTableToInputTable(
            AddTableRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            InputTableServiceClient.AddTableToInputTableCallbackFn callback) {
        return addTableToInputTable(
                requestMessage,
                Js.<InputTableServiceClient.AddTableToInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse addTableToInputTable(
            AddTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return addTableToInputTable(
                requestMessage,
                Js.<InputTableServiceClient.AddTableToInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse deleteTableFromInputTable(
            DeleteTableRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            InputTableServiceClient.DeleteTableFromInputTableCallbackFn callback) {
        return deleteTableFromInputTable(
                requestMessage,
                Js.<InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse deleteTableFromInputTable(
            DeleteTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return deleteTableFromInputTable(
                requestMessage,
                Js.<InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse deleteTableFromInputTable(
            DeleteTableRequest requestMessage,
            InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackFn metadata_or_callback,
            InputTableServiceClient.DeleteTableFromInputTableCallbackFn callback) {
        return deleteTableFromInputTable(
                requestMessage,
                Js.<InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse deleteTableFromInputTable(
            DeleteTableRequest requestMessage,
            InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackFn metadata_or_callback) {
        return deleteTableFromInputTable(
                requestMessage,
                Js.<InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse deleteTableFromInputTable(
            DeleteTableRequest requestMessage,
            InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackUnionType metadata_or_callback,
            InputTableServiceClient.DeleteTableFromInputTableCallbackFn callback);

    public native UnaryResponse deleteTableFromInputTable(
            DeleteTableRequest requestMessage,
            InputTableServiceClient.DeleteTableFromInputTableMetadata_or_callbackUnionType metadata_or_callback);
}
