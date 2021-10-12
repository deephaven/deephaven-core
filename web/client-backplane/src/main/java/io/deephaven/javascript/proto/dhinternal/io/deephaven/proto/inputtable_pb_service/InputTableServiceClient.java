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
    public interface AddTablesToInputTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static InputTableServiceClient.AddTablesToInputTableCallbackFn.P0Type create() {
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
                InputTableServiceClient.AddTablesToInputTableCallbackFn.P0Type p0, AddTableResponse p1);
    }

    @JsFunction
    public interface AddTablesToInputTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackFn.P0Type create() {
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
                InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackFn.P0Type p0,
                AddTableResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AddTablesToInputTableMetadata_or_callbackUnionType {
        @JsOverlay
        static InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackFn asAddTablesToInputTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isAddTablesToInputTableMetadata_or_callbackFn() {
            return (Object) this instanceof InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackFn;
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }
    }

    @JsFunction
    public interface DeleteTablesFromInputTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static InputTableServiceClient.DeleteTablesFromInputTableCallbackFn.P0Type create() {
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
                InputTableServiceClient.DeleteTablesFromInputTableCallbackFn.P0Type p0,
                DeleteTableResponse p1);
    }

    @JsFunction
    public interface DeleteTablesFromInputTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackFn.P0Type create() {
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
                InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackFn.P0Type p0,
                DeleteTableResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DeleteTablesFromInputTableMetadata_or_callbackUnionType {
        @JsOverlay
        static InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackUnionType of(
                Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackFn asDeleteTablesFromInputTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isDeleteTablesFromInputTableMetadata_or_callbackFn() {
            return (Object) this instanceof InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public InputTableServiceClient(String serviceHost, Object options) {}

    public InputTableServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse addTablesToInputTable(
            AddTableRequest requestMessage,
            InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackFn metadata_or_callback,
            InputTableServiceClient.AddTablesToInputTableCallbackFn callback) {
        return addTablesToInputTable(
                requestMessage,
                Js.<InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse addTablesToInputTable(
            AddTableRequest requestMessage,
            InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackFn metadata_or_callback) {
        return addTablesToInputTable(
                requestMessage,
                Js.<InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse addTablesToInputTable(
            AddTableRequest requestMessage,
            InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackUnionType metadata_or_callback,
            InputTableServiceClient.AddTablesToInputTableCallbackFn callback);

    public native UnaryResponse addTablesToInputTable(
            AddTableRequest requestMessage,
            InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse addTablesToInputTable(
            AddTableRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            InputTableServiceClient.AddTablesToInputTableCallbackFn callback) {
        return addTablesToInputTable(
                requestMessage,
                Js.<InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse addTablesToInputTable(
            AddTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return addTablesToInputTable(
                requestMessage,
                Js.<InputTableServiceClient.AddTablesToInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse deleteTablesFromInputTable(
            DeleteTableRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            InputTableServiceClient.DeleteTablesFromInputTableCallbackFn callback) {
        return deleteTablesFromInputTable(
                requestMessage,
                Js.<InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse deleteTablesFromInputTable(
            DeleteTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return deleteTablesFromInputTable(
                requestMessage,
                Js.<InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse deleteTablesFromInputTable(
            DeleteTableRequest requestMessage,
            InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackFn metadata_or_callback,
            InputTableServiceClient.DeleteTablesFromInputTableCallbackFn callback) {
        return deleteTablesFromInputTable(
                requestMessage,
                Js.<InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse deleteTablesFromInputTable(
            DeleteTableRequest requestMessage,
            InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackFn metadata_or_callback) {
        return deleteTablesFromInputTable(
                requestMessage,
                Js.<InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse deleteTablesFromInputTable(
            DeleteTableRequest requestMessage,
            InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackUnionType metadata_or_callback,
            InputTableServiceClient.DeleteTablesFromInputTableCallbackFn callback);

    public native UnaryResponse deleteTablesFromInputTable(
            DeleteTableRequest requestMessage,
            InputTableServiceClient.DeleteTablesFromInputTableMetadata_or_callbackUnionType metadata_or_callback);
}
