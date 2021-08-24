package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.BindTableToVariableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.BindTableToVariableResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.CancelCommandRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.CancelCommandResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.ChangeDocumentRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.ChangeDocumentResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.CloseDocumentRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.CloseDocumentResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.ExecuteCommandRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.ExecuteCommandResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchFigureRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchFigureResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchPandasTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchTableMapRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchTableMapResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetCompletionItemsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetCompletionItemsResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetConsoleTypesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetConsoleTypesResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.LogSubscriptionData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.LogSubscriptionRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.OpenDocumentRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.OpenDocumentResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.StartConsoleRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.StartConsoleResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb_service.ConsoleServiceClient",
    namespace = JsPackage.GLOBAL)
public class ConsoleServiceClient {
    @JsFunction
    public interface BindTableToVariableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.BindTableToVariableCallbackFn.P0Type create() {
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
            ConsoleServiceClient.BindTableToVariableCallbackFn.P0Type p0,
            BindTableToVariableResponse p1);
    }

    @JsFunction
    public interface BindTableToVariableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.BindTableToVariableMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.BindTableToVariableMetadata_or_callbackFn.P0Type p0,
            BindTableToVariableResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface BindTableToVariableMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.BindTableToVariableMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default ConsoleServiceClient.BindTableToVariableMetadata_or_callbackFn asBindTableToVariableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBindTableToVariableMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.BindTableToVariableMetadata_or_callbackFn;
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }
    }

    @JsFunction
    public interface CancelCommandCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.CancelCommandCallbackFn.P0Type create() {
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

        void onInvoke(ConsoleServiceClient.CancelCommandCallbackFn.P0Type p0,
            CancelCommandResponse p1);
    }

    @JsFunction
    public interface CancelCommandMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.CancelCommandMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.CancelCommandMetadata_or_callbackFn.P0Type p0,
            CancelCommandResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CancelCommandMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.CancelCommandMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.CancelCommandMetadata_or_callbackFn asCancelCommandMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isCancelCommandMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.CancelCommandMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface ChangeDocumentCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.ChangeDocumentCallbackFn.P0Type create() {
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
            ConsoleServiceClient.ChangeDocumentCallbackFn.P0Type p0, ChangeDocumentResponse p1);
    }

    @JsFunction
    public interface ChangeDocumentMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.ChangeDocumentMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.ChangeDocumentMetadata_or_callbackFn.P0Type p0,
            ChangeDocumentResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ChangeDocumentMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.ChangeDocumentMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.ChangeDocumentMetadata_or_callbackFn asChangeDocumentMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isChangeDocumentMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.ChangeDocumentMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface CloseDocumentCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.CloseDocumentCallbackFn.P0Type create() {
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

        void onInvoke(ConsoleServiceClient.CloseDocumentCallbackFn.P0Type p0,
            CloseDocumentResponse p1);
    }

    @JsFunction
    public interface CloseDocumentMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.CloseDocumentMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.CloseDocumentMetadata_or_callbackFn.P0Type p0,
            CloseDocumentResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CloseDocumentMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.CloseDocumentMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.CloseDocumentMetadata_or_callbackFn asCloseDocumentMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isCloseDocumentMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.CloseDocumentMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface ExecuteCommandCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.ExecuteCommandCallbackFn.P0Type create() {
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
            ConsoleServiceClient.ExecuteCommandCallbackFn.P0Type p0, ExecuteCommandResponse p1);
    }

    @JsFunction
    public interface ExecuteCommandMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.ExecuteCommandMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.ExecuteCommandMetadata_or_callbackFn.P0Type p0,
            ExecuteCommandResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ExecuteCommandMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.ExecuteCommandMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.ExecuteCommandMetadata_or_callbackFn asExecuteCommandMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isExecuteCommandMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.ExecuteCommandMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface FetchFigureCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.FetchFigureCallbackFn.P0Type create() {
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

        void onInvoke(ConsoleServiceClient.FetchFigureCallbackFn.P0Type p0, FetchFigureResponse p1);
    }

    @JsFunction
    public interface FetchFigureMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.FetchFigureMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.FetchFigureMetadata_or_callbackFn.P0Type p0,
            FetchFigureResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FetchFigureMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.FetchFigureMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.FetchFigureMetadata_or_callbackFn asFetchFigureMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFetchFigureMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.FetchFigureMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface FetchPandasTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.FetchPandasTableCallbackFn.P0Type create() {
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
            ConsoleServiceClient.FetchPandasTableCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface FetchPandasTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.FetchPandasTableMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.FetchPandasTableMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FetchPandasTableMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.FetchPandasTableMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.FetchPandasTableMetadata_or_callbackFn asFetchPandasTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFetchPandasTableMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.FetchPandasTableMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface FetchTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.FetchTableCallbackFn.P0Type create() {
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
            ConsoleServiceClient.FetchTableCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface FetchTableMapCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.FetchTableMapCallbackFn.P0Type create() {
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

        void onInvoke(ConsoleServiceClient.FetchTableMapCallbackFn.P0Type p0,
            FetchTableMapResponse p1);
    }

    @JsFunction
    public interface FetchTableMapMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.FetchTableMapMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.FetchTableMapMetadata_or_callbackFn.P0Type p0,
            FetchTableMapResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FetchTableMapMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.FetchTableMapMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.FetchTableMapMetadata_or_callbackFn asFetchTableMapMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFetchTableMapMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.FetchTableMapMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface FetchTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.FetchTableMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.FetchTableMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FetchTableMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.FetchTableMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.FetchTableMetadata_or_callbackFn asFetchTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFetchTableMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.FetchTableMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface GetCompletionItemsCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.GetCompletionItemsCallbackFn.P0Type create() {
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
            ConsoleServiceClient.GetCompletionItemsCallbackFn.P0Type p0,
            GetCompletionItemsResponse p1);
    }

    @JsFunction
    public interface GetCompletionItemsMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackFn.P0Type p0,
            GetCompletionItemsResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetCompletionItemsMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackFn asGetCompletionItemsMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isGetCompletionItemsMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface GetConsoleTypesCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.GetConsoleTypesCallbackFn.P0Type create() {
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
            ConsoleServiceClient.GetConsoleTypesCallbackFn.P0Type p0, GetConsoleTypesResponse p1);
    }

    @JsFunction
    public interface GetConsoleTypesMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackFn.P0Type p0,
            GetConsoleTypesResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetConsoleTypesMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackFn asGetConsoleTypesMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isGetConsoleTypesMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface OpenDocumentCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.OpenDocumentCallbackFn.P0Type create() {
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

        void onInvoke(ConsoleServiceClient.OpenDocumentCallbackFn.P0Type p0,
            OpenDocumentResponse p1);
    }

    @JsFunction
    public interface OpenDocumentMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.OpenDocumentMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.OpenDocumentMetadata_or_callbackFn.P0Type p0,
            OpenDocumentResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface OpenDocumentMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.OpenDocumentMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.OpenDocumentMetadata_or_callbackFn asOpenDocumentMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isOpenDocumentMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.OpenDocumentMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface StartConsoleCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.StartConsoleCallbackFn.P0Type create() {
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

        void onInvoke(ConsoleServiceClient.StartConsoleCallbackFn.P0Type p0,
            StartConsoleResponse p1);
    }

    @JsFunction
    public interface StartConsoleMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.StartConsoleMetadata_or_callbackFn.P0Type create() {
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
            ConsoleServiceClient.StartConsoleMetadata_or_callbackFn.P0Type p0,
            StartConsoleResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface StartConsoleMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.StartConsoleMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.StartConsoleMetadata_or_callbackFn asStartConsoleMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isStartConsoleMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.StartConsoleMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public ConsoleServiceClient(String serviceHost, Object options) {}

    public ConsoleServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse bindTableToVariable(
        BindTableToVariableRequest requestMessage,
        ConsoleServiceClient.BindTableToVariableMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.BindTableToVariableCallbackFn callback) {
        return bindTableToVariable(
            requestMessage,
            Js.<ConsoleServiceClient.BindTableToVariableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse bindTableToVariable(
        BindTableToVariableRequest requestMessage,
        ConsoleServiceClient.BindTableToVariableMetadata_or_callbackFn metadata_or_callback) {
        return bindTableToVariable(
            requestMessage,
            Js.<ConsoleServiceClient.BindTableToVariableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse bindTableToVariable(
        BindTableToVariableRequest requestMessage,
        ConsoleServiceClient.BindTableToVariableMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.BindTableToVariableCallbackFn callback);

    public native UnaryResponse bindTableToVariable(
        BindTableToVariableRequest requestMessage,
        ConsoleServiceClient.BindTableToVariableMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse bindTableToVariable(
        BindTableToVariableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.BindTableToVariableCallbackFn callback) {
        return bindTableToVariable(
            requestMessage,
            Js.<ConsoleServiceClient.BindTableToVariableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse bindTableToVariable(
        BindTableToVariableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return bindTableToVariable(
            requestMessage,
            Js.<ConsoleServiceClient.BindTableToVariableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse cancelCommand(
        CancelCommandRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.CancelCommandCallbackFn callback) {
        return cancelCommand(
            requestMessage,
            Js.<ConsoleServiceClient.CancelCommandMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse cancelCommand(
        CancelCommandRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return cancelCommand(
            requestMessage,
            Js.<ConsoleServiceClient.CancelCommandMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse cancelCommand(
        CancelCommandRequest requestMessage,
        ConsoleServiceClient.CancelCommandMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.CancelCommandCallbackFn callback) {
        return cancelCommand(
            requestMessage,
            Js.<ConsoleServiceClient.CancelCommandMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse cancelCommand(
        CancelCommandRequest requestMessage,
        ConsoleServiceClient.CancelCommandMetadata_or_callbackFn metadata_or_callback) {
        return cancelCommand(
            requestMessage,
            Js.<ConsoleServiceClient.CancelCommandMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse cancelCommand(
        CancelCommandRequest requestMessage,
        ConsoleServiceClient.CancelCommandMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.CancelCommandCallbackFn callback);

    public native UnaryResponse cancelCommand(
        CancelCommandRequest requestMessage,
        ConsoleServiceClient.CancelCommandMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse changeDocument(
        ChangeDocumentRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.ChangeDocumentCallbackFn callback) {
        return changeDocument(
            requestMessage,
            Js.<ConsoleServiceClient.ChangeDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse changeDocument(
        ChangeDocumentRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return changeDocument(
            requestMessage,
            Js.<ConsoleServiceClient.ChangeDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse changeDocument(
        ChangeDocumentRequest requestMessage,
        ConsoleServiceClient.ChangeDocumentMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.ChangeDocumentCallbackFn callback) {
        return changeDocument(
            requestMessage,
            Js.<ConsoleServiceClient.ChangeDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse changeDocument(
        ChangeDocumentRequest requestMessage,
        ConsoleServiceClient.ChangeDocumentMetadata_or_callbackFn metadata_or_callback) {
        return changeDocument(
            requestMessage,
            Js.<ConsoleServiceClient.ChangeDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse changeDocument(
        ChangeDocumentRequest requestMessage,
        ConsoleServiceClient.ChangeDocumentMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.ChangeDocumentCallbackFn callback);

    public native UnaryResponse changeDocument(
        ChangeDocumentRequest requestMessage,
        ConsoleServiceClient.ChangeDocumentMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse closeDocument(
        CloseDocumentRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.CloseDocumentCallbackFn callback) {
        return closeDocument(
            requestMessage,
            Js.<ConsoleServiceClient.CloseDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse closeDocument(
        CloseDocumentRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return closeDocument(
            requestMessage,
            Js.<ConsoleServiceClient.CloseDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse closeDocument(
        CloseDocumentRequest requestMessage,
        ConsoleServiceClient.CloseDocumentMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.CloseDocumentCallbackFn callback) {
        return closeDocument(
            requestMessage,
            Js.<ConsoleServiceClient.CloseDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse closeDocument(
        CloseDocumentRequest requestMessage,
        ConsoleServiceClient.CloseDocumentMetadata_or_callbackFn metadata_or_callback) {
        return closeDocument(
            requestMessage,
            Js.<ConsoleServiceClient.CloseDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse closeDocument(
        CloseDocumentRequest requestMessage,
        ConsoleServiceClient.CloseDocumentMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.CloseDocumentCallbackFn callback);

    public native UnaryResponse closeDocument(
        CloseDocumentRequest requestMessage,
        ConsoleServiceClient.CloseDocumentMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse executeCommand(
        ExecuteCommandRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.ExecuteCommandCallbackFn callback) {
        return executeCommand(
            requestMessage,
            Js.<ConsoleServiceClient.ExecuteCommandMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse executeCommand(
        ExecuteCommandRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return executeCommand(
            requestMessage,
            Js.<ConsoleServiceClient.ExecuteCommandMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse executeCommand(
        ExecuteCommandRequest requestMessage,
        ConsoleServiceClient.ExecuteCommandMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.ExecuteCommandCallbackFn callback) {
        return executeCommand(
            requestMessage,
            Js.<ConsoleServiceClient.ExecuteCommandMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse executeCommand(
        ExecuteCommandRequest requestMessage,
        ConsoleServiceClient.ExecuteCommandMetadata_or_callbackFn metadata_or_callback) {
        return executeCommand(
            requestMessage,
            Js.<ConsoleServiceClient.ExecuteCommandMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse executeCommand(
        ExecuteCommandRequest requestMessage,
        ConsoleServiceClient.ExecuteCommandMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.ExecuteCommandCallbackFn callback);

    public native UnaryResponse executeCommand(
        ExecuteCommandRequest requestMessage,
        ConsoleServiceClient.ExecuteCommandMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse fetchFigure(
        FetchFigureRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.FetchFigureCallbackFn callback) {
        return fetchFigure(
            requestMessage,
            Js.<ConsoleServiceClient.FetchFigureMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse fetchFigure(
        FetchFigureRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return fetchFigure(
            requestMessage,
            Js.<ConsoleServiceClient.FetchFigureMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse fetchFigure(
        FetchFigureRequest requestMessage,
        ConsoleServiceClient.FetchFigureMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.FetchFigureCallbackFn callback) {
        return fetchFigure(
            requestMessage,
            Js.<ConsoleServiceClient.FetchFigureMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse fetchFigure(
        FetchFigureRequest requestMessage,
        ConsoleServiceClient.FetchFigureMetadata_or_callbackFn metadata_or_callback) {
        return fetchFigure(
            requestMessage,
            Js.<ConsoleServiceClient.FetchFigureMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse fetchFigure(
        FetchFigureRequest requestMessage,
        ConsoleServiceClient.FetchFigureMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.FetchFigureCallbackFn callback);

    public native UnaryResponse fetchFigure(
        FetchFigureRequest requestMessage,
        ConsoleServiceClient.FetchFigureMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse fetchPandasTable(
        FetchPandasTableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.FetchPandasTableCallbackFn callback) {
        return fetchPandasTable(
            requestMessage,
            Js.<ConsoleServiceClient.FetchPandasTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse fetchPandasTable(
        FetchPandasTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return fetchPandasTable(
            requestMessage,
            Js.<ConsoleServiceClient.FetchPandasTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse fetchPandasTable(
        FetchPandasTableRequest requestMessage,
        ConsoleServiceClient.FetchPandasTableMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.FetchPandasTableCallbackFn callback) {
        return fetchPandasTable(
            requestMessage,
            Js.<ConsoleServiceClient.FetchPandasTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse fetchPandasTable(
        FetchPandasTableRequest requestMessage,
        ConsoleServiceClient.FetchPandasTableMetadata_or_callbackFn metadata_or_callback) {
        return fetchPandasTable(
            requestMessage,
            Js.<ConsoleServiceClient.FetchPandasTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse fetchPandasTable(
        FetchPandasTableRequest requestMessage,
        ConsoleServiceClient.FetchPandasTableMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.FetchPandasTableCallbackFn callback);

    public native UnaryResponse fetchPandasTable(
        FetchPandasTableRequest requestMessage,
        ConsoleServiceClient.FetchPandasTableMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse fetchTable(
        FetchTableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.FetchTableCallbackFn callback) {
        return fetchTable(
            requestMessage,
            Js.<ConsoleServiceClient.FetchTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse fetchTable(
        FetchTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return fetchTable(
            requestMessage,
            Js.<ConsoleServiceClient.FetchTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse fetchTable(
        FetchTableRequest requestMessage,
        ConsoleServiceClient.FetchTableMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.FetchTableCallbackFn callback) {
        return fetchTable(
            requestMessage,
            Js.<ConsoleServiceClient.FetchTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse fetchTable(
        FetchTableRequest requestMessage,
        ConsoleServiceClient.FetchTableMetadata_or_callbackFn metadata_or_callback) {
        return fetchTable(
            requestMessage,
            Js.<ConsoleServiceClient.FetchTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse fetchTable(
        FetchTableRequest requestMessage,
        ConsoleServiceClient.FetchTableMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.FetchTableCallbackFn callback);

    public native UnaryResponse fetchTable(
        FetchTableRequest requestMessage,
        ConsoleServiceClient.FetchTableMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse fetchTableMap(
        FetchTableMapRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.FetchTableMapCallbackFn callback) {
        return fetchTableMap(
            requestMessage,
            Js.<ConsoleServiceClient.FetchTableMapMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse fetchTableMap(
        FetchTableMapRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return fetchTableMap(
            requestMessage,
            Js.<ConsoleServiceClient.FetchTableMapMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse fetchTableMap(
        FetchTableMapRequest requestMessage,
        ConsoleServiceClient.FetchTableMapMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.FetchTableMapCallbackFn callback) {
        return fetchTableMap(
            requestMessage,
            Js.<ConsoleServiceClient.FetchTableMapMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse fetchTableMap(
        FetchTableMapRequest requestMessage,
        ConsoleServiceClient.FetchTableMapMetadata_or_callbackFn metadata_or_callback) {
        return fetchTableMap(
            requestMessage,
            Js.<ConsoleServiceClient.FetchTableMapMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse fetchTableMap(
        FetchTableMapRequest requestMessage,
        ConsoleServiceClient.FetchTableMapMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.FetchTableMapCallbackFn callback);

    public native UnaryResponse fetchTableMap(
        FetchTableMapRequest requestMessage,
        ConsoleServiceClient.FetchTableMapMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse getCompletionItems(
        GetCompletionItemsRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.GetCompletionItemsCallbackFn callback) {
        return getCompletionItems(
            requestMessage,
            Js.<ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse getCompletionItems(
        GetCompletionItemsRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return getCompletionItems(
            requestMessage,
            Js.<ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse getCompletionItems(
        GetCompletionItemsRequest requestMessage,
        ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.GetCompletionItemsCallbackFn callback) {
        return getCompletionItems(
            requestMessage,
            Js.<ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse getCompletionItems(
        GetCompletionItemsRequest requestMessage,
        ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackFn metadata_or_callback) {
        return getCompletionItems(
            requestMessage,
            Js.<ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse getCompletionItems(
        GetCompletionItemsRequest requestMessage,
        ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.GetCompletionItemsCallbackFn callback);

    public native UnaryResponse getCompletionItems(
        GetCompletionItemsRequest requestMessage,
        ConsoleServiceClient.GetCompletionItemsMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse getConsoleTypes(
        GetConsoleTypesRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.GetConsoleTypesCallbackFn callback) {
        return getConsoleTypes(
            requestMessage,
            Js.<ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse getConsoleTypes(
        GetConsoleTypesRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return getConsoleTypes(
            requestMessage,
            Js.<ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse getConsoleTypes(
        GetConsoleTypesRequest requestMessage,
        ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.GetConsoleTypesCallbackFn callback) {
        return getConsoleTypes(
            requestMessage,
            Js.<ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse getConsoleTypes(
        GetConsoleTypesRequest requestMessage,
        ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackFn metadata_or_callback) {
        return getConsoleTypes(
            requestMessage,
            Js.<ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse getConsoleTypes(
        GetConsoleTypesRequest requestMessage,
        ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.GetConsoleTypesCallbackFn callback);

    public native UnaryResponse getConsoleTypes(
        GetConsoleTypesRequest requestMessage,
        ConsoleServiceClient.GetConsoleTypesMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse openDocument(
        OpenDocumentRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.OpenDocumentCallbackFn callback) {
        return openDocument(
            requestMessage,
            Js.<ConsoleServiceClient.OpenDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse openDocument(
        OpenDocumentRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return openDocument(
            requestMessage,
            Js.<ConsoleServiceClient.OpenDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse openDocument(
        OpenDocumentRequest requestMessage,
        ConsoleServiceClient.OpenDocumentMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.OpenDocumentCallbackFn callback) {
        return openDocument(
            requestMessage,
            Js.<ConsoleServiceClient.OpenDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse openDocument(
        OpenDocumentRequest requestMessage,
        ConsoleServiceClient.OpenDocumentMetadata_or_callbackFn metadata_or_callback) {
        return openDocument(
            requestMessage,
            Js.<ConsoleServiceClient.OpenDocumentMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse openDocument(
        OpenDocumentRequest requestMessage,
        ConsoleServiceClient.OpenDocumentMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.OpenDocumentCallbackFn callback);

    public native UnaryResponse openDocument(
        OpenDocumentRequest requestMessage,
        ConsoleServiceClient.OpenDocumentMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse startConsole(
        StartConsoleRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        ConsoleServiceClient.StartConsoleCallbackFn callback) {
        return startConsole(
            requestMessage,
            Js.<ConsoleServiceClient.StartConsoleMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse startConsole(
        StartConsoleRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return startConsole(
            requestMessage,
            Js.<ConsoleServiceClient.StartConsoleMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse startConsole(
        StartConsoleRequest requestMessage,
        ConsoleServiceClient.StartConsoleMetadata_or_callbackFn metadata_or_callback,
        ConsoleServiceClient.StartConsoleCallbackFn callback) {
        return startConsole(
            requestMessage,
            Js.<ConsoleServiceClient.StartConsoleMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse startConsole(
        StartConsoleRequest requestMessage,
        ConsoleServiceClient.StartConsoleMetadata_or_callbackFn metadata_or_callback) {
        return startConsole(
            requestMessage,
            Js.<ConsoleServiceClient.StartConsoleMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse startConsole(
        StartConsoleRequest requestMessage,
        ConsoleServiceClient.StartConsoleMetadata_or_callbackUnionType metadata_or_callback,
        ConsoleServiceClient.StartConsoleCallbackFn callback);

    public native UnaryResponse startConsole(
        StartConsoleRequest requestMessage,
        ConsoleServiceClient.StartConsoleMetadata_or_callbackUnionType metadata_or_callback);

    public native ResponseStream<LogSubscriptionData> subscribeToLogs(
        LogSubscriptionRequest requestMessage, BrowserHeaders metadata);

    public native ResponseStream<LogSubscriptionData> subscribeToLogs(
        LogSubscriptionRequest requestMessage);
}
