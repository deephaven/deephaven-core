package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.AutoCompleteRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.AutoCompleteResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.BindTableToVariableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.BindTableToVariableResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.BrowserNextResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.CancelCommandRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.CancelCommandResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.ExecuteCommandRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.ExecuteCommandResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchFigureRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.FetchFigureResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetConsoleTypesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.GetConsoleTypesResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.LogSubscriptionData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.LogSubscriptionRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.StartConsoleRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.StartConsoleResponse;
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

        void onInvoke(ConsoleServiceClient.CancelCommandCallbackFn.P0Type p0, CancelCommandResponse p1);
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
                ConsoleServiceClient.FetchFigureMetadata_or_callbackFn.P0Type p0, FetchFigureResponse p1);
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
    public interface NextAutoCompleteStreamCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.NextAutoCompleteStreamCallbackFn.P0Type create() {
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
                ConsoleServiceClient.NextAutoCompleteStreamCallbackFn.P0Type p0, BrowserNextResponse p1);
    }

    @JsFunction
    public interface NextAutoCompleteStreamMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackFn.P0Type create() {
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
                ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackFn.P0Type p0,
                BrowserNextResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface NextAutoCompleteStreamMetadata_or_callbackUnionType {
        @JsOverlay
        static ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackFn asNextAutoCompleteStreamMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isNextAutoCompleteStreamMetadata_or_callbackFn() {
            return (Object) this instanceof ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackFn;
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

        void onInvoke(ConsoleServiceClient.StartConsoleCallbackFn.P0Type p0, StartConsoleResponse p1);
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
                ConsoleServiceClient.StartConsoleMetadata_or_callbackFn.P0Type p0, StartConsoleResponse p1);
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

    public native BidirectionalStream<AutoCompleteRequest, AutoCompleteResponse> autoCompleteStream();

    public native BidirectionalStream<AutoCompleteRequest, AutoCompleteResponse> autoCompleteStream(
            BrowserHeaders metadata);

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
    public final UnaryResponse nextAutoCompleteStream(
            AutoCompleteRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            ConsoleServiceClient.NextAutoCompleteStreamCallbackFn callback) {
        return nextAutoCompleteStream(
                requestMessage,
                Js.<ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse nextAutoCompleteStream(
            AutoCompleteRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return nextAutoCompleteStream(
                requestMessage,
                Js.<ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse nextAutoCompleteStream(
            AutoCompleteRequest requestMessage,
            ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackFn metadata_or_callback,
            ConsoleServiceClient.NextAutoCompleteStreamCallbackFn callback) {
        return nextAutoCompleteStream(
                requestMessage,
                Js.<ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse nextAutoCompleteStream(
            AutoCompleteRequest requestMessage,
            ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackFn metadata_or_callback) {
        return nextAutoCompleteStream(
                requestMessage,
                Js.<ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse nextAutoCompleteStream(
            AutoCompleteRequest requestMessage,
            ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackUnionType metadata_or_callback,
            ConsoleServiceClient.NextAutoCompleteStreamCallbackFn callback);

    public native UnaryResponse nextAutoCompleteStream(
            AutoCompleteRequest requestMessage,
            ConsoleServiceClient.NextAutoCompleteStreamMetadata_or_callbackUnionType metadata_or_callback);

    public native ResponseStream<AutoCompleteResponse> openAutoCompleteStream(
            AutoCompleteRequest requestMessage, BrowserHeaders metadata);

    public native ResponseStream<AutoCompleteResponse> openAutoCompleteStream(
            AutoCompleteRequest requestMessage);

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
