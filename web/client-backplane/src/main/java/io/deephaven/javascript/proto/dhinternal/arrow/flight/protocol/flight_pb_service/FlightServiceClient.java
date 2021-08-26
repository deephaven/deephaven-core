package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb_service;

import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Action;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.ActionType;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Criteria;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Empty;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightDescriptor;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightInfo;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.HandshakeRequest;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.HandshakeResponse;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.PutResult;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Result;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.SchemaResult;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.Flight_pb_service.FlightServiceClient",
    namespace = JsPackage.GLOBAL)
public class FlightServiceClient {
    @JsFunction
    public interface GetFlightInfoCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static FlightServiceClient.GetFlightInfoCallbackFn.P0Type create() {
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

        void onInvoke(FlightServiceClient.GetFlightInfoCallbackFn.P0Type p0, FlightInfo p1);
    }

    @JsFunction
    public interface GetFlightInfoMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static FlightServiceClient.GetFlightInfoMetadata_or_callbackFn.P0Type create() {
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

        void onInvoke(FlightServiceClient.GetFlightInfoMetadata_or_callbackFn.P0Type p0,
            FlightInfo p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetFlightInfoMetadata_or_callbackUnionType {
        @JsOverlay
        static FlightServiceClient.GetFlightInfoMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default FlightServiceClient.GetFlightInfoMetadata_or_callbackFn asGetFlightInfoMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isGetFlightInfoMetadata_or_callbackFn() {
            return (Object) this instanceof FlightServiceClient.GetFlightInfoMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface GetSchemaCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static FlightServiceClient.GetSchemaCallbackFn.P0Type create() {
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

        void onInvoke(FlightServiceClient.GetSchemaCallbackFn.P0Type p0, SchemaResult p1);
    }

    @JsFunction
    public interface GetSchemaMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static FlightServiceClient.GetSchemaMetadata_or_callbackFn.P0Type create() {
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

        void onInvoke(FlightServiceClient.GetSchemaMetadata_or_callbackFn.P0Type p0,
            SchemaResult p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSchemaMetadata_or_callbackUnionType {
        @JsOverlay
        static FlightServiceClient.GetSchemaMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default FlightServiceClient.GetSchemaMetadata_or_callbackFn asGetSchemaMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isGetSchemaMetadata_or_callbackFn() {
            return (Object) this instanceof FlightServiceClient.GetSchemaMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public FlightServiceClient(String serviceHost, Object options) {}

    public FlightServiceClient(String serviceHost) {}

    public native ResponseStream<Result> doAction(Action requestMessage, BrowserHeaders metadata);

    public native ResponseStream<Result> doAction(Action requestMessage);

    public native BidirectionalStream<FlightData, FlightData> doExchange();

    public native BidirectionalStream<FlightData, FlightData> doExchange(BrowserHeaders metadata);

    public native ResponseStream<FlightData> doGet(Ticket requestMessage, BrowserHeaders metadata);

    public native ResponseStream<FlightData> doGet(Ticket requestMessage);

    public native BidirectionalStream<FlightData, PutResult> doPut();

    public native BidirectionalStream<FlightData, PutResult> doPut(BrowserHeaders metadata);

    @JsOverlay
    public final UnaryResponse getFlightInfo(
        FlightDescriptor requestMessage,
        BrowserHeaders metadata_or_callback,
        FlightServiceClient.GetFlightInfoCallbackFn callback) {
        return getFlightInfo(
            requestMessage,
            Js.<FlightServiceClient.GetFlightInfoMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse getFlightInfo(
        FlightDescriptor requestMessage, BrowserHeaders metadata_or_callback) {
        return getFlightInfo(
            requestMessage,
            Js.<FlightServiceClient.GetFlightInfoMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse getFlightInfo(
        FlightDescriptor requestMessage,
        FlightServiceClient.GetFlightInfoMetadata_or_callbackFn metadata_or_callback,
        FlightServiceClient.GetFlightInfoCallbackFn callback) {
        return getFlightInfo(
            requestMessage,
            Js.<FlightServiceClient.GetFlightInfoMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse getFlightInfo(
        FlightDescriptor requestMessage,
        FlightServiceClient.GetFlightInfoMetadata_or_callbackFn metadata_or_callback) {
        return getFlightInfo(
            requestMessage,
            Js.<FlightServiceClient.GetFlightInfoMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse getFlightInfo(
        FlightDescriptor requestMessage,
        FlightServiceClient.GetFlightInfoMetadata_or_callbackUnionType metadata_or_callback,
        FlightServiceClient.GetFlightInfoCallbackFn callback);

    public native UnaryResponse getFlightInfo(
        FlightDescriptor requestMessage,
        FlightServiceClient.GetFlightInfoMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse getSchema(
        FlightDescriptor requestMessage,
        BrowserHeaders metadata_or_callback,
        FlightServiceClient.GetSchemaCallbackFn callback) {
        return getSchema(
            requestMessage,
            Js.<FlightServiceClient.GetSchemaMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse getSchema(
        FlightDescriptor requestMessage, BrowserHeaders metadata_or_callback) {
        return getSchema(
            requestMessage,
            Js.<FlightServiceClient.GetSchemaMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse getSchema(
        FlightDescriptor requestMessage,
        FlightServiceClient.GetSchemaMetadata_or_callbackFn metadata_or_callback,
        FlightServiceClient.GetSchemaCallbackFn callback) {
        return getSchema(
            requestMessage,
            Js.<FlightServiceClient.GetSchemaMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse getSchema(
        FlightDescriptor requestMessage,
        FlightServiceClient.GetSchemaMetadata_or_callbackFn metadata_or_callback) {
        return getSchema(
            requestMessage,
            Js.<FlightServiceClient.GetSchemaMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse getSchema(
        FlightDescriptor requestMessage,
        FlightServiceClient.GetSchemaMetadata_or_callbackUnionType metadata_or_callback,
        FlightServiceClient.GetSchemaCallbackFn callback);

    public native UnaryResponse getSchema(
        FlightDescriptor requestMessage,
        FlightServiceClient.GetSchemaMetadata_or_callbackUnionType metadata_or_callback);

    public native BidirectionalStream<HandshakeRequest, HandshakeResponse> handshake();

    public native BidirectionalStream<HandshakeRequest, HandshakeResponse> handshake(
        BrowserHeaders metadata);

    public native ResponseStream<ActionType> listActions(
        Empty requestMessage, BrowserHeaders metadata);

    public native ResponseStream<ActionType> listActions(Empty requestMessage);

    public native ResponseStream<FlightInfo> listFlights(
        Criteria requestMessage, BrowserHeaders metadata);

    public native ResponseStream<FlightInfo> listFlights(Criteria requestMessage);
}
