package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb_service;

import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb.BrowserNextResponse;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.HandshakeRequest;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.HandshakeResponse;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.PutResult;
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
    name = "dhinternal.arrow.flight.protocol.BrowserFlight_pb_service.BrowserFlightServiceClient",
    namespace = JsPackage.GLOBAL)
public class BrowserFlightServiceClient {
  @JsFunction
  public interface NextDoExchangeCallbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static BrowserFlightServiceClient.NextDoExchangeCallbackFn.P0Type create() {
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
        BrowserFlightServiceClient.NextDoExchangeCallbackFn.P0Type p0, BrowserNextResponse p1);
  }

  @JsFunction
  public interface NextDoExchangeMetadata_or_callbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackFn.P0Type create() {
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
        BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackFn.P0Type p0,
        BrowserNextResponse p1);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface NextDoExchangeMetadata_or_callbackUnionType {
    @JsOverlay
    static BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BrowserHeaders asBrowserHeaders() {
      return Js.cast(this);
    }

    @JsOverlay
    default BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackFn
        asNextDoExchangeMetadata_or_callbackFn() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBrowserHeaders() {
      return (Object) this instanceof BrowserHeaders;
    }

    @JsOverlay
    default boolean isNextDoExchangeMetadata_or_callbackFn() {
      return (Object) this
          instanceof BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackFn;
    }
  }

  @JsFunction
  public interface NextDoPutCallbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static BrowserFlightServiceClient.NextDoPutCallbackFn.P0Type create() {
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

    void onInvoke(BrowserFlightServiceClient.NextDoPutCallbackFn.P0Type p0, BrowserNextResponse p1);
  }

  @JsFunction
  public interface NextDoPutMetadata_or_callbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static BrowserFlightServiceClient.NextDoPutMetadata_or_callbackFn.P0Type create() {
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
        BrowserFlightServiceClient.NextDoPutMetadata_or_callbackFn.P0Type p0,
        BrowserNextResponse p1);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface NextDoPutMetadata_or_callbackUnionType {
    @JsOverlay
    static BrowserFlightServiceClient.NextDoPutMetadata_or_callbackUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BrowserHeaders asBrowserHeaders() {
      return Js.cast(this);
    }

    @JsOverlay
    default BrowserFlightServiceClient.NextDoPutMetadata_or_callbackFn
        asNextDoPutMetadata_or_callbackFn() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBrowserHeaders() {
      return (Object) this instanceof BrowserHeaders;
    }

    @JsOverlay
    default boolean isNextDoPutMetadata_or_callbackFn() {
      return (Object) this instanceof BrowserFlightServiceClient.NextDoPutMetadata_or_callbackFn;
    }
  }

  @JsFunction
  public interface NextHandshakeCallbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static BrowserFlightServiceClient.NextHandshakeCallbackFn.P0Type create() {
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
        BrowserFlightServiceClient.NextHandshakeCallbackFn.P0Type p0, BrowserNextResponse p1);
  }

  @JsFunction
  public interface NextHandshakeMetadata_or_callbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackFn.P0Type create() {
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
        BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackFn.P0Type p0,
        BrowserNextResponse p1);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface NextHandshakeMetadata_or_callbackUnionType {
    @JsOverlay
    static BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BrowserHeaders asBrowserHeaders() {
      return Js.cast(this);
    }

    @JsOverlay
    default BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackFn
        asNextHandshakeMetadata_or_callbackFn() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBrowserHeaders() {
      return (Object) this instanceof BrowserHeaders;
    }

    @JsOverlay
    default boolean isNextHandshakeMetadata_or_callbackFn() {
      return (Object) this
          instanceof BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackFn;
    }
  }

  public String serviceHost;

  public BrowserFlightServiceClient(String serviceHost, Object options) {}

  public BrowserFlightServiceClient(String serviceHost) {}

  @JsOverlay
  public final UnaryResponse nextDoExchange(
      FlightData requestMessage,
      BrowserHeaders metadata_or_callback,
      BrowserFlightServiceClient.NextDoExchangeCallbackFn callback) {
    return nextDoExchange(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse nextDoExchange(
      FlightData requestMessage, BrowserHeaders metadata_or_callback) {
    return nextDoExchange(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  @JsOverlay
  public final UnaryResponse nextDoExchange(
      FlightData requestMessage,
      BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackFn metadata_or_callback,
      BrowserFlightServiceClient.NextDoExchangeCallbackFn callback) {
    return nextDoExchange(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse nextDoExchange(
      FlightData requestMessage,
      BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackFn metadata_or_callback) {
    return nextDoExchange(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  public native UnaryResponse nextDoExchange(
      FlightData requestMessage,
      BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackUnionType metadata_or_callback,
      BrowserFlightServiceClient.NextDoExchangeCallbackFn callback);

  public native UnaryResponse nextDoExchange(
      FlightData requestMessage,
      BrowserFlightServiceClient.NextDoExchangeMetadata_or_callbackUnionType metadata_or_callback);

  @JsOverlay
  public final UnaryResponse nextDoPut(
      FlightData requestMessage,
      BrowserHeaders metadata_or_callback,
      BrowserFlightServiceClient.NextDoPutCallbackFn callback) {
    return nextDoPut(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextDoPutMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse nextDoPut(
      FlightData requestMessage, BrowserHeaders metadata_or_callback) {
    return nextDoPut(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextDoPutMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  @JsOverlay
  public final UnaryResponse nextDoPut(
      FlightData requestMessage,
      BrowserFlightServiceClient.NextDoPutMetadata_or_callbackFn metadata_or_callback,
      BrowserFlightServiceClient.NextDoPutCallbackFn callback) {
    return nextDoPut(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextDoPutMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse nextDoPut(
      FlightData requestMessage,
      BrowserFlightServiceClient.NextDoPutMetadata_or_callbackFn metadata_or_callback) {
    return nextDoPut(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextDoPutMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  public native UnaryResponse nextDoPut(
      FlightData requestMessage,
      BrowserFlightServiceClient.NextDoPutMetadata_or_callbackUnionType metadata_or_callback,
      BrowserFlightServiceClient.NextDoPutCallbackFn callback);

  public native UnaryResponse nextDoPut(
      FlightData requestMessage,
      BrowserFlightServiceClient.NextDoPutMetadata_or_callbackUnionType metadata_or_callback);

  @JsOverlay
  public final UnaryResponse nextHandshake(
      HandshakeRequest requestMessage,
      BrowserHeaders metadata_or_callback,
      BrowserFlightServiceClient.NextHandshakeCallbackFn callback) {
    return nextHandshake(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse nextHandshake(
      HandshakeRequest requestMessage, BrowserHeaders metadata_or_callback) {
    return nextHandshake(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  @JsOverlay
  public final UnaryResponse nextHandshake(
      HandshakeRequest requestMessage,
      BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackFn metadata_or_callback,
      BrowserFlightServiceClient.NextHandshakeCallbackFn callback) {
    return nextHandshake(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse nextHandshake(
      HandshakeRequest requestMessage,
      BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackFn metadata_or_callback) {
    return nextHandshake(
        requestMessage,
        Js.<BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  public native UnaryResponse nextHandshake(
      HandshakeRequest requestMessage,
      BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackUnionType metadata_or_callback,
      BrowserFlightServiceClient.NextHandshakeCallbackFn callback);

  public native UnaryResponse nextHandshake(
      HandshakeRequest requestMessage,
      BrowserFlightServiceClient.NextHandshakeMetadata_or_callbackUnionType metadata_or_callback);

  public native ResponseStream<FlightData> openDoExchange(
      FlightData requestMessage, BrowserHeaders metadata);

  public native ResponseStream<FlightData> openDoExchange(FlightData requestMessage);

  public native ResponseStream<PutResult> openDoPut(
      FlightData requestMessage, BrowserHeaders metadata);

  public native ResponseStream<PutResult> openDoPut(FlightData requestMessage);

  public native ResponseStream<HandshakeResponse> openHandshake(
      HandshakeRequest requestMessage, BrowserHeaders metadata);

  public native ResponseStream<HandshakeResponse> openHandshake(HandshakeRequest requestMessage);
}
