package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb.BarrageData;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb.OutOfBandSubscriptionResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb.SubscriptionRequest;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.barrage_pb_service.BarrageServiceClient",
    namespace = JsPackage.GLOBAL)
public class BarrageServiceClient {
  @JsFunction
  public interface DoUpdateSubscriptionCallbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static BarrageServiceClient.DoUpdateSubscriptionCallbackFn.P0Type create() {
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
        BarrageServiceClient.DoUpdateSubscriptionCallbackFn.P0Type p0,
        OutOfBandSubscriptionResponse p1);
  }

  @JsFunction
  public interface DoUpdateSubscriptionMetadata_or_callbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackFn.P0Type create() {
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
        BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackFn.P0Type p0,
        OutOfBandSubscriptionResponse p1);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface DoUpdateSubscriptionMetadata_or_callbackUnionType {
    @JsOverlay
    static BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BrowserHeaders asBrowserHeaders() {
      return Js.cast(this);
    }

    @JsOverlay
    default BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackFn
        asDoUpdateSubscriptionMetadata_or_callbackFn() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBrowserHeaders() {
      return (Object) this instanceof BrowserHeaders;
    }

    @JsOverlay
    default boolean isDoUpdateSubscriptionMetadata_or_callbackFn() {
      return (Object) this
          instanceof BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackFn;
    }
  }

  public String serviceHost;

  public BarrageServiceClient(String serviceHost, Object options) {}

  public BarrageServiceClient(String serviceHost) {}

  public native BidirectionalStream<SubscriptionRequest, BarrageData> doSubscribe();

  public native BidirectionalStream<SubscriptionRequest, BarrageData> doSubscribe(
      BrowserHeaders metadata);

  public native ResponseStream<BarrageData> doSubscribeNoClientStream(
      SubscriptionRequest requestMessage, BrowserHeaders metadata);

  public native ResponseStream<BarrageData> doSubscribeNoClientStream(
      SubscriptionRequest requestMessage);

  @JsOverlay
  public final UnaryResponse doUpdateSubscription(
      SubscriptionRequest requestMessage,
      BrowserHeaders metadata_or_callback,
      BarrageServiceClient.DoUpdateSubscriptionCallbackFn callback) {
    return doUpdateSubscription(
        requestMessage,
        Js.<BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse doUpdateSubscription(
      SubscriptionRequest requestMessage, BrowserHeaders metadata_or_callback) {
    return doUpdateSubscription(
        requestMessage,
        Js.<BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  @JsOverlay
  public final UnaryResponse doUpdateSubscription(
      SubscriptionRequest requestMessage,
      BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackFn metadata_or_callback,
      BarrageServiceClient.DoUpdateSubscriptionCallbackFn callback) {
    return doUpdateSubscription(
        requestMessage,
        Js.<BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse doUpdateSubscription(
      SubscriptionRequest requestMessage,
      BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackFn metadata_or_callback) {
    return doUpdateSubscription(
        requestMessage,
        Js.<BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  public native UnaryResponse doUpdateSubscription(
      SubscriptionRequest requestMessage,
      BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackUnionType metadata_or_callback,
      BarrageServiceClient.DoUpdateSubscriptionCallbackFn callback);

  public native UnaryResponse doUpdateSubscription(
      SubscriptionRequest requestMessage,
      BarrageServiceClient.DoUpdateSubscriptionMetadata_or_callbackUnionType metadata_or_callback);
}
