package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.barrage_pb_service;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.barrage_pb_service.BarrageService",
    namespace = JsPackage.GLOBAL)
public class BarrageService {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface DoSubscribeNoClientStreamType {
    @JsOverlay
    static BarrageService.DoSubscribeNoClientStreamType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getMethodName();

    @JsProperty
    Object getRequestType();

    @JsProperty
    Object getResponseType();

    @JsProperty
    Object getService();

    @JsProperty
    boolean isRequestStream();

    @JsProperty
    boolean isResponseStream();

    @JsProperty
    void setMethodName(String methodName);

    @JsProperty
    void setRequestStream(boolean requestStream);

    @JsProperty
    void setRequestType(Object requestType);

    @JsProperty
    void setResponseStream(boolean responseStream);

    @JsProperty
    void setResponseType(Object responseType);

    @JsProperty
    void setService(Object service);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface DoSubscribeType {
    @JsOverlay
    static BarrageService.DoSubscribeType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getMethodName();

    @JsProperty
    Object getRequestType();

    @JsProperty
    Object getResponseType();

    @JsProperty
    Object getService();

    @JsProperty
    boolean isRequestStream();

    @JsProperty
    boolean isResponseStream();

    @JsProperty
    void setMethodName(String methodName);

    @JsProperty
    void setRequestStream(boolean requestStream);

    @JsProperty
    void setRequestType(Object requestType);

    @JsProperty
    void setResponseStream(boolean responseStream);

    @JsProperty
    void setResponseType(Object responseType);

    @JsProperty
    void setService(Object service);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface DoUpdateSubscriptionType {
    @JsOverlay
    static BarrageService.DoUpdateSubscriptionType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getMethodName();

    @JsProperty
    Object getRequestType();

    @JsProperty
    Object getResponseType();

    @JsProperty
    Object getService();

    @JsProperty
    boolean isRequestStream();

    @JsProperty
    boolean isResponseStream();

    @JsProperty
    void setMethodName(String methodName);

    @JsProperty
    void setRequestStream(boolean requestStream);

    @JsProperty
    void setRequestType(Object requestType);

    @JsProperty
    void setResponseStream(boolean responseStream);

    @JsProperty
    void setResponseType(Object responseType);

    @JsProperty
    void setService(Object service);
  }

  public static BarrageService.DoSubscribeType DoSubscribe;
  public static BarrageService.DoSubscribeNoClientStreamType DoSubscribeNoClientStream;
  public static BarrageService.DoUpdateSubscriptionType DoUpdateSubscription;
  public static String serviceName;
}
