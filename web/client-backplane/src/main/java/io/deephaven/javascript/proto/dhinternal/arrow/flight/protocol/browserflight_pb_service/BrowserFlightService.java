package io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.browserflight_pb_service;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.arrow.flight.protocol.BrowserFlight_pb_service.BrowserFlightService",
    namespace = JsPackage.GLOBAL)
public class BrowserFlightService {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface NextDoExchangeType {
    @JsOverlay
    static BrowserFlightService.NextDoExchangeType create() {
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
  public interface NextDoPutType {
    @JsOverlay
    static BrowserFlightService.NextDoPutType create() {
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
  public interface NextHandshakeType {
    @JsOverlay
    static BrowserFlightService.NextHandshakeType create() {
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
  public interface OpenDoExchangeType {
    @JsOverlay
    static BrowserFlightService.OpenDoExchangeType create() {
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
  public interface OpenDoPutType {
    @JsOverlay
    static BrowserFlightService.OpenDoPutType create() {
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
  public interface OpenHandshakeType {
    @JsOverlay
    static BrowserFlightService.OpenHandshakeType create() {
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

  public static BrowserFlightService.NextDoExchangeType NextDoExchange;
  public static BrowserFlightService.NextDoPutType NextDoPut;
  public static BrowserFlightService.NextHandshakeType NextHandshake;
  public static BrowserFlightService.OpenDoExchangeType OpenDoExchange;
  public static BrowserFlightService.OpenDoPutType OpenDoPut;
  public static BrowserFlightService.OpenHandshakeType OpenHandshake;
  public static String serviceName;
}
