package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb_service;

import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.ExportNotification;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.ExportNotificationRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.HandshakeRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.HandshakeResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.ReleaseResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.session_pb_service.SessionServiceClient",
    namespace = JsPackage.GLOBAL)
public class SessionServiceClient {
  @JsFunction
  public interface CloseSessionCallbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static SessionServiceClient.CloseSessionCallbackFn.P0Type create() {
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

    void onInvoke(SessionServiceClient.CloseSessionCallbackFn.P0Type p0, ReleaseResponse p1);
  }

  @JsFunction
  public interface CloseSessionMetadata_or_callbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static SessionServiceClient.CloseSessionMetadata_or_callbackFn.P0Type create() {
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
        SessionServiceClient.CloseSessionMetadata_or_callbackFn.P0Type p0, ReleaseResponse p1);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface CloseSessionMetadata_or_callbackUnionType {
    @JsOverlay
    static SessionServiceClient.CloseSessionMetadata_or_callbackUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BrowserHeaders asBrowserHeaders() {
      return Js.cast(this);
    }

    @JsOverlay
    default SessionServiceClient.CloseSessionMetadata_or_callbackFn
        asCloseSessionMetadata_or_callbackFn() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBrowserHeaders() {
      return (Object) this instanceof BrowserHeaders;
    }

    @JsOverlay
    default boolean isCloseSessionMetadata_or_callbackFn() {
      return (Object) this instanceof SessionServiceClient.CloseSessionMetadata_or_callbackFn;
    }
  }

  @JsFunction
  public interface NewSessionCallbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static SessionServiceClient.NewSessionCallbackFn.P0Type create() {
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

    void onInvoke(SessionServiceClient.NewSessionCallbackFn.P0Type p0, HandshakeResponse p1);
  }

  @JsFunction
  public interface NewSessionMetadata_or_callbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static SessionServiceClient.NewSessionMetadata_or_callbackFn.P0Type create() {
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
        SessionServiceClient.NewSessionMetadata_or_callbackFn.P0Type p0, HandshakeResponse p1);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface NewSessionMetadata_or_callbackUnionType {
    @JsOverlay
    static SessionServiceClient.NewSessionMetadata_or_callbackUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BrowserHeaders asBrowserHeaders() {
      return Js.cast(this);
    }

    @JsOverlay
    default SessionServiceClient.NewSessionMetadata_or_callbackFn
        asNewSessionMetadata_or_callbackFn() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBrowserHeaders() {
      return (Object) this instanceof BrowserHeaders;
    }

    @JsOverlay
    default boolean isNewSessionMetadata_or_callbackFn() {
      return (Object) this instanceof SessionServiceClient.NewSessionMetadata_or_callbackFn;
    }
  }

  @JsFunction
  public interface RefreshSessionTokenCallbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static SessionServiceClient.RefreshSessionTokenCallbackFn.P0Type create() {
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
        SessionServiceClient.RefreshSessionTokenCallbackFn.P0Type p0, HandshakeResponse p1);
  }

  @JsFunction
  public interface RefreshSessionTokenMetadata_or_callbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static SessionServiceClient.RefreshSessionTokenMetadata_or_callbackFn.P0Type create() {
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
        SessionServiceClient.RefreshSessionTokenMetadata_or_callbackFn.P0Type p0,
        HandshakeResponse p1);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface RefreshSessionTokenMetadata_or_callbackUnionType {
    @JsOverlay
    static SessionServiceClient.RefreshSessionTokenMetadata_or_callbackUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BrowserHeaders asBrowserHeaders() {
      return Js.cast(this);
    }

    @JsOverlay
    default SessionServiceClient.RefreshSessionTokenMetadata_or_callbackFn
        asRefreshSessionTokenMetadata_or_callbackFn() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBrowserHeaders() {
      return (Object) this instanceof BrowserHeaders;
    }

    @JsOverlay
    default boolean isRefreshSessionTokenMetadata_or_callbackFn() {
      return (Object) this
          instanceof SessionServiceClient.RefreshSessionTokenMetadata_or_callbackFn;
    }
  }

  @JsFunction
  public interface ReleaseCallbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static SessionServiceClient.ReleaseCallbackFn.P0Type create() {
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

    void onInvoke(SessionServiceClient.ReleaseCallbackFn.P0Type p0, ReleaseResponse p1);
  }

  @JsFunction
  public interface ReleaseMetadata_or_callbackFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0Type {
      @JsOverlay
      static SessionServiceClient.ReleaseMetadata_or_callbackFn.P0Type create() {
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

    void onInvoke(SessionServiceClient.ReleaseMetadata_or_callbackFn.P0Type p0, ReleaseResponse p1);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ReleaseMetadata_or_callbackUnionType {
    @JsOverlay
    static SessionServiceClient.ReleaseMetadata_or_callbackUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BrowserHeaders asBrowserHeaders() {
      return Js.cast(this);
    }

    @JsOverlay
    default SessionServiceClient.ReleaseMetadata_or_callbackFn asReleaseMetadata_or_callbackFn() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBrowserHeaders() {
      return (Object) this instanceof BrowserHeaders;
    }

    @JsOverlay
    default boolean isReleaseMetadata_or_callbackFn() {
      return (Object) this instanceof SessionServiceClient.ReleaseMetadata_or_callbackFn;
    }
  }

  public String serviceHost;

  public SessionServiceClient(String serviceHost, Object options) {}

  public SessionServiceClient(String serviceHost) {}

  @JsOverlay
  public final UnaryResponse closeSession(
      HandshakeRequest requestMessage,
      BrowserHeaders metadata_or_callback,
      SessionServiceClient.CloseSessionCallbackFn callback) {
    return closeSession(
        requestMessage,
        Js.<SessionServiceClient.CloseSessionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse closeSession(
      HandshakeRequest requestMessage, BrowserHeaders metadata_or_callback) {
    return closeSession(
        requestMessage,
        Js.<SessionServiceClient.CloseSessionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  @JsOverlay
  public final UnaryResponse closeSession(
      HandshakeRequest requestMessage,
      SessionServiceClient.CloseSessionMetadata_or_callbackFn metadata_or_callback,
      SessionServiceClient.CloseSessionCallbackFn callback) {
    return closeSession(
        requestMessage,
        Js.<SessionServiceClient.CloseSessionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse closeSession(
      HandshakeRequest requestMessage,
      SessionServiceClient.CloseSessionMetadata_or_callbackFn metadata_or_callback) {
    return closeSession(
        requestMessage,
        Js.<SessionServiceClient.CloseSessionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  public native UnaryResponse closeSession(
      HandshakeRequest requestMessage,
      SessionServiceClient.CloseSessionMetadata_or_callbackUnionType metadata_or_callback,
      SessionServiceClient.CloseSessionCallbackFn callback);

  public native UnaryResponse closeSession(
      HandshakeRequest requestMessage,
      SessionServiceClient.CloseSessionMetadata_or_callbackUnionType metadata_or_callback);

  public native ResponseStream<ExportNotification> exportNotifications(
      ExportNotificationRequest requestMessage, BrowserHeaders metadata);

  public native ResponseStream<ExportNotification> exportNotifications(
      ExportNotificationRequest requestMessage);

  @JsOverlay
  public final UnaryResponse newSession(
      HandshakeRequest requestMessage,
      BrowserHeaders metadata_or_callback,
      SessionServiceClient.NewSessionCallbackFn callback) {
    return newSession(
        requestMessage,
        Js.<SessionServiceClient.NewSessionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse newSession(
      HandshakeRequest requestMessage, BrowserHeaders metadata_or_callback) {
    return newSession(
        requestMessage,
        Js.<SessionServiceClient.NewSessionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  @JsOverlay
  public final UnaryResponse newSession(
      HandshakeRequest requestMessage,
      SessionServiceClient.NewSessionMetadata_or_callbackFn metadata_or_callback,
      SessionServiceClient.NewSessionCallbackFn callback) {
    return newSession(
        requestMessage,
        Js.<SessionServiceClient.NewSessionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse newSession(
      HandshakeRequest requestMessage,
      SessionServiceClient.NewSessionMetadata_or_callbackFn metadata_or_callback) {
    return newSession(
        requestMessage,
        Js.<SessionServiceClient.NewSessionMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  public native UnaryResponse newSession(
      HandshakeRequest requestMessage,
      SessionServiceClient.NewSessionMetadata_or_callbackUnionType metadata_or_callback,
      SessionServiceClient.NewSessionCallbackFn callback);

  public native UnaryResponse newSession(
      HandshakeRequest requestMessage,
      SessionServiceClient.NewSessionMetadata_or_callbackUnionType metadata_or_callback);

  @JsOverlay
  public final UnaryResponse refreshSessionToken(
      HandshakeRequest requestMessage,
      BrowserHeaders metadata_or_callback,
      SessionServiceClient.RefreshSessionTokenCallbackFn callback) {
    return refreshSessionToken(
        requestMessage,
        Js.<SessionServiceClient.RefreshSessionTokenMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse refreshSessionToken(
      HandshakeRequest requestMessage, BrowserHeaders metadata_or_callback) {
    return refreshSessionToken(
        requestMessage,
        Js.<SessionServiceClient.RefreshSessionTokenMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  @JsOverlay
  public final UnaryResponse refreshSessionToken(
      HandshakeRequest requestMessage,
      SessionServiceClient.RefreshSessionTokenMetadata_or_callbackFn metadata_or_callback,
      SessionServiceClient.RefreshSessionTokenCallbackFn callback) {
    return refreshSessionToken(
        requestMessage,
        Js.<SessionServiceClient.RefreshSessionTokenMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse refreshSessionToken(
      HandshakeRequest requestMessage,
      SessionServiceClient.RefreshSessionTokenMetadata_or_callbackFn metadata_or_callback) {
    return refreshSessionToken(
        requestMessage,
        Js.<SessionServiceClient.RefreshSessionTokenMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  public native UnaryResponse refreshSessionToken(
      HandshakeRequest requestMessage,
      SessionServiceClient.RefreshSessionTokenMetadata_or_callbackUnionType metadata_or_callback,
      SessionServiceClient.RefreshSessionTokenCallbackFn callback);

  public native UnaryResponse refreshSessionToken(
      HandshakeRequest requestMessage,
      SessionServiceClient.RefreshSessionTokenMetadata_or_callbackUnionType metadata_or_callback);

  @JsOverlay
  public final UnaryResponse release(
      Ticket requestMessage,
      BrowserHeaders metadata_or_callback,
      SessionServiceClient.ReleaseCallbackFn callback) {
    return release(
        requestMessage,
        Js.<SessionServiceClient.ReleaseMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse release(Ticket requestMessage, BrowserHeaders metadata_or_callback) {
    return release(
        requestMessage,
        Js.<SessionServiceClient.ReleaseMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  @JsOverlay
  public final UnaryResponse release(
      Ticket requestMessage,
      SessionServiceClient.ReleaseMetadata_or_callbackFn metadata_or_callback,
      SessionServiceClient.ReleaseCallbackFn callback) {
    return release(
        requestMessage,
        Js.<SessionServiceClient.ReleaseMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback),
        callback);
  }

  @JsOverlay
  public final UnaryResponse release(
      Ticket requestMessage,
      SessionServiceClient.ReleaseMetadata_or_callbackFn metadata_or_callback) {
    return release(
        requestMessage,
        Js.<SessionServiceClient.ReleaseMetadata_or_callbackUnionType>uncheckedCast(
            metadata_or_callback));
  }

  public native UnaryResponse release(
      Ticket requestMessage,
      SessionServiceClient.ReleaseMetadata_or_callbackUnionType metadata_or_callback,
      SessionServiceClient.ReleaseCallbackFn callback);

  public native UnaryResponse release(
      Ticket requestMessage,
      SessionServiceClient.ReleaseMetadata_or_callbackUnionType metadata_or_callback);
}
