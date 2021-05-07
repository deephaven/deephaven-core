package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.CloseDocumentRequest",
    namespace = JsPackage.GLOBAL)
public class CloseDocumentRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static CloseDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType of(
            Object o) {
          return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
          return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
          return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
          return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
          return (Object) this instanceof Uint8Array;
        }
      }

      @JsOverlay
      static CloseDocumentRequest.ToObjectReturnType.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      CloseDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(CloseDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<CloseDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<CloseDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TextdocumentFieldType {
      @JsOverlay
      static CloseDocumentRequest.ToObjectReturnType.TextdocumentFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getUri();

      @JsProperty
      double getVersion();

      @JsProperty
      void setUri(String uri);

      @JsProperty
      void setVersion(double version);
    }

    @JsOverlay
    static CloseDocumentRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    CloseDocumentRequest.ToObjectReturnType.ConsoleidFieldType getConsoleid();

    @JsProperty
    CloseDocumentRequest.ToObjectReturnType.TextdocumentFieldType getTextdocument();

    @JsProperty
    void setConsoleid(CloseDocumentRequest.ToObjectReturnType.ConsoleidFieldType consoleid);

    @JsProperty
    void setTextdocument(
        CloseDocumentRequest.ToObjectReturnType.TextdocumentFieldType textdocument);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static CloseDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType of(
            Object o) {
          return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
          return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
          return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
          return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
          return (Object) this instanceof Uint8Array;
        }
      }

      @JsOverlay
      static CloseDocumentRequest.ToObjectReturnType0.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      CloseDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(CloseDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<CloseDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<CloseDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TextdocumentFieldType {
      @JsOverlay
      static CloseDocumentRequest.ToObjectReturnType0.TextdocumentFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getUri();

      @JsProperty
      double getVersion();

      @JsProperty
      void setUri(String uri);

      @JsProperty
      void setVersion(double version);
    }

    @JsOverlay
    static CloseDocumentRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    CloseDocumentRequest.ToObjectReturnType0.ConsoleidFieldType getConsoleid();

    @JsProperty
    CloseDocumentRequest.ToObjectReturnType0.TextdocumentFieldType getTextdocument();

    @JsProperty
    void setConsoleid(CloseDocumentRequest.ToObjectReturnType0.ConsoleidFieldType consoleid);

    @JsProperty
    void setTextdocument(
        CloseDocumentRequest.ToObjectReturnType0.TextdocumentFieldType textdocument);
  }

  public static native CloseDocumentRequest deserializeBinary(Uint8Array bytes);

  public static native CloseDocumentRequest deserializeBinaryFromReader(
      CloseDocumentRequest message, Object reader);

  public static native void serializeBinaryToWriter(CloseDocumentRequest message, Object writer);

  public static native CloseDocumentRequest.ToObjectReturnType toObject(
      boolean includeInstance, CloseDocumentRequest msg);

  public native void clearConsoleid();

  public native void clearTextdocument();

  public native Ticket getConsoleid();

  public native VersionedTextDocumentIdentifier getTextdocument();

  public native boolean hasConsoleid();

  public native boolean hasTextdocument();

  public native Uint8Array serializeBinary();

  public native void setConsoleid();

  public native void setConsoleid(Ticket value);

  public native void setTextdocument();

  public native void setTextdocument(VersionedTextDocumentIdentifier value);

  public native CloseDocumentRequest.ToObjectReturnType0 toObject();

  public native CloseDocumentRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
