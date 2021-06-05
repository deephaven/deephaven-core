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
    name = "dhinternal.io.deephaven.proto.console_pb.OpenDocumentRequest",
    namespace = JsPackage.GLOBAL)
public class OpenDocumentRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static OpenDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType of(
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
      static OpenDocumentRequest.ToObjectReturnType.ConsoleIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      OpenDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(OpenDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<OpenDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<OpenDocumentRequest.ToObjectReturnType.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TextDocumentFieldType {
      @JsOverlay
      static OpenDocumentRequest.ToObjectReturnType.TextDocumentFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getLanguageId();

      @JsProperty
      String getText();

      @JsProperty
      String getUri();

      @JsProperty
      double getVersion();

      @JsProperty
      void setLanguageId(String languageId);

      @JsProperty
      void setText(String text);

      @JsProperty
      void setUri(String uri);

      @JsProperty
      void setVersion(double version);
    }

    @JsOverlay
    static OpenDocumentRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    OpenDocumentRequest.ToObjectReturnType.ConsoleIdFieldType getConsoleId();

    @JsProperty
    OpenDocumentRequest.ToObjectReturnType.TextDocumentFieldType getTextDocument();

    @JsProperty
    void setConsoleId(OpenDocumentRequest.ToObjectReturnType.ConsoleIdFieldType consoleId);

    @JsProperty
    void setTextDocument(OpenDocumentRequest.ToObjectReturnType.TextDocumentFieldType textDocument);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static OpenDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType of(
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
      static OpenDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      OpenDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(OpenDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<OpenDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<OpenDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TextDocumentFieldType {
      @JsOverlay
      static OpenDocumentRequest.ToObjectReturnType0.TextDocumentFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getLanguageId();

      @JsProperty
      String getText();

      @JsProperty
      String getUri();

      @JsProperty
      double getVersion();

      @JsProperty
      void setLanguageId(String languageId);

      @JsProperty
      void setText(String text);

      @JsProperty
      void setUri(String uri);

      @JsProperty
      void setVersion(double version);
    }

    @JsOverlay
    static OpenDocumentRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    OpenDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType getConsoleId();

    @JsProperty
    OpenDocumentRequest.ToObjectReturnType0.TextDocumentFieldType getTextDocument();

    @JsProperty
    void setConsoleId(OpenDocumentRequest.ToObjectReturnType0.ConsoleIdFieldType consoleId);

    @JsProperty
    void setTextDocument(
        OpenDocumentRequest.ToObjectReturnType0.TextDocumentFieldType textDocument);
  }

  public static native OpenDocumentRequest deserializeBinary(Uint8Array bytes);

  public static native OpenDocumentRequest deserializeBinaryFromReader(
      OpenDocumentRequest message, Object reader);

  public static native void serializeBinaryToWriter(OpenDocumentRequest message, Object writer);

  public static native OpenDocumentRequest.ToObjectReturnType toObject(
      boolean includeInstance, OpenDocumentRequest msg);

  public native void clearConsoleId();

  public native void clearTextDocument();

  public native Ticket getConsoleId();

  public native TextDocumentItem getTextDocument();

  public native boolean hasConsoleId();

  public native boolean hasTextDocument();

  public native Uint8Array serializeBinary();

  public native void setConsoleId();

  public native void setConsoleId(Ticket value);

  public native void setTextDocument();

  public native void setTextDocument(TextDocumentItem value);

  public native OpenDocumentRequest.ToObjectReturnType0 toObject();

  public native OpenDocumentRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
