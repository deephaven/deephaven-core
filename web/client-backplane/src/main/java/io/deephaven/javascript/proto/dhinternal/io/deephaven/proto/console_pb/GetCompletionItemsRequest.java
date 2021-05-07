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
    name = "dhinternal.io.deephaven.proto.console_pb.GetCompletionItemsRequest",
    namespace = JsPackage.GLOBAL)
public class GetCompletionItemsRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static GetCompletionItemsRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType of(
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
      static GetCompletionItemsRequest.ToObjectReturnType.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      GetCompletionItemsRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(GetCompletionItemsRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<GetCompletionItemsRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<GetCompletionItemsRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ContextFieldType {
      @JsOverlay
      static GetCompletionItemsRequest.ToObjectReturnType.ContextFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getTriggercharacter();

      @JsProperty
      double getTriggerkind();

      @JsProperty
      void setTriggercharacter(String triggercharacter);

      @JsProperty
      void setTriggerkind(double triggerkind);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface PositionFieldType {
      @JsOverlay
      static GetCompletionItemsRequest.ToObjectReturnType.PositionFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getCharacter();

      @JsProperty
      double getLine();

      @JsProperty
      void setCharacter(double character);

      @JsProperty
      void setLine(double line);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TextdocumentFieldType {
      @JsOverlay
      static GetCompletionItemsRequest.ToObjectReturnType.TextdocumentFieldType create() {
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
    static GetCompletionItemsRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    GetCompletionItemsRequest.ToObjectReturnType.ConsoleidFieldType getConsoleid();

    @JsProperty
    GetCompletionItemsRequest.ToObjectReturnType.ContextFieldType getContext();

    @JsProperty
    GetCompletionItemsRequest.ToObjectReturnType.PositionFieldType getPosition();

    @JsProperty
    GetCompletionItemsRequest.ToObjectReturnType.TextdocumentFieldType getTextdocument();

    @JsProperty
    void setConsoleid(GetCompletionItemsRequest.ToObjectReturnType.ConsoleidFieldType consoleid);

    @JsProperty
    void setContext(GetCompletionItemsRequest.ToObjectReturnType.ContextFieldType context);

    @JsProperty
    void setPosition(GetCompletionItemsRequest.ToObjectReturnType.PositionFieldType position);

    @JsProperty
    void setTextdocument(
        GetCompletionItemsRequest.ToObjectReturnType.TextdocumentFieldType textdocument);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static GetCompletionItemsRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType of(
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
      static GetCompletionItemsRequest.ToObjectReturnType0.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      GetCompletionItemsRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(
          GetCompletionItemsRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<GetCompletionItemsRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<GetCompletionItemsRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ContextFieldType {
      @JsOverlay
      static GetCompletionItemsRequest.ToObjectReturnType0.ContextFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getTriggercharacter();

      @JsProperty
      double getTriggerkind();

      @JsProperty
      void setTriggercharacter(String triggercharacter);

      @JsProperty
      void setTriggerkind(double triggerkind);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface PositionFieldType {
      @JsOverlay
      static GetCompletionItemsRequest.ToObjectReturnType0.PositionFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getCharacter();

      @JsProperty
      double getLine();

      @JsProperty
      void setCharacter(double character);

      @JsProperty
      void setLine(double line);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TextdocumentFieldType {
      @JsOverlay
      static GetCompletionItemsRequest.ToObjectReturnType0.TextdocumentFieldType create() {
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
    static GetCompletionItemsRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    GetCompletionItemsRequest.ToObjectReturnType0.ConsoleidFieldType getConsoleid();

    @JsProperty
    GetCompletionItemsRequest.ToObjectReturnType0.ContextFieldType getContext();

    @JsProperty
    GetCompletionItemsRequest.ToObjectReturnType0.PositionFieldType getPosition();

    @JsProperty
    GetCompletionItemsRequest.ToObjectReturnType0.TextdocumentFieldType getTextdocument();

    @JsProperty
    void setConsoleid(GetCompletionItemsRequest.ToObjectReturnType0.ConsoleidFieldType consoleid);

    @JsProperty
    void setContext(GetCompletionItemsRequest.ToObjectReturnType0.ContextFieldType context);

    @JsProperty
    void setPosition(GetCompletionItemsRequest.ToObjectReturnType0.PositionFieldType position);

    @JsProperty
    void setTextdocument(
        GetCompletionItemsRequest.ToObjectReturnType0.TextdocumentFieldType textdocument);
  }

  public static native GetCompletionItemsRequest deserializeBinary(Uint8Array bytes);

  public static native GetCompletionItemsRequest deserializeBinaryFromReader(
      GetCompletionItemsRequest message, Object reader);

  public static native void serializeBinaryToWriter(
      GetCompletionItemsRequest message, Object writer);

  public static native GetCompletionItemsRequest.ToObjectReturnType toObject(
      boolean includeInstance, GetCompletionItemsRequest msg);

  public native void clearConsoleid();

  public native void clearContext();

  public native void clearPosition();

  public native void clearTextdocument();

  public native Ticket getConsoleid();

  public native CompletionContext getContext();

  public native Position getPosition();

  public native VersionedTextDocumentIdentifier getTextdocument();

  public native boolean hasConsoleid();

  public native boolean hasContext();

  public native boolean hasPosition();

  public native boolean hasTextdocument();

  public native Uint8Array serializeBinary();

  public native void setConsoleid();

  public native void setConsoleid(Ticket value);

  public native void setContext();

  public native void setContext(CompletionContext value);

  public native void setPosition();

  public native void setPosition(Position value);

  public native void setTextdocument();

  public native void setTextdocument(VersionedTextDocumentIdentifier value);

  public native GetCompletionItemsRequest.ToObjectReturnType0 toObject();

  public native GetCompletionItemsRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
