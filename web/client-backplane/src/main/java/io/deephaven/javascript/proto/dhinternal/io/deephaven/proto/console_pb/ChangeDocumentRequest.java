package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.changedocumentrequest.TextDocumentContentChangeEvent;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.console_pb.ChangeDocumentRequest",
    namespace = JsPackage.GLOBAL)
public class ChangeDocumentRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ChangeDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType of(
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
      static ChangeDocumentRequest.ToObjectReturnType.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ChangeDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ChangeDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ChangeDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ChangeDocumentRequest.ToObjectReturnType.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ContentchangesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface RangeFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface StartFieldType {
          @JsOverlay
          static ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType.RangeFieldType
                  .StartFieldType
              create() {
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

        @JsOverlay
        static ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType.RangeFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getEnd();

        @JsProperty
        ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType.RangeFieldType
                .StartFieldType
            getStart();

        @JsProperty
        void setEnd(Object end);

        @JsProperty
        void setStart(
            ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType.RangeFieldType
                    .StartFieldType
                start);
      }

      @JsOverlay
      static ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType.RangeFieldType
          getRange();

      @JsProperty
      double getRangelength();

      @JsProperty
      String getText();

      @JsProperty
      void setRange(
          ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType.RangeFieldType
              range);

      @JsProperty
      void setRangelength(double rangelength);

      @JsProperty
      void setText(String text);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TextdocumentFieldType {
      @JsOverlay
      static ChangeDocumentRequest.ToObjectReturnType.TextdocumentFieldType create() {
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
    static ChangeDocumentRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    ChangeDocumentRequest.ToObjectReturnType.ConsoleidFieldType getConsoleid();

    @JsProperty
    JsArray<ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType>
        getContentchangesList();

    @JsProperty
    ChangeDocumentRequest.ToObjectReturnType.TextdocumentFieldType getTextdocument();

    @JsProperty
    void setConsoleid(ChangeDocumentRequest.ToObjectReturnType.ConsoleidFieldType consoleid);

    @JsOverlay
    default void setContentchangesList(
        ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType[] contentchangesList) {
      setContentchangesList(
          Js
              .<JsArray<ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType>>
                  uncheckedCast(contentchangesList));
    }

    @JsProperty
    void setContentchangesList(
        JsArray<ChangeDocumentRequest.ToObjectReturnType.ContentchangesListFieldType>
            contentchangesList);

    @JsProperty
    void setTextdocument(
        ChangeDocumentRequest.ToObjectReturnType.TextdocumentFieldType textdocument);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ConsoleidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ChangeDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType of(
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
      static ChangeDocumentRequest.ToObjectReturnType0.ConsoleidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ChangeDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ChangeDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ChangeDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ChangeDocumentRequest.ToObjectReturnType0.ConsoleidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ContentchangesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface RangeFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface StartFieldType {
          @JsOverlay
          static ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType
                  .RangeFieldType.StartFieldType
              create() {
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

        @JsOverlay
        static ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType.RangeFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        Object getEnd();

        @JsProperty
        ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType.RangeFieldType
                .StartFieldType
            getStart();

        @JsProperty
        void setEnd(Object end);

        @JsProperty
        void setStart(
            ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType.RangeFieldType
                    .StartFieldType
                start);
      }

      @JsOverlay
      static ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType.RangeFieldType
          getRange();

      @JsProperty
      double getRangelength();

      @JsProperty
      String getText();

      @JsProperty
      void setRange(
          ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType.RangeFieldType
              range);

      @JsProperty
      void setRangelength(double rangelength);

      @JsProperty
      void setText(String text);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TextdocumentFieldType {
      @JsOverlay
      static ChangeDocumentRequest.ToObjectReturnType0.TextdocumentFieldType create() {
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
    static ChangeDocumentRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    ChangeDocumentRequest.ToObjectReturnType0.ConsoleidFieldType getConsoleid();

    @JsProperty
    JsArray<ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType>
        getContentchangesList();

    @JsProperty
    ChangeDocumentRequest.ToObjectReturnType0.TextdocumentFieldType getTextdocument();

    @JsProperty
    void setConsoleid(ChangeDocumentRequest.ToObjectReturnType0.ConsoleidFieldType consoleid);

    @JsOverlay
    default void setContentchangesList(
        ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType[]
            contentchangesList) {
      setContentchangesList(
          Js
              .<JsArray<ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType>>
                  uncheckedCast(contentchangesList));
    }

    @JsProperty
    void setContentchangesList(
        JsArray<ChangeDocumentRequest.ToObjectReturnType0.ContentchangesListFieldType>
            contentchangesList);

    @JsProperty
    void setTextdocument(
        ChangeDocumentRequest.ToObjectReturnType0.TextdocumentFieldType textdocument);
  }

  public static native ChangeDocumentRequest deserializeBinary(Uint8Array bytes);

  public static native ChangeDocumentRequest deserializeBinaryFromReader(
      ChangeDocumentRequest message, Object reader);

  public static native void serializeBinaryToWriter(ChangeDocumentRequest message, Object writer);

  public static native ChangeDocumentRequest.ToObjectReturnType toObject(
      boolean includeInstance, ChangeDocumentRequest msg);

  public native TextDocumentContentChangeEvent addContentchanges();

  public native TextDocumentContentChangeEvent addContentchanges(
      TextDocumentContentChangeEvent value, double index);

  public native TextDocumentContentChangeEvent addContentchanges(
      TextDocumentContentChangeEvent value);

  public native void clearConsoleid();

  public native void clearContentchangesList();

  public native void clearTextdocument();

  public native Ticket getConsoleid();

  public native JsArray<TextDocumentContentChangeEvent> getContentchangesList();

  public native VersionedTextDocumentIdentifier getTextdocument();

  public native boolean hasConsoleid();

  public native boolean hasTextdocument();

  public native Uint8Array serializeBinary();

  public native void setConsoleid();

  public native void setConsoleid(Ticket value);

  public native void setContentchangesList(JsArray<TextDocumentContentChangeEvent> value);

  @JsOverlay
  public final void setContentchangesList(TextDocumentContentChangeEvent[] value) {
    setContentchangesList(Js.<JsArray<TextDocumentContentChangeEvent>>uncheckedCast(value));
  }

  public native void setTextdocument();

  public native void setTextdocument(VersionedTextDocumentIdentifier value);

  public native ChangeDocumentRequest.ToObjectReturnType0 toObject();

  public native ChangeDocumentRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
