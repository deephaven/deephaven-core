package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
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
    name = "dhinternal.io.deephaven.proto.table_pb.HeadOrTailByRequest",
    namespace = JsPackage.GLOBAL)
public class HeadOrTailByRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static HeadOrTailByRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(
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
      static HeadOrTailByRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      HeadOrTailByRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(HeadOrTailByRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<HeadOrTailByRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<HeadOrTailByRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static HeadOrTailByRequest.ToObjectReturnType.SourceidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchoffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchoffset(double batchoffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsOverlay
    static HeadOrTailByRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getGroupbycolumnspecsList();

    @JsProperty
    String getNumrows();

    @JsProperty
    HeadOrTailByRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    HeadOrTailByRequest.ToObjectReturnType.SourceidFieldType getSourceid();

    @JsProperty
    void setGroupbycolumnspecsList(JsArray<String> groupbycolumnspecsList);

    @JsOverlay
    default void setGroupbycolumnspecsList(String[] groupbycolumnspecsList) {
      setGroupbycolumnspecsList(Js.<JsArray<String>>uncheckedCast(groupbycolumnspecsList));
    }

    @JsProperty
    void setNumrows(String numrows);

    @JsProperty
    void setResultid(HeadOrTailByRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(HeadOrTailByRequest.ToObjectReturnType.SourceidFieldType sourceid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static HeadOrTailByRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
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
      static HeadOrTailByRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      HeadOrTailByRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(HeadOrTailByRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<HeadOrTailByRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<HeadOrTailByRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static HeadOrTailByRequest.ToObjectReturnType0.SourceidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchoffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchoffset(double batchoffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsOverlay
    static HeadOrTailByRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getGroupbycolumnspecsList();

    @JsProperty
    String getNumrows();

    @JsProperty
    HeadOrTailByRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    HeadOrTailByRequest.ToObjectReturnType0.SourceidFieldType getSourceid();

    @JsProperty
    void setGroupbycolumnspecsList(JsArray<String> groupbycolumnspecsList);

    @JsOverlay
    default void setGroupbycolumnspecsList(String[] groupbycolumnspecsList) {
      setGroupbycolumnspecsList(Js.<JsArray<String>>uncheckedCast(groupbycolumnspecsList));
    }

    @JsProperty
    void setNumrows(String numrows);

    @JsProperty
    void setResultid(HeadOrTailByRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(HeadOrTailByRequest.ToObjectReturnType0.SourceidFieldType sourceid);
  }

  public static native HeadOrTailByRequest deserializeBinary(Uint8Array bytes);

  public static native HeadOrTailByRequest deserializeBinaryFromReader(
      HeadOrTailByRequest message, Object reader);

  public static native void serializeBinaryToWriter(HeadOrTailByRequest message, Object writer);

  public static native HeadOrTailByRequest.ToObjectReturnType toObject(
      boolean includeInstance, HeadOrTailByRequest msg);

  public native String addGroupbycolumnspecs(String value, double index);

  public native String addGroupbycolumnspecs(String value);

  public native void clearGroupbycolumnspecsList();

  public native void clearResultid();

  public native void clearSourceid();

  public native JsArray<String> getGroupbycolumnspecsList();

  public native String getNumrows();

  public native Ticket getResultid();

  public native TableReference getSourceid();

  public native boolean hasResultid();

  public native boolean hasSourceid();

  public native Uint8Array serializeBinary();

  public native void setGroupbycolumnspecsList(JsArray<String> value);

  @JsOverlay
  public final void setGroupbycolumnspecsList(String[] value) {
    setGroupbycolumnspecsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setNumrows(String value);

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSourceid();

  public native void setSourceid(TableReference value);

  public native HeadOrTailByRequest.ToObjectReturnType0 toObject();

  public native HeadOrTailByRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
