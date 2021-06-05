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
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static HeadOrTailByRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType of(
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
      static HeadOrTailByRequest.ToObjectReturnType.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      HeadOrTailByRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(HeadOrTailByRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<HeadOrTailByRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<HeadOrTailByRequest.ToObjectReturnType.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdFieldType {
      @JsOverlay
      static HeadOrTailByRequest.ToObjectReturnType.SourceIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchOffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchOffset(double batchOffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsOverlay
    static HeadOrTailByRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getGroupByColumnSpecsList();

    @JsProperty
    String getNumRows();

    @JsProperty
    HeadOrTailByRequest.ToObjectReturnType.ResultIdFieldType getResultId();

    @JsProperty
    HeadOrTailByRequest.ToObjectReturnType.SourceIdFieldType getSourceId();

    @JsProperty
    void setGroupByColumnSpecsList(JsArray<String> groupByColumnSpecsList);

    @JsOverlay
    default void setGroupByColumnSpecsList(String[] groupByColumnSpecsList) {
      setGroupByColumnSpecsList(Js.<JsArray<String>>uncheckedCast(groupByColumnSpecsList));
    }

    @JsProperty
    void setNumRows(String numRows);

    @JsProperty
    void setResultId(HeadOrTailByRequest.ToObjectReturnType.ResultIdFieldType resultId);

    @JsProperty
    void setSourceId(HeadOrTailByRequest.ToObjectReturnType.SourceIdFieldType sourceId);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultIdFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static HeadOrTailByRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType of(
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
      static HeadOrTailByRequest.ToObjectReturnType0.ResultIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      HeadOrTailByRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(HeadOrTailByRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<HeadOrTailByRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<HeadOrTailByRequest.ToObjectReturnType0.ResultIdFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceIdFieldType {
      @JsOverlay
      static HeadOrTailByRequest.ToObjectReturnType0.SourceIdFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchOffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchOffset(double batchOffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsOverlay
    static HeadOrTailByRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<String> getGroupByColumnSpecsList();

    @JsProperty
    String getNumRows();

    @JsProperty
    HeadOrTailByRequest.ToObjectReturnType0.ResultIdFieldType getResultId();

    @JsProperty
    HeadOrTailByRequest.ToObjectReturnType0.SourceIdFieldType getSourceId();

    @JsProperty
    void setGroupByColumnSpecsList(JsArray<String> groupByColumnSpecsList);

    @JsOverlay
    default void setGroupByColumnSpecsList(String[] groupByColumnSpecsList) {
      setGroupByColumnSpecsList(Js.<JsArray<String>>uncheckedCast(groupByColumnSpecsList));
    }

    @JsProperty
    void setNumRows(String numRows);

    @JsProperty
    void setResultId(HeadOrTailByRequest.ToObjectReturnType0.ResultIdFieldType resultId);

    @JsProperty
    void setSourceId(HeadOrTailByRequest.ToObjectReturnType0.SourceIdFieldType sourceId);
  }

  public static native HeadOrTailByRequest deserializeBinary(Uint8Array bytes);

  public static native HeadOrTailByRequest deserializeBinaryFromReader(
      HeadOrTailByRequest message, Object reader);

  public static native void serializeBinaryToWriter(HeadOrTailByRequest message, Object writer);

  public static native HeadOrTailByRequest.ToObjectReturnType toObject(
      boolean includeInstance, HeadOrTailByRequest msg);

  public native String addGroupByColumnSpecs(String value, double index);

  public native String addGroupByColumnSpecs(String value);

  public native void clearGroupByColumnSpecsList();

  public native void clearResultId();

  public native void clearSourceId();

  public native JsArray<String> getGroupByColumnSpecsList();

  public native String getNumRows();

  public native Ticket getResultId();

  public native TableReference getSourceId();

  public native boolean hasResultId();

  public native boolean hasSourceId();

  public native Uint8Array serializeBinary();

  public native void setGroupByColumnSpecsList(JsArray<String> value);

  @JsOverlay
  public final void setGroupByColumnSpecsList(String[] value) {
    setGroupByColumnSpecsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setNumRows(String value);

  public native void setResultId();

  public native void setResultId(Ticket value);

  public native void setSourceId();

  public native void setSourceId(TableReference value);

  public native HeadOrTailByRequest.ToObjectReturnType0 toObject();

  public native HeadOrTailByRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
