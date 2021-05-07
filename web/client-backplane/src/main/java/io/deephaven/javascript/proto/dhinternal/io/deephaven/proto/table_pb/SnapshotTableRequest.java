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
    name = "dhinternal.io.deephaven.proto.table_pb.SnapshotTableRequest",
    namespace = JsPackage.GLOBAL)
public class SnapshotTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LeftidFieldType {
      @JsOverlay
      static SnapshotTableRequest.ToObjectReturnType.LeftidFieldType create() {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static SnapshotTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(
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
      static SnapshotTableRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SnapshotTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(SnapshotTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<SnapshotTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<SnapshotTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static SnapshotTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SnapshotTableRequest.ToObjectReturnType.LeftidFieldType getLeftid();

    @JsProperty
    SnapshotTableRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    Object getRightid();

    @JsProperty
    JsArray<String> getStampcolumnsList();

    @JsProperty
    boolean isDoinitialsnapshot();

    @JsProperty
    void setDoinitialsnapshot(boolean doinitialsnapshot);

    @JsProperty
    void setLeftid(SnapshotTableRequest.ToObjectReturnType.LeftidFieldType leftid);

    @JsProperty
    void setResultid(SnapshotTableRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setRightid(Object rightid);

    @JsProperty
    void setStampcolumnsList(JsArray<String> stampcolumnsList);

    @JsOverlay
    default void setStampcolumnsList(String[] stampcolumnsList) {
      setStampcolumnsList(Js.<JsArray<String>>uncheckedCast(stampcolumnsList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LeftidFieldType {
      @JsOverlay
      static SnapshotTableRequest.ToObjectReturnType0.LeftidFieldType create() {
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static SnapshotTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
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
      static SnapshotTableRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      SnapshotTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(SnapshotTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<SnapshotTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<SnapshotTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsOverlay
    static SnapshotTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    SnapshotTableRequest.ToObjectReturnType0.LeftidFieldType getLeftid();

    @JsProperty
    SnapshotTableRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    Object getRightid();

    @JsProperty
    JsArray<String> getStampcolumnsList();

    @JsProperty
    boolean isDoinitialsnapshot();

    @JsProperty
    void setDoinitialsnapshot(boolean doinitialsnapshot);

    @JsProperty
    void setLeftid(SnapshotTableRequest.ToObjectReturnType0.LeftidFieldType leftid);

    @JsProperty
    void setResultid(SnapshotTableRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setRightid(Object rightid);

    @JsProperty
    void setStampcolumnsList(JsArray<String> stampcolumnsList);

    @JsOverlay
    default void setStampcolumnsList(String[] stampcolumnsList) {
      setStampcolumnsList(Js.<JsArray<String>>uncheckedCast(stampcolumnsList));
    }
  }

  public static native SnapshotTableRequest deserializeBinary(Uint8Array bytes);

  public static native SnapshotTableRequest deserializeBinaryFromReader(
      SnapshotTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(SnapshotTableRequest message, Object writer);

  public static native SnapshotTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, SnapshotTableRequest msg);

  public native String addStampcolumns(String value, double index);

  public native String addStampcolumns(String value);

  public native void clearLeftid();

  public native void clearResultid();

  public native void clearRightid();

  public native void clearStampcolumnsList();

  public native boolean getDoinitialsnapshot();

  public native TableReference getLeftid();

  public native Ticket getResultid();

  public native TableReference getRightid();

  public native JsArray<String> getStampcolumnsList();

  public native boolean hasLeftid();

  public native boolean hasResultid();

  public native boolean hasRightid();

  public native Uint8Array serializeBinary();

  public native void setDoinitialsnapshot(boolean value);

  public native void setLeftid();

  public native void setLeftid(TableReference value);

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setRightid();

  public native void setRightid(TableReference value);

  public native void setStampcolumnsList(JsArray<String> value);

  @JsOverlay
  public final void setStampcolumnsList(String[] value) {
    setStampcolumnsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native SnapshotTableRequest.ToObjectReturnType0 toObject();

  public native SnapshotTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
